//! In-memory registry of suspended tool-call groups (spec §4.6). The process that hit client tool
//! calls is parked: each outstanding tool_call_id holds a `oneshot` the follow-up delivers a result
//! into, and the turn's continuation `EventStream` is held until the FULL result set arrives.
//! Complete follow-up → fire every sender + return the continuation; partial → 409 (group stays
//! live); duplicate/unknown id → 400; idle group reaped after `tool_result_timeout_s`; registering
//! past `max_suspended_sessions` → 503.
//!
//! Phase 4a is driven by a fake bridge (the continuation is a scripted stream awaiting the same
//! oneshots). Phase 4b stores the live CLI child + the rmcp server's parked call handlers behind
//! this identical API (and additionally manages the active-concurrency permit, §4.7).
use crate::engine::EventStream;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

#[derive(Debug, PartialEq, Eq)]
pub enum RegisterError {
    /// `max_suspended_sessions` reached — refuse the new suspension (HTTP 503).
    Full,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DeliverError {
    /// An id matches no live group (unknown or already-timed-out) — HTTP 400.
    Unknown,
    /// The group has outstanding ids not covered by this follow-up — HTTP 409. Group stays live.
    Partial(Vec<String>),
    /// An id supplied twice in the same follow-up — HTTP 400.
    Duplicate(String),
}

/// One parked turn.
struct Group {
    outstanding: HashSet<String>,
    senders: HashMap<String, oneshot::Sender<String>>,
    continuation: EventStream,
}

struct Inner {
    groups: HashMap<u64, Group>, // group_id -> Group
    index: HashMap<String, u64>, // tool_call_id -> group_id
}

#[derive(Clone)]
pub struct SuspendedSessions {
    inner: Arc<Mutex<Inner>>,
    max: usize,
    next_id: Arc<AtomicU64>,
}

impl SuspendedSessions {
    pub fn new(max_suspended_sessions: usize) -> Self {
        SuspendedSessions {
            inner: Arc::new(Mutex::new(Inner { groups: HashMap::new(), index: HashMap::new() })),
            max: max_suspended_sessions.max(1),
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Park a turn: `pairs` is (tool_call_id, result-delivery sender) per outstanding call;
    /// `continuation` resumes once every result is delivered. Returns the new group id, or
    /// `Full` when at `max_suspended_sessions`.
    pub fn register(
        &self,
        pairs: Vec<(String, oneshot::Sender<String>)>,
        continuation: EventStream,
    ) -> Result<u64, RegisterError> {
        let mut g = self.inner.lock().unwrap();
        if g.groups.len() >= self.max {
            return Err(RegisterError::Full);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let outstanding: HashSet<String> = pairs.iter().map(|(k, _)| k.clone()).collect();
        let senders: HashMap<String, oneshot::Sender<String>> = pairs.into_iter().collect();
        for k in &outstanding {
            g.index.insert(k.clone(), id);
        }
        g.groups.insert(id, Group { outstanding, senders, continuation });
        Ok(id)
    }

    /// Deliver a follow-up's result set. On a complete, valid set: fire every sender, remove the
    /// group, and return its continuation `EventStream` (the response to *this* follow-up).
    pub fn deliver(&self, results: &[(String, String)]) -> Result<EventStream, DeliverError> {
        let mut g = self.inner.lock().unwrap();

        // No duplicate ids within this follow-up.
        let mut seen = HashSet::new();
        for (id, _) in results {
            if !seen.insert(id.as_str()) {
                return Err(DeliverError::Duplicate(id.clone()));
            }
        }
        // Resolve the group from the first id; every id must belong to that SAME live group.
        let Some((first, _)) = results.first() else {
            return Err(DeliverError::Unknown);
        };
        let Some(&gid) = g.index.get(first) else {
            return Err(DeliverError::Unknown);
        };
        if results.iter().any(|(id, _)| g.index.get(id) != Some(&gid)) {
            return Err(DeliverError::Unknown); // an id from another/no group
        }
        // Completeness: the follow-up must cover every outstanding id of the group.
        {
            let group = g.groups.get(&gid).expect("indexed group present");
            let provided: HashSet<&str> = results.iter().map(|(id, _)| id.as_str()).collect();
            let mut missing: Vec<String> =
                group.outstanding.iter().filter(|id| !provided.contains(id.as_str())).cloned().collect();
            if !missing.is_empty() {
                missing.sort();
                return Err(DeliverError::Partial(missing));
            }
        }
        // Complete: remove the group, fire each sender, hand back the continuation.
        let mut group = g.groups.remove(&gid).unwrap();
        for id in &group.outstanding {
            g.index.remove(id);
        }
        for (id, content) in results {
            if let Some(tx) = group.senders.remove(id) {
                let _ = tx.send(content.clone());
            }
        }
        Ok(group.continuation)
    }

    /// Remove a group if still present. Dropping it drops the senders (parked calls see `Closed`)
    /// and the continuation (in Phase 4b that drops the held CLI child, killing it).
    pub fn reap(&self, group_id: u64) {
        let mut g = self.inner.lock().unwrap();
        if let Some(group) = g.groups.remove(&group_id) {
            for id in &group.outstanding {
                g.index.remove(id);
            }
        }
    }

    /// Spawn the orphan-timeout reaper for a group (`tool_result_timeout_s`). A no-op if the group
    /// was already delivered/removed by the time it fires.
    pub fn spawn_reaper(&self, group_id: u64, timeout: std::time::Duration) {
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            this.reap(group_id);
        });
    }

    pub fn live_count(&self) -> usize {
        self.inner.lock().unwrap().groups.len()
    }

    /// True when the suspended pool is at `max_suspended_sessions` (a new suspension would be refused).
    pub fn is_full(&self) -> bool {
        self.inner.lock().unwrap().groups.len() >= self.max
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::AgentEvent;
    use futures::StreamExt;
    use tokio::sync::oneshot;

    fn continuation() -> crate::engine::EventStream {
        Box::pin(futures::stream::iter(vec![
            AgentEvent::AssistantText("continued".into()),
            AgentEvent::Done { finish_reason: "stop".into() },
        ]))
    }

    #[allow(clippy::type_complexity)]
    fn channels(ids: &[&str]) -> (Vec<(String, oneshot::Sender<String>)>, Vec<oneshot::Receiver<String>>) {
        let mut pairs = Vec::new();
        let mut rxs = Vec::new();
        for id in ids {
            let (tx, rx) = oneshot::channel();
            pairs.push(((*id).to_string(), tx));
            rxs.push(rx);
        }
        (pairs, rxs)
    }

    #[tokio::test]
    async fn complete_delivery_fires_senders_and_returns_continuation() {
        let s = SuspendedSessions::new(8);
        let (pairs, mut rxs) = channels(&["call_a", "call_b"]);
        s.register(pairs, continuation()).unwrap();
        assert_eq!(s.live_count(), 1);

        let cont = s.deliver(&[("call_a".into(), "RA".into()), ("call_b".into(), "RB".into())]).unwrap();
        assert_eq!(rxs[0].try_recv().unwrap(), "RA");
        assert_eq!(rxs[1].try_recv().unwrap(), "RB");
        assert_eq!(s.live_count(), 0);
        let evs: Vec<_> = cont.collect().await;
        assert!(evs.iter().any(|e| matches!(e, AgentEvent::AssistantText(t) if t == "continued")));
    }

    #[tokio::test]
    async fn partial_set_is_409_and_keeps_group_live() {
        let s = SuspendedSessions::new(8);
        let (pairs, _rxs) = channels(&["call_a", "call_b"]);
        s.register(pairs, continuation()).unwrap();
        let err = s.deliver(&[("call_a".into(), "RA".into())]).err().unwrap();
        assert_eq!(err, DeliverError::Partial(vec!["call_b".into()]));
        assert_eq!(s.live_count(), 1);
    }

    #[tokio::test]
    async fn duplicate_and_unknown_are_400() {
        let s = SuspendedSessions::new(8);
        let (pairs, _rxs) = channels(&["call_a"]);
        s.register(pairs, continuation()).unwrap();
        assert_eq!(
            s.deliver(&[("call_a".into(), "x".into()), ("call_a".into(), "y".into())]).err().unwrap(),
            DeliverError::Duplicate("call_a".into())
        );
        assert_eq!(s.deliver(&[("nope".into(), "x".into())]).err().unwrap(), DeliverError::Unknown);
        assert_eq!(s.live_count(), 1);
    }

    #[tokio::test]
    async fn register_past_cap_is_full() {
        let s = SuspendedSessions::new(1);
        let (p1, _r1) = channels(&["a"]);
        s.register(p1, continuation()).unwrap();
        let (p2, _r2) = channels(&["b"]);
        assert_eq!(s.register(p2, continuation()).unwrap_err(), RegisterError::Full);
    }

    #[test]
    fn is_full_reflects_capacity() {
        let s = SuspendedSessions::new(1);
        assert!(!s.is_full(), "empty pool should not be full");
        let (pairs, _rxs) = channels(&["call_a"]);
        s.register(pairs, continuation()).unwrap();
        assert!(s.is_full(), "pool at max should be full");
    }

    #[tokio::test]
    async fn reap_removes_idle_group() {
        let s = SuspendedSessions::new(8);
        let (pairs, _rxs) = channels(&["call_a"]);
        let gid = s.register(pairs, continuation()).unwrap();
        s.spawn_reaper(gid, std::time::Duration::from_millis(10));
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        assert_eq!(s.live_count(), 0);
        assert_eq!(s.deliver(&[("call_a".into(), "x".into())]).err().unwrap(), DeliverError::Unknown);
    }
}
