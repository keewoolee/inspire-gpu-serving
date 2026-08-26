//! Sidecar store: account changes newer than the serving generation's
//! snapshot. Broadcast identically to every client (small, and trivially
//! private since everyone receives the same bytes). Each flip truncates
//! only through the RETIRING generation's snapshot, so a generation still
//! answering in-flight lookups keeps its complete suffix.

pub use pir_keyword::manifest::SidecarEntry;
use std::sync::Mutex;

#[derive(Default)]
pub struct Sidecar {
    entries: Mutex<Vec<SidecarEntry>>,
}

impl Sidecar {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, address: &[u8], value: &[u8], block: u64) {
        self.entries.lock().unwrap().push(SidecarEntry {
            address_hex: hex::encode(address),
            value_hex: hex::encode(value),
            block,
        });
    }

    /// Entries from blocks strictly newer than `since` (the HTTP layer
    /// passes the snapshot block of the generation that answered).
    pub fn since(&self, since: u64) -> Vec<SidecarEntry> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.block > since)
            .cloned()
            .collect()
    }

    /// Drop entries at or before `snapshot_block`. Called at a flip with
    /// the RETIRING generation's snapshot: everything older is baked into
    /// every generation that can still answer.
    pub fn truncate_through(&self, snapshot_block: u64) {
        self.entries
            .lock()
            .unwrap()
            .retain(|e| e.block > snapshot_block);
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_since_and_truncate() {
        let s = Sidecar::new();
        s.push(&[1u8; 20], &[9u8; 40], 100);
        s.push(&[2u8; 20], &[8u8; 40], 101);
        s.push(&[3u8; 20], &[7u8; 40], 102);
        assert_eq!(s.since(100).len(), 2);
        assert_eq!(s.since(0).len(), 3);
        s.truncate_through(101);
        assert_eq!(s.len(), 1);
        assert_eq!(s.since(0)[0].block, 102);
    }
}
