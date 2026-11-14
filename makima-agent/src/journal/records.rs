use imbl::OrdMap;
use makima_storage::session_state::{StateFoldError, StoredAgentRecord, StoredStateRecord};
use serde::{
    Serialize, Serializer,
    ser::{Error as _, SerializeSeq},
};
use std::{ops::Bound, sync::Arc};

pub trait JournalRecord: Clone {
    fn sequence(&self) -> u64;
}

impl JournalRecord for StoredStateRecord {
    fn sequence(&self) -> u64 {
        self.seq
    }
}

impl JournalRecord for (u64, StoredAgentRecord) {
    fn sequence(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct JournalRecords<T: Clone> {
    records: OrdMap<u64, Arc<T>>,
    identity: Arc<()>,
    invalid_sequence: Option<StateFoldError>,
}

impl<T: Clone> Default for JournalRecords<T> {
    fn default() -> Self {
        Self {
            records: OrdMap::new(),
            identity: Arc::new(()),
            invalid_sequence: None,
        }
    }
}

impl<T: JournalRecord> JournalRecords<T> {
    pub fn try_from_ordered(
        records: impl IntoIterator<Item = T>,
        stream: &'static str,
    ) -> Result<Self, StateFoldError> {
        let mut result = Self::default();
        let mut previous = None;
        for record in records {
            let seq = record.sequence();
            if let Some(previous) = previous
                && seq <= previous
            {
                return Err(StateFoldError::UnorderedSequence {
                    stream,
                    previous,
                    seq,
                });
            }
            previous = Some(seq);
            result.records.insert(seq, Arc::new(record));
        }
        Ok(result)
    }

    pub fn insert(&mut self, seq: u64, record: T) {
        self.identity = Arc::new(());
        if seq != record.sequence() && self.invalid_sequence.is_none() {
            self.invalid_sequence = Some(StateFoldError::DuplicateSequence { seq });
        }
        self.records.insert(seq, Arc::new(record));
    }

    pub fn push(&mut self, record: T) {
        self.insert(record.sequence(), record);
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        let removed: Vec<_> = self
            .records
            .iter()
            .filter_map(|(seq, record)| (!keep(record)).then_some(*seq))
            .collect();
        if !removed.is_empty() {
            self.identity = Arc::new(());
        }
        for seq in removed {
            self.records.remove(&seq);
        }
    }
}

impl<T: Clone> JournalRecords<T> {
    pub fn len(&self) -> usize {
        self.records.len()
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    pub fn get(&self, seq: u64) -> Option<&T> {
        self.records.get(&seq).map(Arc::as_ref)
    }
    pub fn remove(&mut self, seq: u64) {
        self.identity = Arc::new(());
        self.records.remove(&seq);
    }
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> + ExactSizeIterator {
        self.records.values().map(Arc::as_ref)
    }
    pub fn after(&self, seq: u64) -> impl DoubleEndedIterator<Item = &T> {
        self.records
            .range((Bound::Excluded(seq), Bound::Unbounded))
            .map(|(_, record)| record.as_ref())
    }
    pub fn to_vec(&self) -> Vec<T> {
        self.iter().cloned().collect()
    }
    pub fn validate(&self) -> Result<(), StateFoldError> {
        self.invalid_sequence.clone().map_or(Ok(()), Err)
    }
    pub(super) fn same_root(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity)
    }
}

impl<T: JournalRecord> FromIterator<T> for JournalRecords<T> {
    fn from_iter<I: IntoIterator<Item = T>>(records: I) -> Self {
        let mut result = Self::default();
        let mut previous = None;
        for record in records {
            let seq = record.sequence();
            if let Some(previous) = previous
                && seq <= previous
                && result.invalid_sequence.is_none()
            {
                result.invalid_sequence = Some(StateFoldError::UnorderedSequence {
                    stream: "journal",
                    previous,
                    seq,
                });
            }
            previous = Some(seq);
            result.records.insert(seq, Arc::new(record));
        }
        result
    }
}

impl<T: JournalRecord> From<Vec<T>> for JournalRecords<T> {
    fn from(records: Vec<T>) -> Self {
        records.into_iter().collect()
    }
}

impl<T: JournalRecord> Extend<T> for JournalRecords<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, records: I) {
        for record in records {
            self.push(record);
        }
    }
}

impl<T: Clone + PartialEq> PartialEq for JournalRecords<T> {
    fn eq(&self, other: &Self) -> bool {
        self.same_root(other)
            || (self.invalid_sequence == other.invalid_sequence && self.records == other.records)
    }
}

impl<T: Clone + Serialize> Serialize for JournalRecords<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(S::Error::custom)?;
        let mut sequence = serializer.serialize_seq(Some(self.len()))?;
        for record in self.iter() {
            sequence.serialize_element(record)?;
        }
        sequence.end()
    }
}

#[cfg(test)]
mod tests {
    use super::JournalRecords;
    use makima_storage::session_state::{StateChange, StateFoldError, StoredStateRecord};
    use test_case::test_case;

    const TOOL_KIND: &str = "profile";

    fn record(seq: u64) -> StoredStateRecord {
        StoredStateRecord {
            seq,
            agent: None,
            generation: 0,
            change: StateChange::ToolExecution {
                kind: TOOL_KIND.into(),
            },
        }
    }

    #[test_case(vec![1, 1]; "duplicate")]
    #[test_case(vec![2, 1]; "out_of_order")]
    fn raw_order_errors_survive_infallible_conversion(sequences: Vec<u64>) {
        let records: Vec<_> = sequences.into_iter().map(record).collect();
        assert!(matches!(
            JournalRecords::try_from_ordered(records.clone(), "state"),
            Err(StateFoldError::UnorderedSequence { .. })
        ));
        let converted: JournalRecords<_> = records.into();
        assert!(matches!(
            converted.validate(),
            Err(StateFoldError::UnorderedSequence { .. })
        ));
        assert!(serde_json::to_value(&converted).is_err());
    }

    #[test]
    fn cloned_records_share_payloads_and_serialize_as_ordered_arrays() {
        let original = JournalRecords::try_from_ordered([record(1), record(3)], "state").unwrap();
        let mut updated = original.clone();
        updated.insert(2, record(2));
        assert_eq!(original.len(), 2);
        assert!(std::ptr::eq(
            original.get(1).unwrap(),
            updated.get(1).unwrap()
        ));
        assert_eq!(
            updated
                .after(1)
                .map(|record| record.seq)
                .collect::<Vec<_>>(),
            [2, 3]
        );
        assert_eq!(
            serde_json::to_value(&updated).unwrap(),
            serde_json::to_value([record(1), record(2), record(3)]).unwrap()
        );
    }
}
