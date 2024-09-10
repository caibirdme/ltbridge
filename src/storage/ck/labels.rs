use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use super::common::LabelType;
use dashmap::DashMap;
use itertools::Itertools;
use tokio::sync::mpsc::{self, Sender};

#[derive(Debug, Clone)]
pub struct SeriesStore {
	m: Arc<DashMap<LabelType, HashSet<String>>>,
}

impl SeriesStore {
	fn inner_new() -> Self {
		Self {
			m: Arc::new(DashMap::new()),
		}
	}
	pub fn new() -> (Self, Sender<(LabelType, String)>) {
		let (tx, mut rx) = mpsc::channel(100_000);
		let ss = Self::inner_new();
		let m = ss.clone();
		tokio::spawn(async move {
			while let Some(msg) = rx.recv().await {
				let (label, v) = msg;
				m.insert(label, v);
			}
		});
		(ss, tx)
	}
	pub fn insert(&self, key: LabelType, value: String) {
		self.m.entry(key).or_default().insert(value);
	}

	pub fn get(&self, key: &LabelType) -> Option<Vec<String>> {
		self.m
			.get(key)
			.map(|v| v.value().iter().cloned().collect_vec())
	}
	pub fn labels(&self) -> Vec<LabelType> {
		let mut keys = self.m.iter().map(|ent| ent.key().clone()).collect_vec();
		keys.sort();
		keys
	}
	pub fn series(&self) -> Vec<HashMap<LabelType, String>> {
		let dic: HashMap<LabelType, Vec<String>> = self
			.m
			.iter()
			.map(|ent| {
				let (k, v) = (ent.key(), ent.value());
				(k.clone(), v.iter().cloned().collect_vec())
			})
			.collect();
		let mut keys: Vec<LabelType> = dic.keys().cloned().collect();
		keys.sort();
		let mut cur = HashMap::new();
		let mut ans = Vec::new();
		Self::convert(&keys, 0, &dic, &mut cur, &mut ans);
		ans
	}
	fn convert(
		keys: &[LabelType],
		idx: usize,
		dic: &HashMap<LabelType, Vec<String>>,
		cur: &mut HashMap<LabelType, String>,
		ans: &mut Vec<HashMap<LabelType, String>>,
	) {
		if idx == keys.len() {
			ans.push(cur.clone());
			return;
		}
		let key = &keys[idx];
		for val in &dic[key] {
			cur.insert(key.clone(), val.clone());
			Self::convert(keys, idx + 1, dic, cur, ans);
			// we don't need to remove the key, because it will be overwriten in the next iteration
		}
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;
	use pretty_assertions::assert_eq;

	#[test]
	fn test_convert() {
		let m = SeriesStore::inner_new();
		m.insert("a".into(), "a1".to_string());
		m.insert("a".into(), "a2".to_string());
		m.insert("b".into(), "b1".to_string());
		m.insert("b".into(), "b2".to_string());
		let actual = m.series();
		assert_eq!(actual.len(), 4);
		let expect = vec![
			[
				("a".into(), "a1".to_string()),
				("b".into(), "b1".to_string()),
			]
			.into(),
			[
				("a".into(), "a1".to_string()),
				("b".into(), "b2".to_string()),
			]
			.into(),
			[
				("a".into(), "a2".to_string()),
				("b".into(), "b1".to_string()),
			]
			.into(),
			[
				("a".into(), "a2".to_string()),
				("b".into(), "b2".to_string()),
			]
			.into(),
		];
		for exp in &expect {
			assert!(actual.contains(exp));
		}
	}

	#[test]
	fn test_labels() {
		let m = SeriesStore::inner_new();
		m.insert("b".into(), "b1".to_string());
		m.insert("b".into(), "b2".to_string());
		m.insert("a".into(), "a1".to_string());
		m.insert("a".into(), "a2".to_string());
		m.insert("c".into(), "c1".to_string());
		m.insert("c".into(), "c2".to_string());
		let expect = vec!["a".into(), "b".into(), "c".into()];
		for _ in 1..10 {
			let actual = m.labels();
			assert_eq!(actual, expect);
		}
	}

	#[tokio::test]
	async fn test_async_convert() -> anyhow::Result<()> {
		use tokio::time;
		let (m, tx) = SeriesStore::new();
		tx.send(("a".into(), "a1".to_string())).await?;
		tx.send(("a".into(), "a2".to_string())).await?;
		tx.send(("b".into(), "b1".to_string())).await?;
		tx.send(("b".into(), "b2".to_string())).await?;
		// wait for the consumer to finish
		time::sleep(Duration::from_millis(200)).await;
		let actual = m.series();
		assert_eq!(actual.len(), 4);
		let expect = vec![
			[
				("a".into(), "a1".to_string()),
				("b".into(), "b1".to_string()),
			]
			.into(),
			[
				("a".into(), "a1".to_string()),
				("b".into(), "b2".to_string()),
			]
			.into(),
			[
				("a".into(), "a2".to_string()),
				("b".into(), "b1".to_string()),
			]
			.into(),
			[
				("a".into(), "a2".to_string()),
				("b".into(), "b2".to_string()),
			]
			.into(),
		];
		for exp in &expect {
			assert!(actual.contains(exp));
		}
		Ok(())
	}
}
