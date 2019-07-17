use std::collections::{BTreeMap, btree_map};

pub trait Timed {
    fn get_time(&self) -> i64;
}

#[derive(Default)]
pub struct TimeSeries<T: Timed> {
    time2item: BTreeMap<i64, T>
}

impl<T: Timed> TimeSeries<T> {
    pub fn new() -> TimeSeries<T> {
        TimeSeries {
            time2item: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, item: T) -> Result<&T, &str> {
        match self.time2item.entry(item.get_time()) {
            btree_map::Entry::Vacant(e) => Ok(e.insert(item)),
            btree_map::Entry::Occupied(_) => Err("Time already in TimeSeries"),
        }
    }

    pub fn get(&self, time: i64) -> Option<&T> {
        self.time2item.get(&time)
    }

    pub fn iter(&self) -> btree_map::Iter<i64, T> {
        self.time2item.iter()
    }
}

impl<T: Timed> IntoIterator for TimeSeries<T>
{
    type Item = (i64, T);
    type IntoIter = btree_map::IntoIter<i64, T>;

    #[inline]
    fn into_iter(self) -> btree_map::IntoIter<i64, T> {
        self.time2item.into_iter()
    }
}
