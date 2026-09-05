// 选择k=2

#[derive(Default)]
struct Entry {
    first_latest_ts: u64,
    second_latest_ts: Option<u64>,
    evictable: bool,
}

pub struct LruKReplacer {
    // Box不扩容，相较于Vec更好
    entries: Box<[Entry]>,
    timestamp: u64,
}

impl LruKReplacer {
    pub fn new(frame_count: usize) -> Self {
        let entries = (0..frame_count)
            .map(|_| Entry::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            entries,
            timestamp: 0,
        }
    }

    pub fn record_access(&mut self, frame_id: usize) {
        let entry = &mut self.entries[frame_id];

        if entry.first_latest_ts != 0 {
            entry.second_latest_ts = Some(entry.first_latest_ts);
        }

        self.timestamp += 1;
        entry.first_latest_ts = self.timestamp;
    }

    #[inline]
    pub fn set_evictable(&mut self, frame_id: usize, evictable: bool) {
        self.entries[frame_id].evictable = evictable;
    }

    pub fn evict(&mut self) -> Option<usize> {
        let mut coldest_once: Option<(usize, u64)> = None;
        let mut coldest_twice: Option<(usize, u64)> = None;

        for (frame_id, entry) in self.entries.iter().enumerate() {
            if !entry.evictable || entry.first_latest_ts == 0 {
                continue;
            }

            match entry.second_latest_ts {
                None => {
                    // 访问次数小于2
                    if coldest_once.is_none_or(|(_, ts)| entry.first_latest_ts < ts) {
                        coldest_once = Some((frame_id, entry.first_latest_ts));
                    }
                }

                Some(second_ts) => {
                    if coldest_twice.is_none_or(|(_, ts)| second_ts < ts) {
                        coldest_twice = Some((frame_id, second_ts));
                    }
                }
            }
        }

        let frame_id = coldest_once
            .or(coldest_twice)
            .map(|(frame_id, _)| frame_id)?;

        self.reset(frame_id);

        Some(frame_id)
    }

    #[inline]
    pub fn reset(&mut self, frame_id: usize) {
        self.entries[frame_id] = Entry::default();
    }
}
