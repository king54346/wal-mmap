mod test_utils;
mod segment;
mod mmap_view_sync;
mod bytes;

use std::cmp::Ordering;
use std::fmt;
use std::fs::{self, File};
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::ops;
use std::path::{Path, PathBuf};
use std::result;
use std::str::FromStr;
use std::thread;
use crossbeam_channel::{Receiver, Sender};

use crate::segment::{Entry, Segment};

#[derive(Debug)]
pub struct WalOptions {
    // 默认的segment容量为32MiB
    pub segment_capacity: usize,

    // 要提前创建的段数，以便追加永远不需要等待创建新段。
    pub segment_queue_len: usize,
}

impl Default for WalOptions {
    fn default() -> WalOptions {
        WalOptions {
            segment_capacity: 32 * 1024 * 1024,
            segment_queue_len: 0,
        }
    }
}

/// An open segment and its ID.
//  一个打开的段和它的ID。
#[derive(Debug)]
struct OpenSegment {
    pub id: u64,
    pub segment: Segment,
}

/// A closed segment, and the associated start and stop indices.
// 一个关闭的段，以及相关的开始和停止索引。
#[derive(Debug)]
struct ClosedSegment {
    pub start_index: u64,
    pub segment: Segment,
}

enum WalSegment {
    Open(OpenSegment),
    Closed(ClosedSegment),
}

// A write ahead log.

pub struct Wal {
    // 当前正在追加的segment
    open_segment: OpenSegment,
    closed_segments: Vec<ClosedSegment>,
    creator: SegmentCreator,

    // 包含预写日志的目录。用于保持打开文件锁的生命周期。
    #[allow(dead_code)]
    dir: File,

    // 目录的路径
    path: PathBuf,

    // 跟踪用户调用`Wal::flush`之间最近关闭的段的刷新状态。
    flush: Option<thread::JoinHandle<Result<()>>>,
}

impl Wal {
    pub fn open<P>(path: P) -> Result<Wal> where P: AsRef<Path> {
        Wal::with_options(path, &WalOptions::default())
    }

    pub fn generate_empty_wal_starting_at_index(
        path: impl Into<PathBuf>,
        options: &WalOptions,
        index: u64,
    ) -> Result<()> {
        let open_id = 0;
        let mut path_buf = path.into();
        path_buf.push(format!("open-{open_id}"));
        let segment = OpenSegment {
            id: index + 1,
            segment: Segment::create(&path_buf, options.segment_capacity)?,
        };

        let mut close_segment = close_segment(segment, index + 1)?;

        close_segment.segment.flush()
    }
    pub fn with_options<P>(path: P, options: &WalOptions) -> Result<Wal> where P: AsRef<Path>{
        #[cfg(not(target_os = "windows"))]
            let path = path.as_ref().to_path_buf();
        #[cfg(not(target_os = "windows"))]
            let dir = File::open(&path)?;

        #[cfg(target_os = "windows")]
            let mut path = path.as_ref().to_path_buf();
        // Windows 解决方法。目录不能独占，因此我们创建一个代理文件在用于锁定的 tmp 目录内。这样做是因为：
        // - 与 Linux 不同，Windows 目录不是文件，因此我们无法使用以下命令打开它
        // `File::open` 也不用 `try_lock_exclusive` 锁定它
        // - 我们希望它与 `TempDir` 一起自动删除
        #[cfg(target_os = "windows")]
            let dir = {
            path.push(".wal");
            let dir = File::options()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)?;
            path.pop();
            dir
        };
        #[cfg(not(target_os = "windows"))]
        dir.try_lock_exclusive()?;

        // Holds open segments in the directory.
        let mut open_segments: Vec<OpenSegment> = Vec::new();
        let mut closed_segments: Vec<ClosedSegment> = Vec::new();
        //读取目录中的所有Segments，并将它们分为打开和关闭的段。
        for entry in fs::read_dir(&path)? {
            match open_dir_entry(entry?)? {
                Some(WalSegment::Open(open_segment)) => open_segments.push(open_segment),
                Some(WalSegment::Closed(closed_segment)) => closed_segments.push(closed_segment),
                None => {}
            }
        }

        //验证已关闭的segments验证这些segments是连续的、非重叠的，并且没有丢失的segments
        closed_segments.sort_by(|a, b| a.start_index.cmp(&b.start_index));
        let mut next_start_index = closed_segments
            .get(0)
            .map_or(0, |segment| segment.start_index);
        for &ClosedSegment {
            start_index,
            ref segment,
            ..
        } in &closed_segments
        {
            match start_index.cmp(&next_start_index) {
                Ordering::Less => {
                    //  这意味着有一个或多个segments被重叠
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "overlapping segments containing wal entries {start_index} to {next_start_index}"
                        ),
                    ));
                }
                Ordering::Equal => {
                    next_start_index = start_index + segment.len() as u64;
                }
                Ordering::Greater => {
                    //当前的segment和前一个segment之间有一个或多个缺失的segments
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "missing segment(s) containing wal entries {next_start_index} to {start_index}"
                        ),
                    ));
                }
            }
        }

        //验证已打开的segments,需要确保最多只有一个非空的segment是开放的
        open_segments.sort_by(|a, b| a.id.cmp(&b.id));

        // The latest open segment, may already have segments.
        let mut open_segment: Option<OpenSegment> = None;
        // Unused open segments.
        let mut unused_segments: Vec<OpenSegment> = Vec::new();
        //  如果发现两个非空的开放segments，那么旧的segment将被关闭，然后用新的segment替换
        for segment in open_segments {
            //其他segments要么是空的，要么已经被关闭并重命名
            //而空的segments根据是否已有一个open segment来决定其归类为新的open segment还是未使用的segment
            if !segment.segment.is_empty() {
                //该段已被写入。如果之前打开过段也已经被写入，我们将其关闭并将其替换为新的。这可能会发生，因为当段已关闭，已重命名，但目录未关闭已同步，因此不能保证该操作是持久的。
                let stranded_segment = open_segment.take();
                open_segment = Some(segment);
                // 它将被关闭，重命名，并添加到closed_segments列表中
                if let Some(segment) = stranded_segment {
                    let closed_segment = close_segment(segment, next_start_index)?;
                    next_start_index += closed_segment.segment.len() as u64;
                    closed_segments.push(closed_segment);
                }
            } else if open_segment.is_none() {
                open_segment = Some(segment);
            } else {
                unused_segments.push(segment);
            }
        }

        let mut creator = SegmentCreator::new(
            &path,
            unused_segments,
            options.segment_capacity,
            options.segment_queue_len,
        );
        // 如果没有打开的segment，那么创建一个新的segment，如果有打开的segment，那么使用一个已经存在的非空的已打开的segment。
        let open_segment = match open_segment {
            Some(segment) => segment,
            None => creator.next()?,
        };
        // 使用以上获取的信息和segments来创建一个新的Wal对象
        let wal = Wal {
            open_segment,
            closed_segments,
            creator,
            dir,
            path,
            flush: None,
        };
        Ok(wal)
    }


    // 它将当前打开的segment关闭，开始新的segment，并确保任何空的关闭segment被删除。
    fn retire_open_segment(&mut self) -> Result<()> {
        // 获取下一个segment，并将其与当前打开的segment交换
        let mut segment = self.creator.next()?;
        mem::swap(&mut self.open_segment, &mut segment);
        //将旧segment写入磁盘
        if let Some(flush) = self.flush.take() {
            flush.join().map_err(|err| {
                Error::new(
                    ErrorKind::Other,
                    format!("wal flush thread panicked: {err:?}"),
                )
            })??;
        };
        // flush重新复制为新的segment的异步刷新
        self.flush = Some(segment.segment.flush_async());
        // 获取新的“打开的”segment的起始索引。
        let start_index = self.open_segment_start_index();

        // 如果最后一个已关闭的segment是空的，则从closed_segments列表中移除并删除它。
        if let Some(last_closed) = self.closed_segments.last() {
            if last_closed.segment.is_empty() {
                let empty_segment = self.closed_segments.pop().unwrap();
                empty_segment.segment.delete()?;
            }
        }
        // 使用close_segment函数关闭当前的segment，并将其添加到closed_segments列表中。
        self.closed_segments
            .push(close_segment(segment, start_index)?);
        Ok(())
    }
    // 条目被添加到segment中
    pub fn append<T>(&mut self, entry: &T) -> Result<u64>
        where
            T: ops::Deref<Target=[u8]>,
    {
        // 确保打开的segment有足够的容量来容纳新条目
        if !self.open_segment.segment.sufficient_capacity(entry.len()) {
            // 检查segment是否为空，如果不是，则关闭它并开始新的segment。则“退休”当前的开放segment，关闭它并开始一个新的segment
            if !self.open_segment.segment.is_empty() {
                self.retire_open_segment()?;
            }
            // 扩容确保新的打开segment有足够的容量
            self.open_segment.segment.ensure_capacity(entry.len())?;
        }
        // 条目被添加到当前打开的segment中
        Ok(self.open_segment_start_index()
            + self.open_segment.segment.append(entry).unwrap() as u64)
    }

    pub fn flush_open_segment(&mut self) -> Result<()> {
        self.open_segment.segment.flush()?;
        Ok(())
    }

    pub fn flush_open_segment_async(&mut self) -> thread::JoinHandle<Result<()>> {
        self.open_segment.segment.flush_async()
    }

    // 从日志中检索具有提供的索引的条目。
    pub fn entry(&self, index: u64) -> Option<Entry> {
        // 获取当前打开的segment的起始索引
        let open_start_index = self.open_segment_start_index();
        // 如果提供的索引大于或等于这个值，那么它在当前打开的segment中
        if index >= open_start_index {
            return self
                .open_segment
                .segment
                .entry((index - open_start_index) as usize);
        }
        // 如果所需的条目不在打开的segment中，那么函数将在已关闭的segments中查找
        match self.find_closed_segment(index) {
            // 如果找到了一个segment，那么它将返回segment_index
            Ok(segment_index) => {
                let segment = &self.closed_segments[segment_index];
                segment
                    .segment
                    .entry((index - segment.start_index) as usize)
            }
            // 如果没有找到segment，那么它将返回None
            Err(i) => {
                // Sanity check that the missing index is less than the start of the log.
                assert_eq!(0, i);
                None
            }
        }
    }

    /// Truncates entries in the log beginning with `from`.
    ///
    /// Entries can be immediately appended to the log once this method returns,
    /// but the truncated entries are not guaranteed to be removed until the
    /// wal is flushed.
    //  根据提供的索引来截断日志
    pub fn truncate(&mut self, from: u64) -> Result<()> {
        let open_start_index = self.open_segment_start_index();
        //它检查所提供的截断索引是否大于或等于打开的segment的起始索引
        if from >= open_start_index {
            //并会从给定索引处截断该segment
            self.open_segment
                .segment
                .truncate((from - open_start_index) as usize);
        } else {
            // 如果提供的截断索引小于打开的segment的起始索引需要在已关闭的segments中进行截断
            // 但在此之前，先完全截断打开的segment
            self.open_segment.segment.truncate(0);

            match self.find_closed_segment(from) {
                Ok(index) => {
                    // 如果给定的截断索引与已关闭segment的起始索引相同，它将删除这个和之后的所有segments
                    if from == self.closed_segments[index].start_index {
                        for segment in self.closed_segments.drain(index..) {
                            let delete_segment = segment.segment;
                            delete_segment.delete()?;
                        }
                    } else {
                        // 如果给定的截断索引在一个已关闭的segment内部，它将截断该segment并删除之后的所有segments
                        {
                            let segment = &mut self.closed_segments[index];
                            segment
                                .segment
                                .truncate((from - segment.start_index) as usize);
                            // flushing closed segment after truncation
                            segment.segment.flush()?;
                        }
                        if index + 1 < self.closed_segments.len() {
                            for segment in self.closed_segments.drain(index + 1..) {
                                let delete_segment = segment.segment;
                                delete_segment.delete()?;
                            }
                        }
                    }
                }
                Err(index) => {
                    //在已关闭segments列表之前的位置进行截断
                    assert!(
                        from <= self
                            .closed_segments
                            .get(index)
                            .map_or(0, |segment| segment.start_index)
                    );
                    // 删除所有已关闭的segments
                    for segment in self.closed_segments.drain(..) {
                        let delete_segment = segment.segment;
                        delete_segment.delete()?;
                    }
                }
            }
        }
        Ok(())
    }

    // 从日志的开头删除指定索引之前的所有条目
    pub fn prefix_truncate(&mut self, until: u64) -> Result<()> {
        // 如果until索引小于或等于第一个已关闭segment的起始索引
        if until
            <= self
            .closed_segments
            .get(0)
            .map_or(0, |segment| segment.start_index)
        {
            // 这表示如果要截断的位置在已关闭的segments的第一个segment之前，那么实际上不需要执行任何操作。
        } else if until >= self.open_segment_start_index() {
            // 如果until索引大于或等于打开的segment的起始索引
            // 除最后一个之外的所有已关闭的segments都会被截断
            if !self.closed_segments.is_empty() {
                for segment in self.closed_segments.drain(..self.closed_segments.len() - 1) {
                    segment.segment.delete()?
                }
            }
        } else {
            // 如果给定的截断索引在两个已关闭的segments之间
            // 会找到该索引所在的segment，并删除它之前的所有segments。
            let mut index = self.find_closed_segment(until).unwrap();
            if index == self.closed_segments.len() {
                index = self.closed_segments.len() - 1;
            }
            for segment in self.closed_segments.drain(..index) {
                segment.segment.delete()?
            }
        }
        Ok(())
    }

    // 返回打开的segment的起始索引
    fn open_segment_start_index(&self) -> u64 {
        self.closed_segments.last().map_or(0, |segment| {
            segment.start_index + segment.segment.len() as u64
        })
    }

    fn find_closed_segment(&self, index: u64) -> result::Result<usize, usize> {
        self.closed_segments.binary_search_by(|segment| {
            if index < segment.start_index {
                Ordering::Greater
            } else if index >= segment.start_index + segment.segment.len() as u64 {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn num_segments(&self) -> usize {
        self.closed_segments.len() + 1
    }

    pub fn num_entries(&self) -> u64 {
        self.open_segment_start_index()
            - self
            .closed_segments
            .get(0)
            .map_or(0, |segment| segment.start_index)
            + self.open_segment.segment.len() as u64
    }

    /// The index of the first entry.
    pub fn first_index(&self) -> u64 {
        self.closed_segments
            .get(0)
            .map_or(0, |segment| segment.start_index)
    }

    /// The index of the last entry
    pub fn last_index(&self) -> u64 {
        let num_entries = self.num_entries();
        self.first_index() + num_entries.saturating_sub(1)
    }

    /// Remove all entries
    pub fn clear(&mut self) -> Result<()> {
        self.truncate(self.first_index())
    }
}

impl fmt::Debug for Wal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let start_index = self
            .closed_segments
            .get(0)
            .map_or(0, |segment| segment.start_index);
        let end_index = self.open_segment_start_index() + self.open_segment.segment.len() as u64;
        write!(
            f,
            "Wal {{ path: {:?}, segment-count: {}, entries: [{}, {})  }}",
            &self.path,
            self.closed_segments.len() + 1,
            start_index,
            end_index
        )
    }
}

// 将一个“打开的”segment转换为“关闭的”segment
fn close_segment(mut segment: OpenSegment, start_index: u64) -> Result<ClosedSegment> {
    let new_path = segment
        .segment
        .path()
        .with_file_name(format!("closed-{start_index}"));
    segment.segment.rename(new_path)?;
    Ok(ClosedSegment {
        start_index,
        segment: segment.segment,
    })
}

fn open_dir_entry(entry: fs::DirEntry) -> Result<Option<WalSegment>> {
    let metadata = entry.metadata()?;
    let error = || {
        Error::new(
            ErrorKind::InvalidData,
            format!("unexpected entry in wal directory: {:?}", entry.path()),
        )
    };

    if !metadata.is_file() {
        return Err(error());
    }

    let filename = entry.file_name().into_string().map_err(|_| error())?;
    match filename.split_once('-') {
        Some(("tmp", _)) => {
            // remove temporary files.
            fs::remove_file(entry.path())?;
            Ok(None)
        }
        Some(("open", id)) => {
            let id = u64::from_str(id).map_err(|_| error())?;
            let segment = Segment::open(entry.path())?;
            Ok(Some(WalSegment::Open(OpenSegment { segment, id })))
        }
        Some(("closed", start)) => {
            let start = u64::from_str(start).map_err(|_| error())?;
            let segment = Segment::open(entry.path())?;
            Ok(Some(WalSegment::Closed(ClosedSegment {
                start_index: start,
                segment,
            })))
        }
        _ => Ok(None), // Ignore other files.
    }
}

struct SegmentCreator {
    // 接收新段的通道
    rx: Option<Receiver<OpenSegment>>,
    // 段创建线程
    // 用于在失败时检索错误。
    thread: Option<thread::JoinHandle<Result<()>>>,
}

impl SegmentCreator {
    /// Creates a new segment creator.
    ///
    /// The segment creator must be started before new segments will be created.
    pub fn new<P>(
        dir: P,
        existing: Vec<OpenSegment>,
        segment_capacity: usize,
        segment_queue_len: usize,
    ) -> SegmentCreator
        where
            P: AsRef<Path>,
    {
        let (tx, rx) = crossbeam_channel::bounded(segment_queue_len);

        let dir = dir.as_ref().to_path_buf();
        let thread = thread::spawn(move || create_loop(tx, dir, segment_capacity, existing));
        SegmentCreator {
            rx: Some(rx),
            thread: Some(thread),
        }
    }

    // 检索下一个段。
    pub fn next(&mut self) -> Result<OpenSegment> {
        self.rx.as_mut().unwrap().recv().map_err(|_| {
            match self.thread.take().map(|join_handle| join_handle.join()) {
                Some(Ok(Err(error))) => error,
                None => Error::new(ErrorKind::Other, "segment creator thread already failed"),
                Some(Ok(Ok(()))) => unreachable!(
                    "segment creator thread finished without an error,
                                                  but the segment creator is still live"
                ),
                Some(Err(_)) => unreachable!("segment creator thread panicked"),
            }
        })
    }
}

impl Drop for SegmentCreator {
    fn drop(&mut self) {
        drop(self.rx.take());
        if let Some(join_handle) = self.thread.take() {
            if let Err(error) = join_handle.join() {}
        }
    }
}

fn create_loop(
    tx: Sender<OpenSegment>,
    mut path: PathBuf,
    capacity: usize,
    mut existing_segments: Vec<OpenSegment>,
) -> Result<()> {
    // Ensure the existing segments are in ID order.
    existing_segments.sort_by(|a, b| a.id.cmp(&b.id));

    let mut cont = true;
    let mut id = 0;

    for segment in existing_segments {
        id = segment.id;
        if tx.send(segment).is_err() {
            cont = false;
            break;
        }
    }

    // Directory being a file only applies to Linux
    #[cfg(not(target_os = "windows"))]
        let dir = File::open(&path)?;

    while cont {
        id += 1;
        path.push(format!("open-{id}"));
        let segment = OpenSegment {
            id,
            segment: Segment::create(&path, capacity)?,
        };
        path.pop();
        // Sync the directory, guaranteeing that the segment file is durably
        // stored on the filesystem.
        #[cfg(not(target_os = "windows"))]
        dir.sync_all()?;
        cont = tx.send(segment).is_ok();
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;
    use quickcheck::TestResult;

    use crate::{OpenSegment, SegmentCreator};
    use crate::segment::Segment;
    use crate::test_utils::EntryGenerator;


    use super::{Wal, WalOptions};


    #[test]
    fn test_generate_empty_wal() {
        fs::create_dir_all("./temp/wal").unwrap();
        let dir = PathBuf::from("./temp/wal");
        let options = WalOptions {
            segment_capacity: 80,
            segment_queue_len: 3,
        };

        // Create empty wal with initial id 10.
        let init_offset = 10;
        Wal::generate_empty_wal_starting_at_index(dir.clone(), &options, init_offset).unwrap();

        let mut wal = Wal::with_options(dir.clone(), &options).unwrap();

        let first_index = wal.first_index();
        let last_index = wal.last_index();
        let num_entries = wal.num_entries();

        assert!(first_index <= last_index);
        assert_eq!(num_entries, 0);

        let next_entry: Vec<u8> = vec![1, 2, 3];
        let op = wal.append(&next_entry).unwrap();

        assert!(op > init_offset);

        let first_index = wal.first_index();
        let last_index = wal.last_index();
        let num_entries = wal.num_entries();

        assert!(first_index <= last_index);
        assert_eq!(num_entries, 1);

        wal.append(&next_entry).unwrap();
        wal.append(&next_entry).unwrap();

        let first_index = wal.first_index();
        let last_index = wal.last_index();
        let num_entries = wal.num_entries();

        assert!(first_index <= last_index);
        assert_eq!(num_entries, 3);
    }

    #[test]
    fn test_create_empty_wal_with_initial_id() {
        fs::create_dir_all("./temp/wal").unwrap();
        let dir = PathBuf::from("./temp/wal");
        let options = WalOptions {
            segment_capacity: 80,
            segment_queue_len: 3,
        };
        // Create empty wal with initial id 10.
        let init_offset = 10;
        Wal::generate_empty_wal_starting_at_index(dir.clone(), &options, init_offset).unwrap();

        let mut wal = Wal::with_options(dir.clone(), &options).unwrap();

        let last_index = wal.last_index();

        assert!(last_index > init_offset);

        assert_eq!(wal.num_entries(), 0);

        let next_entry: Vec<u8> = vec![1, 2, 3];

        wal.append(&next_entry).unwrap();

        let last_index = wal.last_index();
        assert_eq!(last_index, init_offset + 1);

        assert_eq!(wal.num_entries(), 1);

        let entry_count = 50;

        let entries = EntryGenerator::new().take(entry_count).collect::<Vec<_>>();

        for entry in &entries {
            wal.append(entry).unwrap();
        }

        let last_index = wal.last_index();
        assert_eq!(last_index, init_offset + 1 + entry_count as u64);

        assert_eq!(wal.num_entries(), 1 + entry_count as u64);

        // read random entry back to make sure it is correct.
        {
            let entry_index = init_offset + 1;
            let entry = wal.entry(entry_index).unwrap();
            assert_eq!(next_entry[..], entry[..]);

            let entry_index = init_offset + 1 + entry_count as u64;
            let entry = wal.entry(entry_index).unwrap();
            assert_eq!(entries[entry_count - 1][..], entry[..]);

            let entry_index = init_offset + 1 + 10;
            let entry = wal.entry(entry_index).unwrap();
            assert_eq!(entries[9][..], entry[..]);
        }

        wal.prefix_truncate(init_offset).unwrap();

        assert_eq!(wal.num_entries(), entry_count as u64 + 1);

        wal.prefix_truncate(init_offset + 20).unwrap();

        assert!(wal.num_entries() < entry_count as u64 + 1);

        let truncate_index = init_offset + 30;
        wal.truncate(truncate_index).unwrap();

        let last_index = wal.last_index();
        assert_eq!(last_index, truncate_index - 1);
    }

    #[test]
    fn check_wal() {
        fn wal(entry_count: u8) -> TestResult {
            fs::create_dir_all("./temp/wal").unwrap();
            let dir = PathBuf::from("./temp/wal");
            let mut wal = Wal::with_options(
                dir.clone(),
                &WalOptions {
                    segment_capacity: 80,
                    segment_queue_len: 3,
                },
            )
                .unwrap();
            let entries = EntryGenerator::new()
                .take(entry_count as usize)
                .collect::<Vec<_>>();

            for entry in &entries {
                wal.append(entry).unwrap();
            }

            for (index, expected) in entries.iter().enumerate() {
                match wal.entry(index as u64) {
                    Some(ref entry) if entry[..] != expected[..] => return TestResult::failed(),
                    None => return TestResult::failed(),
                    _ => (),
                }
            }
            fs::remove_dir_all(dir).unwrap();
            TestResult::passed()
        }

        quickcheck::quickcheck(wal as fn(u8) -> TestResult);
    }

    #[test]
    fn check_last_index() {
        fn check(entry_count: u8) -> TestResult {
            fs::create_dir_all("./temp/wal").unwrap();
            let dir = PathBuf::from("./temp/wal");
            let mut wal = Wal::with_options(
                dir.clone(),
                &WalOptions {
                    segment_capacity: 80,
                    segment_queue_len: 3,
                },
            )
                .unwrap();
            let entries = EntryGenerator::new()
                .take(entry_count as usize)
                .collect::<Vec<_>>();

            for entry in &entries {
                wal.append(entry).unwrap();
            }
            if entries.is_empty() {
                assert_eq!(wal.last_index(), 0);
            } else {
                assert_eq!(wal.last_index(), entries.len() as u64 - 1);
            }

            let last_index = wal.last_index();
            if wal.entry(last_index).is_none() && wal.num_entries() != 0 {
                return TestResult::failed();
            }
            if wal.entry(last_index + 1).is_some() {
                return TestResult::failed();
            }
            fs::remove_dir_all(dir).unwrap();
            TestResult::passed()
        }

        quickcheck::quickcheck(check as fn(u8) -> TestResult)
    }

    #[test]
    fn check_clear() {
        fn check(entry_count: u8) -> TestResult {
            fs::create_dir_all("./temp/wal").unwrap();
            let dir = PathBuf::from("./temp/wal");
            let mut wal = Wal::with_options(
                dir.clone(),
                &WalOptions {
                    segment_capacity: 80,
                    segment_queue_len: 3,
                },
            )
                .unwrap();
            let entries = EntryGenerator::new()
                .take(entry_count as usize)
                .collect::<Vec<_>>();

            for entry in &entries {
                wal.append(entry).unwrap();
            }
            wal.clear().unwrap();
            TestResult::from_bool(wal.num_entries() == 0)
        }

        quickcheck::quickcheck(check as fn(u8) -> TestResult)
    }

    /// Check that the Wal will read previously written entries.
    #[test]
    fn check_reopen() {
        fn wal(entry_count: u8) -> TestResult {
            let entries = EntryGenerator::new()
                .take(entry_count as usize)
                .collect::<Vec<_>>();
            fs::create_dir_all("./temp/wal").unwrap();
            let dir = PathBuf::from("./temp/wal");
            {
                let mut wal = Wal::with_options(
                    dir.clone(),
                    &WalOptions {
                        segment_capacity: 80,
                        segment_queue_len: 3,
                    },
                )
                    .unwrap();
                for entry in &entries {
                    let _ = wal.append(entry);
                }
            }

            {
                // Create fake temp file to simulate a crash.
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(dir.clone().join("tmp-open-123"))
                    .unwrap();

                let _ = file.write(b"123").unwrap();
            }

            let wal = Wal::with_options(
                dir.clone(),
                &WalOptions {
                    segment_capacity: 80,
                    segment_queue_len: 3,
                },
            )
                .unwrap();
            // Check that all of the entries are present.
            for (index, expected) in entries.iter().enumerate() {
                match wal.entry(index as u64) {
                    Some(ref entry) if entry[..] != expected[..] => return TestResult::failed(),
                    None => return TestResult::failed(),
                    _ => (),
                }
            }
            fs::remove_dir_all(dir).unwrap();
            TestResult::passed()
        }

        quickcheck::quickcheck(wal as fn(u8) -> TestResult);
    }

    #[test]
    fn check_truncate() {
        fn truncate(entry_count: u8, truncate: u8) -> TestResult {
            if truncate > entry_count {
                return TestResult::discard();
            }
            fs::create_dir_all("./temp/wal").unwrap();
            let dir = PathBuf::from("./temp/wal");
            let mut wal = Wal::with_options(
                dir.clone(),
                &WalOptions {
                    segment_capacity: 80,
                    segment_queue_len: 3,
                },
            )
                .unwrap();
            let entries = EntryGenerator::new()
                .take(entry_count as usize)
                .collect::<Vec<_>>();

            for entry in &entries {
                if let Err(error) = wal.append(entry) {
                    return TestResult::error(error.to_string());
                }
            }

            wal.truncate(truncate as u64).unwrap();

            for (index, expected) in entries.iter().take(truncate as usize).enumerate() {
                match wal.entry(index as u64) {
                    Some(ref entry) if entry[..] != expected[..] => return TestResult::failed(),
                    None => return TestResult::failed(),
                    _ => (),
                }
            }
            fs::remove_dir_all(dir).unwrap();
            TestResult::from_bool(wal.entry(truncate as u64).is_none())
        }

        quickcheck::quickcheck(truncate as fn(u8, u8) -> TestResult);
    }

    #[test]
    fn check_prefix_truncate() {
        fn prefix_truncate(entry_count: u8, until: u8) -> TestResult {
            if until > entry_count {
                return TestResult::discard();
            }
            fs::create_dir_all("./temp/wal").unwrap();
            let dir = PathBuf::from("./temp/wal");
            let mut wal = Wal::with_options(
                dir.clone(),
                &WalOptions {
                    segment_capacity: 80,
                    segment_queue_len: 3,
                },
            )
                .unwrap();
            let entries = EntryGenerator::new()
                .take(entry_count as usize)
                .collect::<Vec<_>>();

            for entry in &entries {
                wal.append(entry).unwrap();
            }

            wal.prefix_truncate(until as u64).unwrap();

            let num_entries = wal.num_entries() as u8;
            fs::remove_dir_all(dir).unwrap();
            TestResult::from_bool(num_entries <= entry_count && num_entries >= entry_count - until)
        }
        quickcheck::quickcheck(prefix_truncate as fn(u8, u8) -> TestResult);
    }

    #[test]
    fn test_append() {

        fs::create_dir_all("./temp/wal").unwrap();
        let dir = PathBuf::from("./temp/wal");
        let mut wal = Wal::open(dir.clone()).unwrap();

        let entry: &[u8] = &[42u8; 4096];
        for _ in 1..10 {
            wal.append(&entry).unwrap();
        }
    }

    #[test]
    fn test_truncate_flush() {

        fs::create_dir_all("./temp/wal").unwrap();
        let dir = PathBuf::from("./temp/wal");
        // 2 entries should fit in each segment
        let mut wal = Wal::with_options(
            dir.clone(),
            &WalOptions {
                segment_capacity: 4096,
                segment_queue_len: 3,
            },
        )
            .unwrap();

        let entry: [u8; 2000] = [42u8; 2000];
        // wal is empty
        assert!(wal.entry(0).is_none());

        // add 10 entries
        for i in 0..10 {
            assert_eq!(i, wal.append(&&entry[..]).unwrap());
        }

        // 4 closed segments
        assert_eq!(wal.num_entries(), 10);
        assert_eq!(wal.first_index(), 0);
        assert_eq!(wal.last_index(), 9);
        assert_eq!(wal.closed_segments.len(), 4); // 4 x 2 entries
        assert_eq!(wal.closed_segments[0].segment.len(), 2);
        assert_eq!(wal.closed_segments[1].segment.len(), 2);
        assert_eq!(wal.closed_segments[2].segment.len(), 2);
        assert_eq!(wal.closed_segments[3].segment.len(), 2);
        assert_eq!(wal.open_segment.segment.len(), 2); // 1 x 2 entries

        // first flush to set `flush_offset
        wal.flush_open_segment().unwrap();

        // content unchanged after flushing
        assert_eq!(wal.num_entries(), 10);
        assert_eq!(wal.first_index(), 0);
        assert_eq!(wal.last_index(), 9);
        assert_eq!(wal.closed_segments.len(), 4); // 4 x 2 entries
        assert_eq!(wal.closed_segments[0].segment.len(), 2);
        assert_eq!(wal.closed_segments[1].segment.len(), 2);
        assert_eq!(wal.closed_segments[2].segment.len(), 2);
        assert_eq!(wal.closed_segments[3].segment.len(), 2);
        assert_eq!(wal.open_segment.segment.len(), 2); // 1 x 2 entries

        wal.truncate(9).unwrap();

        assert_eq!(wal.open_segment.segment.len(), 1); // 1 x 2 entries

        // truncate half of it
        wal.truncate(5).unwrap();

        // assert truncation
        for i in 5..10 {
            assert!(wal.entry(i).is_none());
        }

        // flush again with `flush_offset` > segment size
        wal.flush_open_segment().unwrap();

        assert_eq!(wal.num_entries(), 5); // 5 entries removed
        assert_eq!(wal.first_index(), 0);
        assert_eq!(wal.last_index(), 4);
        assert_eq!(wal.closed_segments.len(), 3); // (0, 1) + (2, 3) + (4, empty slot)
        assert_eq!(wal.closed_segments[0].segment.len(), 2);
        assert_eq!(wal.closed_segments[1].segment.len(), 2);
        assert_eq!(wal.closed_segments[2].segment.len(), 1);
        assert_eq!(wal.open_segment.segment.len(), 0); // empty open segment

        // add 5 more entries
        for i in 0..5 {
            assert_eq!(i + 5, wal.append(&&entry[..]).unwrap());
        }

        // 5 closed segments
        assert_eq!(wal.num_entries(), 10);
        assert_eq!(wal.first_index(), 0);
        assert_eq!(wal.last_index(), 9);
        assert_eq!(wal.closed_segments.len(), 5);
        assert_eq!(wal.closed_segments[0].segment.len(), 2); // 1,2
        assert_eq!(wal.closed_segments[1].segment.len(), 2); // 3
        assert_eq!(wal.closed_segments[2].segment.len(), 1); // 4 empty slot due to truncation
        assert_eq!(wal.closed_segments[3].segment.len(), 2); // 5, 6
        assert_eq!(wal.closed_segments[4].segment.len(), 2); // 7, 8
        assert_eq!(wal.open_segment.segment.len(), 1); // 9

        eprintln!("wal: {wal:?}");
        eprintln!("wal open: {:?}", wal.open_segment);
        eprintln!("wal closed: {:?}", wal.closed_segments);

        // test persistence
        drop(wal);
        let wal = Wal::open(dir.clone()).unwrap();
        assert_eq!(wal.num_entries(), 10);
        assert_eq!(wal.first_index(), 0);
        assert_eq!(wal.last_index(), 9);
        assert_eq!(wal.closed_segments.len(), 5);
        assert_eq!(wal.closed_segments[0].segment.len(), 2);
        assert_eq!(wal.closed_segments[1].segment.len(), 2);
        assert_eq!(wal.closed_segments[2].segment.len(), 1); // previously half truncated
        assert_eq!(wal.closed_segments[3].segment.len(), 2);
        assert_eq!(wal.closed_segments[4].segment.len(), 2);
        assert_eq!(wal.open_segment.segment.len(), 1);
    }

    #[test]
    fn test_segment_creator() {
        fs::create_dir_all("./temp/wal").unwrap();
        let dir = PathBuf::from("./temp/wal");

        let segments = vec![OpenSegment {
            id: 3,
            segment: Segment::create(dir.clone().join("open-3"), 1024).unwrap(),
        }];

        let mut creator = SegmentCreator::new(dir.clone(), segments, 1024, 1);
        for i in 3..10 {
            assert_eq!(i, creator.next().unwrap().id);
        }
    }

    #[test]
    fn test_record_id_preserving() {
        let entry_count = 55;
        fs::create_dir_all("./temp/wal").unwrap();
        let dir = PathBuf::from("./temp/wal");
        let options = WalOptions {
            segment_capacity: 512,
            segment_queue_len: 3,
        };

        let mut wal = Wal::with_options(dir.clone(), &options).unwrap();
        let entries = EntryGenerator::new().take(entry_count).collect::<Vec<_>>();

        for entry in &entries {
            wal.append(entry).unwrap();
        }
        let closed_segments = wal.closed_segments.len();
        let start_index = wal.open_segment_start_index();

        wal.prefix_truncate(25).unwrap();
        let half_trunk_closed_segments = wal.closed_segments.len();
        let half_trunk_start_index = wal.open_segment_start_index();

        wal.prefix_truncate((entry_count - 2) as u64).unwrap();
        let full_trunk_closed_segments = wal.closed_segments.len();
        let full_trunk_start_index = wal.open_segment_start_index();

        assert!(closed_segments > half_trunk_closed_segments);
        assert!(half_trunk_closed_segments > full_trunk_closed_segments);

        assert_eq!(start_index, half_trunk_start_index);
        assert_eq!(start_index, full_trunk_start_index);
    }

    #[test]
    fn test_offset_after_open() {
        let entry_count = 55;
        fs::create_dir_all("./temp/wal").unwrap();
        let dir = PathBuf::from("./temp/wal");
        let options = WalOptions {
            segment_capacity: 512,
            segment_queue_len: 3,
        };
        let start_index;
        {
            let mut wal = Wal::with_options(dir.clone(), &options).unwrap();
            let entries = EntryGenerator::new().take(entry_count).collect::<Vec<_>>();

            for entry in &entries {
                wal.append(entry).unwrap();
            }
            start_index = wal.open_segment_start_index();
            wal.prefix_truncate(25).unwrap();
            assert_eq!(start_index, wal.open_segment_start_index());
        }
        {
            let wal2 = Wal::with_options(dir.clone(), &options).unwrap();
            assert_eq!(start_index, wal2.open_segment_start_index());
        }
    }
    #[test]
    fn test_truncate() {
        fs::create_dir_all("./temp/wal").unwrap();
        let dir = PathBuf::from("./temp/wal");
        // 2 entries should fit in each segment
        let mut wal = Wal::with_options(
            dir,
            &WalOptions {
                segment_capacity: 4096,
                segment_queue_len: 3,
            },
        )
            .unwrap();

        let entry: [u8; 2000] = [42u8; 2000];

        for truncate_index in 0..10 {
            assert!(wal.entry(0).is_none());
            for i in 0..10 {
                assert_eq!(i, wal.append(&&entry[..]).unwrap());
            }

            wal.truncate(truncate_index).unwrap();

            assert!(wal.entry(truncate_index).is_none());

            if truncate_index > 0 {
                assert!(wal.entry(truncate_index - 1).is_some());
            }
            wal.truncate(0).unwrap();
        }
    }

}