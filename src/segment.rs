use std::cmp::Ordering;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::ops::Deref;
#[cfg(target_os = "windows")]
use std::os::windows::io::AsRawHandle;
use std::path::{Path, PathBuf};
use std::ptr;
use std::thread;
#[cfg(target_os = "windows")]
use std::time::Duration;

use byteorder::{ByteOrder, LittleEndian};
#[cfg(target_os = "windows")]
use winapi::um::ioapiset::DeviceIoControl;
#[cfg(target_os = "windows")]
use winapi::um::winioctl::FSCTL_SET_SPARSE;

use crate::mmap_view_sync::MmapViewSync;

pub fn entry_overhead(len: usize) -> usize {
    padding(len) + HEADER_LEN + CRC_LEN
}

/// Returns the fixed-overhead of segment metadata.
pub fn segment_overhead() -> usize {
    HEADER_LEN
}

pub fn copy_memory(src: &[u8], dst: &mut [u8]) {
    let len_src = src.len();
    assert!(dst.len() >= len_src);
    unsafe {
        ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), len_src);
    }
}

// 填充8位对齐后的长度
fn padding(len: usize) -> usize {
    4usize.wrapping_sub(len) & 7
}

#[cfg(target_os = "windows")]
fn allocate(file: &File, size: u64) -> Result<()> {
    // 获取Windows文件句柄
    let handle = file.as_raw_handle();

    // 设置文件为稀疏文件
    let mut bytes_returned = 0;
    let success = unsafe {
        DeviceIoControl(
            handle as *mut _,
            FSCTL_SET_SPARSE,
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            0,
            &mut bytes_returned,
            std::ptr::null_mut(),
        ) != 0
    };

    if success {
        // 设置文件大小
        file.set_len(size)?;
        println!("Sparse file created successfully.");
        Ok(())
    } else {
        Err(Error::last_os_error())
    }
}

/// The magic bytes and version tag of the segment header.
const SEGMENT_MAGIC: &[u8; 3] = b"wal";
const SEGMENT_VERSION: u8 = 0;

/// The length of both the segment and entry header.
const HEADER_LEN: usize = 8;

/// The length of a CRC value.
const CRC_LEN: usize = 4;


pub struct Entry {
    view: MmapViewSync,
}

impl Deref for Entry {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        unsafe { self.view.as_slice() }
    }
}

impl fmt::Debug for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Entry {{ len: {} }}", self.view.len())
    }
}

// clone
#[derive(Debug)]
pub struct Segment {
    /// The segment file buffer.
    mmap: MmapViewSync,
    /// The segment file path.
    path: PathBuf,
    /// Index of entry offset and lengths.
    index: Vec<(usize, usize)>,
    /// The crc of the last appended entry.
    crc: u32,
    /// Offset of last flush.
    flush_offset: usize,
}

impl Segment {
    pub fn create<P>(path: P, capacity: usize) -> Result<Segment> where P: AsRef<Path> {
        let file_name = path
            .as_ref()
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .expect("Path to WAL segment file provided");

        let tmp_file_path = match path.as_ref().parent() {
            Some(parent) => parent.join(format!("tmp-{file_name}")),
            None => PathBuf::from(format!("tmp-{file_name}")),
        };

        // Round capacity down to the nearest 8-byte alignment, since the
        // segment would not be able to take advantage of the space.
        let capacity = capacity & !7;
        if capacity < HEADER_LEN  {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("invalid segment capacity: {capacity}"),
            ));
        }
        let seed = rand::random();

        {
            // Prepare properly formatted segment in a temporary file, so in case of failure it won't be corrupted.
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&tmp_file_path)?;

            // fs4 provides some cross-platform bindings which help for Windows.
            #[cfg(target_os = "windows")]
            {
                allocate(&file, capacity as u64)?;
            }

            // For all unix systems WAL can just use ftruncate directly
            #[cfg(unix)]
            {
                fs::ftruncate(&file, capacity)?;
            }

            let mut mmap = MmapViewSync::from_file(&file, 0, capacity as usize)?;
            {
                let segment = unsafe { &mut mmap.as_mut_slice() };
                copy_memory(SEGMENT_MAGIC, segment);
                segment[3] = SEGMENT_VERSION;
                LittleEndian::write_u32(&mut segment[4..], seed);
            }

            // From "man 2 close":
            // > A successful close does not guarantee that the data has been successfully saved to disk, as the kernel defers writes.
            // So we need to flush magic header manually to ensure that it is written to disk.
            mmap.flush()?;

            // Manually sync each file in Windows since sync-ing cannot be done for the whole directory.
            #[cfg(target_os = "windows")]
            {
                file.sync_all()?;
            }
        };

        // File renames are atomic, so we can safely rename the temporary file to the final file.
        fs::rename(&tmp_file_path, &path)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let mmap = MmapViewSync::from_file(&file, 0, capacity as usize)?;

        let segment = Segment {
            mmap,
            path: path.as_ref().to_path_buf(),
            index: Vec::new(),
            crc: seed,
            flush_offset: 0,
        };
        Ok(segment)
    }
    pub fn open<P>(path: P) -> Result<Segment> where P: AsRef<Path> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)?;
        let capacity = file.metadata()?.len();
        if capacity > usize::MAX as u64 || capacity < HEADER_LEN as u64 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("invalid segment capacity: {capacity}"),
            ));
        }

        // Round capacity down to the nearest 8-byte alignment, since the
        // segment would not be able to take advantage of the space.
        let capacity = capacity as usize & !7;
        let mmap = MmapViewSync::from_file(&file, 0, capacity)?;

        let mut index = Vec::new();
        let mut crc;
        {
            // 匹配segment，填充包含每个条目的偏移量和长度的索引，以及最新的CRC值。
            // 如果任何条目的CRC不匹配，则解析停止，并且文件的其余部分被视为空。

            let segment = unsafe { mmap.as_slice() };

            if &segment[0..3] != SEGMENT_MAGIC {
                return Err(Error::new(ErrorKind::InvalidData, "Illegal segment header"));
            }

            if segment[3] != SEGMENT_VERSION {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Segment version unsupported: {}", segment[3]),
                ));
            }

            crc = LittleEndian::read_u32(&segment[4..]);
            let mut offset = HEADER_LEN;
            // 遍历segment中的所有数据项，对每个数据项进行CRC校验
            while offset + HEADER_LEN + CRC_LEN < capacity {
                let len = LittleEndian::read_u64(&segment[offset..]) as usize;
                let padding = padding(len);
                let padded_len = len + padding;// 计算数据项的长度
                // offset 为 u64的length+data+CRC
                if offset + HEADER_LEN + padded_len + CRC_LEN > capacity {
                    break;
                }
                let mut digest = crc32fast::Hasher::new_with_initial(crc);
                digest.update(&segment[offset..offset + HEADER_LEN + padded_len]);
                let entry_crc = digest.finalize(); // 计算CRC
                let stored_crc = LittleEndian::read_u32(&segment[offset + HEADER_LEN + padded_len..]); // 读取CRC
                if entry_crc != stored_crc {
                    // stored_crc 为0，说明后面没有entry了
                    if stored_crc != 0 {
                        println!(
                            "CRC mismatch at offset {}: {} != {}",
                            offset,
                            entry_crc,
                            stored_crc
                        );
                    }
                    break;
                }

                crc = entry_crc;
                index.push((offset + HEADER_LEN, len));
                offset += HEADER_LEN + padded_len + CRC_LEN;
            }
        }

        let segment = Segment {
            mmap,
            path: path.as_ref().to_path_buf(),
            index,
            crc,
            flush_offset: 0,
        };
        Ok(segment)
    }
    pub fn append<T>(&mut self, entry: &T) -> Option<usize> where T: Deref<Target=[u8]> {
        // 确保空间足够
        if !self.sufficient_capacity(entry.len()) {
            return None;
        }
        // 计算需要padding的长度
        let padding = padding(entry.len());

        let padded_len = entry.len() + padding;

        let offset = self.size();

        let mut crc = self.crc;
        let mut digest = crc32fast::Hasher::new_with_initial(crc);
        // 写入数据项的长度
        LittleEndian::write_u64(&mut self.as_mut_slice()[offset..], entry.len() as u64);
        copy_memory(
            entry.deref(),
            &mut self.as_mut_slice()[offset + HEADER_LEN..],
        );
        // 如果需要padding，则在数据项后面填充0
        if padding > 0 {
            let zeros: [u8; 8] = [0; 8];
            copy_memory(
                &zeros[..padding],
                &mut self.as_mut_slice()[offset + HEADER_LEN + entry.len()..],
            );
        }
        digest.update(&self.as_slice()[offset..offset + HEADER_LEN + padded_len]);
        crc = digest.finalize();
        // 写入CRC
        LittleEndian::write_u32(
            &mut self.as_mut_slice()[offset + HEADER_LEN + padded_len..],
            crc,
        );

        self.crc = crc;
        self.index.push((offset + HEADER_LEN, entry.len()));
        Some(self.index.len() - 1)
    }

    // 返回true，如果segment有足够的剩余容量添加一个大小为entry_len的条目。
    pub fn sufficient_capacity(&self, entry_len: usize) -> bool {
        (self.capacity() - self.size())
            .checked_sub(HEADER_LEN + CRC_LEN)
            .map_or(false, |rem| rem >= entry_len + padding(entry_len))
    }

    pub fn size(&self) -> usize {
        self.index.last().map_or(HEADER_LEN, |&(offset, len)| {
            offset + len + padding(len) + CRC_LEN
        })
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { self.mmap.as_mut_slice() }
    }
    fn as_slice(&self) -> &[u8] {
        unsafe { self.mmap.as_slice() }
    }
    pub fn capacity(&self) -> usize {
        self.mmap.len()
    }

    //  返回指定索引处的段条目，如果没有这样的条目，则返回`None`条目存在。
    pub fn entry(&self, index: usize) -> Option<Entry> {
        self.index.get(index).map(|&(offset, len)| {
            let mut view = unsafe { self.mmap.clone() };
            // 仅在边界错误时失败，但段的不变性是索引始终保持有效的偏移量和长度界限。
            view.restrict(offset, len)
                .expect("illegal segment offset or length");
            Entry { view }
        })
    }
    // 截断segment中从from开始的entries ,entries 不保证立即删除，只有在segment flush之后才会真正删除
    pub fn truncate(&mut self, from: usize) {
        if from >= self.index.len() {
            return;
        }
        // Remove the index entries.
        let deleted = self.index.drain(from..).count();

        // Update the CRC.
        if self.index.is_empty() {
            self.crc = self._read_seed_crc(); // Seed
        } else {
            // Read CRC of the last entry.
            self.crc = self._read_entry_crc(self.index.len() - 1);
        }

        // And overwrite the existing data so that we will not read the data back after a crash.
        let size = self.size();
        let zeroes: [u8; 16] = [0; 16];
        copy_memory(&zeroes, &mut self.as_mut_slice()[size..]);
    }

    // 刷新最近写入的条目到持久存储
    pub fn flush(&mut self) -> Result<()> {
        let start = self.flush_offset;
        let end = self.size();

        match start.cmp(&end) {
            Ordering::Equal => {
                Ok(())
            } // nothing to flush
            Ordering::Less => {
                // flush new elements added since last flush
                let mut view = unsafe { self.mmap.clone() };
                self.set_flush_offset(end);
                view.restrict(start, end - start)?;
                view.flush()
            }
            Ordering::Greater => {
                // most likely truncated in between flushes
                // register new flush offset & flush the whole segment
                let view = unsafe { self.mmap.clone() };
                self.set_flush_offset(end);
                view.flush()
            }
        }
    }
    pub fn flush_async(&mut self) -> thread::JoinHandle<Result<()>> {
        let start = self.flush_offset();
        let end = self.size();

        match start.cmp(&end) {
            Ordering::Equal => thread::spawn(move || Ok(())), // nothing to flush
            Ordering::Less => {
                // flush new elements added since last flush
                let mut view = unsafe { self.mmap.clone() };
                self.set_flush_offset(end);

                // let log_msg = if log_enabled!(log::Level::Trace) {
                //     format!(
                //         "{:?}: async flushing byte range [{}, {})",
                //         &self, start, end
                //     )
                // } else {
                //     String::new()
                // };

                thread::spawn(move || {
                    view.restrict(start, end - start).and_then(|_| view.flush())
                })
            }
            Ordering::Greater => {
                // most likely truncated in between flushes
                // register new flush offset & flush the whole segment
                let view = unsafe { self.mmap.clone() };
                self.set_flush_offset(end);

                // let log_msg = if log_enabled!(log::Level::Trace) {
                //     format!("{:?}: async flushing after truncation", &self)
                // } else {
                //     String::new()
                // };

                thread::spawn(move || {
                    view.flush()
                })
            }
        }
    }
    fn _read_seed_crc(&self) -> u32 {
        LittleEndian::read_u32(&self.as_slice()[4..])
    }
    fn _read_entry_crc(&self, entry_id: usize) -> u32 {
        let (offset, len) = self.index[entry_id];
        let padding = padding(len);
        let padded_len = len + padding;
        LittleEndian::read_u32(&self.as_slice()[offset + padded_len..])
    }

    fn set_flush_offset(&mut self, offset: usize) {
        self.flush_offset = offset;
    }
    fn flush_offset(&self) -> usize {
        self.flush_offset
    }


    pub fn delete(self) -> Result<()> {
        #[cfg(not(target_os = "windows"))]
        {
            self.delete_unix()
        }

        #[cfg(target_os = "windows")]
        {
            self.delete_windows()
        }
    }

    #[cfg(not(target_os = "windows"))]
    fn delete_unix(self) -> Result<()> {
        fs::remove_file(&self.path).map_err(|e| {
            error!("{:?}: failed to delete segment {}", self, e);
            e
        })
    }

    #[cfg(target_os = "windows")]
    fn delete_windows(self) -> Result<()> {
        const DELETE_TRIES: u32 = 3;

        let Segment {
            mmap,
            path,
            index,
            flush_offset,
            ..
        } = self;
        let mmap_len = mmap.len();

        // Unmaps the file before `fs::remove_file` else access will be denied
        mmap.flush()?;
        std::mem::drop(mmap);

        let mut tries = 0;
        loop {
            tries += 1;
            match fs::remove_file(&path) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if tries >= DELETE_TRIES {
                        return Err(e);
                    } else {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        }
    }


    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn ensure_capacity(&mut self, entry_size: usize) -> Result<()> {
        let required_capacity =
            entry_size + padding(entry_size) + HEADER_LEN + CRC_LEN + self.size();
        // Sanity check the 8-byte alignment invariant.
        assert_eq!(required_capacity & !7, required_capacity);
        if required_capacity > self.capacity() {
            self.flush()?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .open(&self.path)?;
            // fs4 provides some cross-platform bindings which help for Windows.
            #[cfg(target_os = "windows")]
            allocate(&file, required_capacity as u64)?;
            // For all unix systems WAL can just use ftruncate directly
            #[cfg(target_os = "unix")]
            {
                rustix::fs::ftruncate(&file, required_capacity as u64)?;
            }

            let mut mmap = MmapViewSync::from_file(&file, 0, required_capacity)?;
            mem::swap(&mut mmap, &mut self.mmap);
        }
        Ok(())
    }

    pub fn rename<P>(&mut self, path: P) -> Result<()> where P: AsRef<Path>{
        fs::rename(&self.path, &path).map_err(|e| {
            println!("{:?}: failed to rename segment {}", self.path, e);
            e
        })?;
        self.path = path.as_ref().to_path_buf();
        Ok(())
    }

}

#[cfg(test)]
mod test {
    use std::path::Path;
    use crate::segment::Segment;

    #[test]
    fn test() {
        let path = Path::new("test.wal");
        let size: u64 = 64 * 1024 * 1024; // 64MB
        Segment::create(path, size as usize).unwrap();
    }

    #[test]
    fn test_open() {
        let path = Path::new("test.wal");
        let segment = Segment::open(path).unwrap();
        println!("{:?}", segment.path);
    }

    #[test]
    fn test_append() {
        let path = Path::new("test.wal");
        let mut segment = Segment::open(path).unwrap();
        let entry: &[u8] = b"hello world";

        segment.append(&entry).unwrap();
        println!("{:?}", segment.index);
    }

    #[test]
    fn test_get() {
        let path = Path::new("test.wal");
        let mut segment = Segment::open(path).unwrap();
        let entry = segment.entry(1).unwrap();
        unsafe { println!("{:?}", std::str::from_utf8(entry.view.as_slice()).unwrap()); }
    }
}