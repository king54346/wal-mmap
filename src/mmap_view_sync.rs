use memmap2::{MmapMut, MmapOptions};
use std::cell::UnsafeCell;
use std::fmt;
use std::fs::File;
use std::io::Result;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

/// ported from https://github.com/danburkert/memmap-rs in version 0.5.2

/// 一个线程安全的内存映射视图。
/// view可以拆分为不相交的范围，每个范围将共享基础内存映射。
pub struct MmapViewSync {
    inner: Arc<UnsafeCell<MmapMut>>,
    offset: usize,
    len: usize,
}

impl MmapViewSync {
    pub fn from_file(file: &File, offset: usize, capacity: usize) -> Result<MmapViewSync> {
        let mmap = unsafe {
            MmapOptions::new()
                .offset(offset as u64)
                .len(capacity)
                .map_mut(file)?
        };

        Ok(mmap.into())
    }

    #[allow(dead_code)]
    pub fn anonymous(capacity: usize) -> Result<MmapViewSync> {
        let mmap = MmapOptions::new().len(capacity).map_anon()?;

        Ok(mmap.into())
    }

    /// 分割视图为不相交的片段
    /// 提供的偏移量必须小于视图的长度。
    #[allow(dead_code)]
    pub fn split_at(self, offset: usize) -> Result<(MmapViewSync, MmapViewSync)> {
        if self.len < offset {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "mmap view split offset must be less than the view length",
            ));
        }
        let MmapViewSync {
            inner,
            offset: self_offset,
            len: self_len,
        } = self;
        Ok((
            MmapViewSync {
                inner: inner.clone(),
                offset: self_offset,
                len: offset,
            },
            MmapViewSync {
                inner,
                offset: self_offset + offset,
                len: self_len - offset,
            },
        ))
    }


    /// 将此视图的范围限制为提供的偏移量和长度。
    /// 提供的范围必须是当前范围的子集（`offset + len < view.len()`）。
    pub fn restrict(&mut self, offset: usize, len: usize) -> Result<()> {
        if offset + len > self.len {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "mmap view may only be restricted to a subrange \
                                       of the current view",
            ));
        }
        self.offset += offset;
        self.len = len;
        Ok(())
    }

    /// Get a reference to the inner mmap.
    /// 获得一个内部mmap的引用。
    /// 调用者必须确保在`offset`/`len`范围之外的内存不被访问。
    fn inner(&self) -> &MmapMut {
        unsafe { &*self.inner.get() }
    }

    /// Get a mutable reference to the inner mmap.
    /// 获得一个内部mmap的可变引用。
    /// 调用者必须确保在`offset`/`len`范围之外的内存不被访问。
    #[allow(clippy::mut_from_ref)]
    fn inner_mut(&self) -> &mut MmapMut {
        unsafe { &mut *self.inner.get() }
    }


    /// 刷新内存映射文件的修改到磁盘。
    /// 当这个函数返回非错误结果时，所有对文件支持的内存映射视图的未完成修改都保证被持久存储。
    /// 文件的元数据（包括最后修改时间戳）可能不会被更新。
    pub fn flush(&self) -> Result<()> {
        self.inner_mut().flush_range(self.offset, self.len)
    }

    /// Returns the length of the memory map view.
    pub fn len(&self) -> usize {
        self.len
    }

    /// 返回内存映射文件的不可变切片。
    /// 调用者必须确保文件没有并发修改。
    pub unsafe fn as_slice(&self) -> &[u8] {
        &self.inner()[self.offset..self.offset + self.len]
    }

    /// 返回内存映射文件的可变切片。
    /// 调用者必须确保文件没有并发访问。
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.inner_mut()[self.offset..self.offset + self.len]
    }

    /// 克隆内存映射的视图。
    /// 底层内存映射是共享的，因此调用者必须确保视图下的内存没有非法别名。
    pub unsafe fn clone(&self) -> MmapViewSync {
        MmapViewSync {
            inner: self.inner.clone(),
            offset: self.offset,
            len: self.len,
        }
    }
}

impl From<MmapMut> for MmapViewSync {
    fn from(mmap: MmapMut) -> MmapViewSync {
        let len = mmap.len();
        MmapViewSync {
            inner: Arc::new(UnsafeCell::new(mmap)),
            offset: 0,
            len,
        }
    }
}

impl fmt::Debug for MmapViewSync {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MmapViewSync {{ offset: {}, len: {} }}",
            self.offset, self.len
        )
    }
}

unsafe impl Sync for MmapViewSync {}
unsafe impl Send for MmapViewSync {}

#[cfg(test)]
mod test {
    use std::{fs, mem};
    use std::io::{Read, Write};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::thread;
    use tempfile::Builder;

    use super::*;

    #[test]
    fn view() {
        let len = 128;
        let split = 32;
        let mut view = MmapViewSync::anonymous(len).unwrap();
        let incr = (0..len).map(|n| n as u8).collect::<Vec<_>>();
        // write values into the view
        unsafe { view.as_mut_slice() }.write_all(&incr[..]).unwrap();

        let (mut view1, view2) = view.split_at(32).unwrap();
        assert_eq!(view1.len(), split);
        assert_eq!(view2.len(), len - split);

        assert_eq!(&incr[0..split], unsafe { view1.as_slice() });
        assert_eq!(&incr[split..], unsafe { view2.as_slice() });

        view1.restrict(10, 10).unwrap();
        assert_eq!(&incr[10..20], unsafe { view1.as_slice() })
    }

    #[test]
    fn view_sync() {
        let len = 128;
        let split = 32;
        let mut view = MmapViewSync::anonymous(len).unwrap();
        let incr = (0..len).map(|n| n as u8).collect::<Vec<_>>();
        // write values into the view
        unsafe { view.as_mut_slice() }.write_all(&incr[..]).unwrap();

        let (mut view1, view2) = view.split_at(32).unwrap();
        assert_eq!(view1.len(), split);
        assert_eq!(view2.len(), len - split);

        assert_eq!(&incr[0..split], unsafe { view1.as_slice() });
        assert_eq!(&incr[split..], unsafe { view2.as_slice() });

        view1.restrict(10, 10).unwrap();
        assert_eq!(&incr[10..20], unsafe { view1.as_slice() })
    }

    #[test]
    fn view_write() {
        let len = 131072; // 256KiB
        let split = 66560; // 65KiB + 10B

        let tempdir = Builder::new().prefix("mmap").tempdir().unwrap();
        let path = tempdir.path().join("mmap");
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        file.set_len(len).unwrap();

        let incr = (0..len).map(|n| n as u8).collect::<Vec<_>>();
        let incr1 = incr[0..split].to_owned();
        let incr2 = incr[split..].to_owned();

        let view: MmapViewSync = MmapViewSync::from_file(&file, 0, len as usize).unwrap();
        let (mut view1, mut view2) = view.split_at(split).unwrap();

        let join1 = thread::spawn(move || {
            let _written = unsafe { view1.as_mut_slice() }.write(&incr1).unwrap();
            view1.flush().unwrap();
        });

        let join2 = thread::spawn(move || {
            let _written = unsafe { view2.as_mut_slice() }.write(&incr2).unwrap();
            view2.flush().unwrap();
        });

        join1.join().unwrap();
        join2.join().unwrap();

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();

        assert_eq!(incr, &buf[..]);
    }

    #[test]
    fn view_sync_send() {
        let view: Arc<MmapViewSync> = Arc::new(MmapViewSync::anonymous(128).unwrap());
        thread::spawn(move || unsafe {
            view.as_slice();
        });
    }
    use winapi::um::memoryapi::GetLargePageMinimum;
    use winapi::um::sysinfoapi::{GetSystemInfo, SYSTEM_INFO};
    #[test]
    fn test(){
        // 创建一个文件
        let len = 131072; // 256KiB
        let split = 66560; // 65KiB + 10B
        // path
        let path = PathBuf::from("test.txt");
        println!("{:?}", path);
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        file.set_len(len).unwrap();
        let mut view: MmapViewSync = MmapViewSync::from_file(&file, 0, len as usize).unwrap();
        let data= "hello world";
        unsafe { view.as_mut_slice() }.write(data.as_bytes()).expect("TODO: panic message");
        view.flush().unwrap();
        // 最小大页面尺寸 2M
        let a = unsafe {GetLargePageMinimum()};
        println!("a:{}",a);
        //     SYSTEM_INFO
        let mut system_info= unsafe { mem::zeroed() };
        unsafe { GetSystemInfo(&mut system_info) };

        println!("{:?}",system_info.dwPageSize as u32);
        println!("{:?}",system_info.dwAllocationGranularity as usize as u32);
    }
}