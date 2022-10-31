use crate::vfs;

use fuser::{
    KernelConfig, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyStatfs, ReplyWrite, Request,
};
use std::{ffi::OsStr, sync::Arc, time::Duration};

const GENERATION: u64 = 0;
const NAME_LEN: u32 = 256;
const BLOCK_SIZE: u32 = 512;
const FRAGMENT_SIZE: u32 = 512;

const TTL: Duration = Duration::from_secs(1);

pub struct Filesystem {
    inner: Arc<FilesystemInner>,
}

struct FilesystemInner {
    vfs: Arc<vfs::Vfs>,
}

impl Filesystem {
    pub fn new(vfs: Arc<vfs::Vfs>) -> Self {
        Self {
            inner: Arc::new(FilesystemInner { vfs }),
        }
    }

    fn spawn<F, Fut>(&self, f: F)
    where
        F: FnOnce(Arc<FilesystemInner>) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::task::spawn(f(inner));
    }
}

impl fuser::Filesystem for Filesystem {
    fn init(
        &mut self,
        _req: &Request,
        _config: &mut KernelConfig,
    ) -> std::result::Result<(), libc::c_int> {
        log::info!("FUSE initialized");
        let _ = sd_notify::notify(false, &[sd_notify::NotifyState::Ready]);
        Ok(())
    }

    fn destroy(&mut self) {
        log::info!("FUSE destroyed");
    }

    // TODO:
    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, BLOCK_SIZE, NAME_LEN, FRAGMENT_SIZE);
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_owned();
        self.spawn(|inner| async move {
            match inner.vfs.lookup(parent, &name).await {
                Err(err) => reply.error(err.into_c_err()),
                Ok(attr) => {
                    reply.entry(&TTL, &attr.get_file_attr(), GENERATION);
                }
            }
        });
    }

    fn forget(&mut self, _req: &Request, ino: u64, nlookup: u64) {
        self.spawn(|inner| async move {
            inner.vfs.forget(ino, nlookup).await.unwrap();
        });
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        self.spawn(|inner| async move {
            match inner.vfs.get_attr(ino).await {
                Err(err) => reply.error(err.into_c_err()),
                Ok(attr) => reply.attr(&TTL, &attr.get_file_attr()),
            }
        });
    }

    fn access(&mut self, _req: &Request, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        self.spawn(|inner| async move {
            match inner.vfs.open_dir(ino).await {
                Err(err) => reply.error(err.into_c_err()),
                Ok(fh) => reply.opened(fh, 0),
            }
        });
    }

    fn releasedir(&mut self, _req: &Request, ino: u64, fh: u64, _flags: i32, reply: ReplyEmpty) {
        self.spawn(|inner| async move {
            inner.vfs.close_dir(ino, fh).await.unwrap();
            reply.ok();
        });
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        self.spawn(|inner| async move {
            match inner.vfs.read_dir(ino, fh, offset).await {
                Err(err) => reply.error(err.into_c_err()),
                Ok(entries) => {
                    for (idx, entry) in entries.as_ref().iter().enumerate().skip(offset as usize) {
                        if reply.add(
                            entry.child_ino as u64,
                            (idx + 1) as i64,
                            entry.file_type,
                            &entry.name,
                        ) {
                            break;
                        }
                    }
                    reply.ok();
                }
            }
        });
    }

    // TODO:
    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        // Read is always allowed.
        static_assertions::const_assert_eq!(libc::O_RDONLY, 0);
        log::trace!("open flags: {:#x}", flags);

        let write = (flags & libc::O_WRONLY) != 0;
        assert_eq!(flags & libc::O_TRUNC, 0);
        let ret_flags = flags & libc::O_WRONLY;

        self.spawn(|inner| async move {
            match inner.vfs.open_file(ino, write).await {
                Ok(fh) => reply.opened(fh, ret_flags as u32),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    // TODO:
    fn create(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        log::trace!("open flags: {:#x}", flags);

        let _write = (flags & libc::O_WRONLY) != 0;
        let exclusive = (flags & libc::O_EXCL) != 0;
        let truncate = (flags & libc::O_TRUNC) != 0;
        let ret_flags = flags & (libc::O_WRONLY | libc::O_EXCL | libc::O_TRUNC);
        let uid = req.uid();
        let gid = req.gid();

        let name = name.to_owned();
        self.spawn(|inner| async move {
            match inner
                .vfs
                .open_create_file(parent, &name, uid, gid, truncate, exclusive)
                .await
            {
                Ok(attr) => {
                    reply.created(&TTL, &attr.get_file_attr(), GENERATION, 0, ret_flags as u32)
                }
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.spawn(|inner| async move {
            match inner.vfs.close_file(ino, fh).await {
                Ok(()) => reply.ok(),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let offset = u64::try_from(offset).unwrap();
        let size = usize::try_from(size).unwrap();
        self.spawn(|inner| async move {
            match inner.vfs.read_file(ino, fh, offset, size).await {
                Ok(data) => {
                    let data = data.as_ref();
                    reply.data(data);
                }
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn mkdir(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name = name.to_owned();
        let uid = req.uid();
        let gid = req.gid();
        self.spawn(|inner| async move {
            match inner.vfs.create_dir(parent, &name, uid, gid).await {
                Ok(attr) => reply.entry(&TTL, &attr.get_file_attr(), GENERATION),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    // TODO
    /*
    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
    }
    */

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_owned();
        self.spawn(|inner| async move {
            match inner.vfs.remove_dir(parent, &name).await {
                Ok(()) => reply.ok(),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_owned();
        self.spawn(|inner| async move {
            match inner.vfs.remove_file(parent, &name).await {
                Ok(()) => reply.ok(),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let data = data.to_owned();
        self.spawn(|inner| async move {
            match inner.vfs.write_file(ino, fh, offset as u64, &data).await {
                // > Write should return exactly the number of bytes requested except on error.
                Ok(()) => reply.written(data.len() as u32),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn fsyncdir(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(&mut self, _req: &Request, ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        self.spawn(|inner| async move {
            match inner.vfs.sync_file(ino).await {
                Ok(()) => reply.ok(),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }
}
