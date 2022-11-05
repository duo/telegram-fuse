use fuser::FileType;
use grammers_client::types::Chat;
use grammers_client::Client;
use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

mod error;
mod file;
mod inode;

use error::{Error, Result};
use file::FileCache;
use inode::{DirEntry, InodeAttr, InodeTree};

pub struct Vfs {
    inode_tree: InodeTree,
    cache: file::DiskCache,
}

impl Vfs {
    pub async fn new(client: Client) -> anyhow::Result<Arc<Self>> {
        let me = client.get_me().await?;

        let this = Arc::new(Self {
            inode_tree: InodeTree::new().await?,
            cache: file::DiskCache::new(client, Chat::User(me)),
        });

        Ok(this)
    }

    pub async fn lookup(&self, parent_ino: u64, child_name: &OsStr) -> Result<InodeAttr> {
        let attr = self.inode_tree.lookup(parent_ino, child_name).await?;

        if let Some(v) = attr {
            log::trace!(target: "vfs::inode", "lookup: ino={} attr={:?}", v.ino, v);
            Ok(v)
        } else {
            Err(Error::NotFound)
        }
    }

    pub async fn forget(&self, ino: u64, count: u64) -> Result<()> {
        // TODO:
        log::trace!(target: "vfs::inode", "forget: ino={} count={}", ino, count);
        Ok(())
    }

    pub async fn get_attr(&self, ino: u64) -> Result<InodeAttr> {
        let attr = self.inode_tree.get(ino).await?;
        log::trace!(target: "vfs::inode", "get_attr: ino={} attr={:?}", ino, attr);

        if let Some(v) = attr {
            Ok(v)
        } else {
            Err(Error::NotFound)
        }
    }

    pub async fn open_dir(&self, ino: u64) -> Result<u64> {
        log::trace!(target: "vfs::dir", "open_dir: ino={}", ino);
        Ok(0)
    }

    pub async fn close_dir(&self, ino: u64, _fh: u64) -> Result<()> {
        log::trace!(target: "vfs::dir", "close_dir: ino={}", ino);
        Ok(())
    }

    pub async fn read_dir(
        &self,
        ino: u64,
        _fh: u64,
        offset: i64,
    ) -> Result<impl AsRef<[DirEntry]>> {
        let ret = self.inode_tree.read_dir(ino).await?;
        log::trace!(target: "vfs::dir", "read_dir: ino={} offset={}", ino, offset);
        Ok(ret)
    }

    pub async fn open_file(&self, ino: u64, _write: bool) -> Result<u64> {
        if let Some(attr) = self.inode_tree.get(ino).await? {
            let fh = self.cache.open(attr.remote_id).await?;
            log::trace!(target: "vfs::file", "open_file: ino={} fh={}", ino, fh);
            Ok(fh)
        } else {
            Err(Error::MediaInvalid)
        }
    }

    pub async fn open_create_file(
        &self,
        parent_ino: u64,
        child_name: &OsStr,
        uid: u32,
        gid: u32,
        truncate: bool,
        exclusive: bool,
    ) -> Result<InodeAttr> {
        let lookup_result = self.inode_tree.lookup(parent_ino, child_name).await?;
        let name = child_name.to_str().unwrap();
        let attr: InodeAttr;
        match lookup_result {
            None => {
                let (_, remote_id) = self.cache.open_create_empty(name).await?;

                attr = self
                    .inode_tree
                    .add(parent_ino, name, FileType::RegularFile, uid, gid, remote_id)
                    .await?;
            }
            Some(v) => {
                if !truncate {
                    if exclusive {
                        return Err(Error::FileExists);
                    }

                    self.open_file(v.ino as u64, true).await?;

                    return Ok(v);
                }
                attr = v;
            }
        }

        log::trace!(
            target: "vfs::file",
            "open_create_file: ino={} name={}",
            attr.ino,
            child_name.to_str().unwrap(),
        );

        Ok(attr)
    }

    pub async fn close_file(&self, ino: u64, fh: u64) -> Result<()> {
        if let Some(attr) = self.inode_tree.get(ino).await? {
            self.cache.flush(attr.remote_id, &attr.name, false).await?;
            log::trace!(target: "vfs::file", "close_file: ino={} fh={}", ino, fh);

            Ok(())
        } else {
            Err(Error::NotFound)
        }
    }

    pub async fn read_file(
        &self,
        ino: u64,
        fh: u64,
        offset: u64,
        size: usize,
    ) -> Result<impl AsRef<[u8]>> {
        if let Some(attr) = self.inode_tree.get(ino).await? {
            if let Some(file) = self.cache.get(&attr.remote_id) {
                let ret = FileCache::read(&file, offset, size).await?;
                log::trace!(
                    target: "vfs::file",
                    "read_file: ino={} fh={} offset={} size={} bytes_read={}",
                    ino,
                    fh,
                    offset,
                    size,
                    ret.as_ref().len(),
                );
                Ok(ret)
            } else {
                Err(Error::NotFound)
            }
        } else {
            Err(Error::NotFound)
        }
    }

    pub async fn create_dir(
        &self,
        parent_ino: u64,
        name: &OsStr,
        uid: u32,
        gid: u32,
    ) -> Result<InodeAttr> {
        let lookup_result = self.inode_tree.lookup(parent_ino, name).await?;

        let name = name.to_str().unwrap();
        match lookup_result {
            None => {
                let attr = self
                    .inode_tree
                    .add(parent_ino, name, FileType::Directory, uid, gid, 0)
                    .await?;
                log::trace!(
                    target: "vfs::dir",
                    "create_dir: parent_ino={} name={} ino={}",
                    parent_ino, name, attr.ino,
                );
                Ok(attr)
            }
            Some(_) => Err(Error::FileExists),
        }
    }

    pub async fn rename(
        &self,
        parent_ino: u64,
        name: &OsStr,
        new_parent_ino: u64,
        new_name: &OsStr,
    ) -> Result<()> {
        if let Some(remote_id) = self
            .inode_tree
            .rename(parent_ino, name, new_parent_ino, new_name)
            .await?
        {
            self.cache.delete(remote_id).await?;
        }

        log::debug!(
            "Moved file from {}/{:?} to {}/{:?}",
            parent_ino,
            name,
            new_parent_ino,
            new_name,
        );

        Ok(())
    }

    pub async fn remove_dir(&self, parent_ino: u64, name: &OsStr) -> Result<()> {
        let lookup_result = self.inode_tree.lookup(parent_ino, name).await?;
        let name = name.to_str().unwrap();

        match lookup_result {
            None => Err(Error::NotFound),
            Some(attr) => {
                if !self.inode_tree.is_directory_empty(attr.ino as u64).await? {
                    return Err(Error::DirectoryNotEmpty);
                }

                self.inode_tree
                    .delete(attr.ino as u64, parent_ino as u32, name)
                    .await?;

                log::trace!(
                    target: "vfs::dir",
                    "remove_dir: ino={} parent_ino={} name={}",
                    attr.ino, parent_ino, name,
                );

                Ok(())
            }
        }
    }

    pub async fn remove_file(&self, parent_ino: u64, name: &OsStr) -> Result<()> {
        let lookup_result = self.inode_tree.lookup(parent_ino, name).await?;
        let name = name.to_str().unwrap();

        match lookup_result {
            None => Err(Error::NotFound),
            Some(attr) => {
                self.cache.delete(attr.remote_id).await?;
                self.inode_tree
                    .delete(attr.ino as u64, parent_ino as u32, name)
                    .await?;

                log::trace!(
                    target: "vfs::dir",
                    "remove_file: ino={} parent_ino={} name={}",
                    attr.ino, parent_ino, name,
                );

                Ok(())
            }
        }
    }

    pub async fn write_file(&self, ino: u64, fh: u64, offset: u64, data: &[u8]) -> Result<()> {
        if let Some(attr) = self.inode_tree.get(ino).await? {
            let (new_size, mtime) = self.cache.write_file(attr.remote_id, offset, data).await?;

            self.inode_tree.update_attr(ino, new_size, mtime).await?;

            log::trace!(
                target: "vfs::file",
                "write_file: ino={} fh={} offset={} len={} new_size={} mtime={}",
                ino, fh, offset, data.len(), new_size, mtime
            );

            Ok(())
        } else {
            Err(Error::NotFound)
        }
    }

    pub async fn set_attr(
        &self,
        ino: u64,
        size: Option<u64>,
        mtime: Option<SystemTime>,
    ) -> Result<InodeAttr> {
        if let Some(mut attr) = self.inode_tree.get(ino).await? {
            match (size, mtime) {
                (Some(new_size), _) if attr.size != new_size as u32 => {
                    let mtime = mtime.unwrap_or_else(SystemTime::now);
                    attr.mtime = mtime.duration_since(UNIX_EPOCH).unwrap().as_secs() as u32;
                    attr.size = new_size as u32;
                    self.cache
                        .truncate_file(attr.remote_id, new_size, &attr.name)
                        .await?;
                }
                (_, Some(mtime)) => {
                    attr.mtime = mtime.duration_since(UNIX_EPOCH).unwrap().as_secs() as u32;
                }
                (_, None) => {}
            }
            self.inode_tree
                .update_attr(ino, attr.size as u64, attr.mtime)
                .await?;

            log::trace!(
                target: "vfs::file",
                "truncate_file: ino={} new_size={:?} new_mtime={:?} ret_attr={:?}",
                ino, size, mtime, attr,
            );

            Ok(attr)
        } else {
            Err(Error::NotFound)
        }
    }

    pub async fn sync_file(&self, ino: u64) -> Result<()> {
        if let Some(attr) = self.inode_tree.get(ino).await? {
            self.cache.flush(attr.remote_id, &attr.name, true).await?;
            log::trace!(target: "vfs::file", "sync_file: ino={}", ino);

            Ok(())
        } else {
            Err(Error::NotFound)
        }
    }
}
