use std::time::{Duration, UNIX_EPOCH};

use fuser::{FileAttr, FileType};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow)]
pub struct InodeAttr {
    pub ino: u32,
    pub size: u32,
    pub blocks: u32,
    pub atime: u32,
    pub mtime: u32,
    pub ctime: u32,
    pub crtime: u32,
    pub kind: u16,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
    pub remote_id: i32,
    pub name: String,
}

impl InodeAttr {
    pub fn get_file_attr(&self) -> FileAttr {
        FileAttr {
            ino: self.ino as u64,
            size: self.size as u64,
            blocks: self.blocks as u64,
            atime: UNIX_EPOCH + Duration::from_secs(self.atime as u64),
            mtime: UNIX_EPOCH + Duration::from_secs(self.mtime as u64),
            ctime: UNIX_EPOCH + Duration::from_secs(self.ctime as u64),
            crtime: UNIX_EPOCH + Duration::from_secs(self.crtime as u64),
            kind: convert_file_type(self.kind),
            perm: self.perm,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: self.rdev,
            blksize: self.blksize,
            flags: self.flags,
        }
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct DirEntry {
    pub parent_ino: u32,
    pub child_ino: u32,
    pub name: String,
    pub file_type: FileType,
}

pub fn convert_file_type(kind: u16) -> FileType {
    match kind {
        libc::S_IFREG => FileType::RegularFile,
        libc::S_IFSOCK => FileType::Socket,
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFIFO => FileType::NamedPipe,
        _ => FileType::RegularFile,
    }
}
