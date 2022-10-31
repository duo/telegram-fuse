use grammers_client::types::Chat;
use grammers_client::{Client, InputMessage};
use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fuser::FileType;
use sqlx::{Pool, Row, Sqlite, SqlitePool};

mod error;
mod file;
mod inode;

use error::{Error, Result};

use self::file::FileCache;
use self::inode::{DirEntry, InodeAttr};

const BLOCK_SIZE: u32 = 512;

const DB_FILE: &str = "sqlite://fuse.db?mode=rwc";

pub struct Vfs {
    db: Pool<Sqlite>,
    cache: file::DiskCache,
    client: Client,
    chat: Chat,
}

impl Vfs {
    pub async fn new(client: Client) -> anyhow::Result<Arc<Self>> {
        let me = client.get_me().await?;

        let this = Arc::new(Self {
            db: SqlitePool::connect(DB_FILE).await?,
            cache: file::DiskCache::new(),
            client,
            chat: Chat::User(me),
        });

        this.init_db().await?;

        Ok(this)
    }

    pub async fn lookup(&self, parent_ino: u64, child_name: &OsStr) -> Result<InodeAttr> {
        let attr = self.lookup_inode(parent_ino, child_name).await?;

        if let Some(v) = attr {
            log::trace!(target: "vfs::inode", "lookup: ino={} attr={:?}", v.ino, v);
            Ok(v)
        } else {
            Err(error::Error::NotFound)
        }
    }

    pub async fn forget(&self, ino: u64, count: u64) -> Result<()> {
        // TODO:
        log::trace!(target: "vfs::inode", "forget: ino={} count={}", ino, count);
        Ok(())
    }

    pub async fn get_attr(&self, ino: u64) -> Result<InodeAttr> {
        let attr = self.get_inode(ino).await?;
        log::trace!(target: "vfs::inode", "get_attr: ino={} attr={:?}", ino, attr);

        if let Some(v) = attr {
            Ok(v)
        } else {
            Err(error::Error::NotFound)
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
        let ret = self.get_entries(ino as u32).await?;
        log::trace!(target: "vfs::dir", "read_dir: ino={} offset={}", ino, offset);
        Ok(ret)
    }

    pub async fn open_file(&self, ino: u64, _write: bool) -> Result<u64> {
        if let Some(_) = self.get_inode(ino).await? {
            // TODO: download file from remote, insert into diskcache
            let fh = 0;
            log::trace!(target: "vfs::file", "open_file: ino={} fh={}", ino, fh);
            Ok(fh)
        } else {
            Err(error::Error::NotFound)
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
        let lookup_result = self.lookup_inode(parent_ino, child_name).await?;
        let name = child_name.to_str().unwrap();
        let attr: InodeAttr;
        match lookup_result {
            None => {
                let buf = vec![0];
                let mut stream = std::io::Cursor::new(buf);
                let upload_file = self
                    .client
                    .upload_stream(&mut stream, 1, String::from(name))
                    .await?;
                let msg = self
                    .client
                    .send_message(&self.chat, InputMessage::text("").file(upload_file))
                    .await?;
                let remote_id = msg.id();

                attr = self
                    .add_inode(
                        parent_ino as u32,
                        name,
                        FileType::RegularFile,
                        uid,
                        gid,
                        remote_id,
                    )
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

        self.cache.insert_empty(attr.remote_id).await?;

        log::trace!(
            target: "vfs::file",
            "open_create_file: ino={} name={}",
            attr.ino,
            child_name.to_str().unwrap(),
        );

        Ok(attr)
    }

    pub async fn close_file(&self, ino: u64, fh: u64) -> Result<()> {
        if let Some(attr) = self.get_inode(ino).await? {
            if let Some(file) = self.cache.get(&attr.remote_id) {
                let ret = FileCache::read_all(&file).await?;
                if ret.len() > 0 {
                    let mut stream = std::io::Cursor::new(ret);
                    let upload_file = self
                        .client
                        .upload_stream(&mut stream, 1, attr.name.clone())
                        .await?;
                    self.client
                        .edit_message(
                            &self.chat,
                            attr.remote_id,
                            InputMessage::text(attr.name.clone()).file(upload_file),
                        )
                        .await?;
                }
                log::trace!(target: "vfs::file", "close_file: ino={} fh={}", ino, fh);
                Ok(())
            } else {
                Err(error::Error::NotFound)
            }
        } else {
            Err(error::Error::NotFound)
        }
    }

    pub async fn read_file(
        &self,
        ino: u64,
        fh: u64,
        offset: u64,
        size: usize,
    ) -> Result<impl AsRef<[u8]>> {
        if let Some(attr) = self.get_inode(ino).await? {
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
                Err(error::Error::NotFound)
            }
        } else {
            Err(error::Error::NotFound)
        }
    }

    pub async fn create_dir(
        &self,
        parent_ino: u64,
        name: &OsStr,
        uid: u32,
        gid: u32,
    ) -> Result<InodeAttr> {
        let lookup_result = self.lookup_inode(parent_ino, name).await?;

        let name = name.to_str().unwrap();
        match lookup_result {
            None => {
                let attr = self
                    .add_inode(parent_ino as u32, name, FileType::Directory, uid, gid, 0)
                    .await?;
                log::trace!(
                    target: "vfs::dir",
                    "create_dir: parent_ino={} name={} ino={}",
                    parent_ino, name, attr.ino,
                );
                Ok(attr)
            }
            Some(_) => Err(error::Error::FileExists),
        }
    }

    pub async fn remove_dir(&self, parent_ino: u64, name: &OsStr) -> Result<()> {
        let lookup_result = self.lookup_inode(parent_ino, name).await?;
        let name = name.to_str().unwrap();

        match lookup_result {
            None => Err(error::Error::NotFound),
            Some(attr) => {
                if !self.is_directory_empty(attr.ino).await? {
                    return Err(error::Error::DirectoryNotEmpty);
                }

                self.delete_inode(attr.ino, parent_ino as u32, name).await?;

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
        let lookup_result = self.lookup_inode(parent_ino, name).await?;
        let name = name.to_str().unwrap();

        match lookup_result {
            None => Err(error::Error::NotFound),
            Some(attr) => {
                self.delete_inode(attr.ino, parent_ino as u32, name).await?;

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
        if let Some(attr) = self.get_inode(ino).await? {
            if let Some(file) = self.cache.get(&attr.remote_id) {
                let (new_size, mtime) = FileCache::write(&file, offset, data).await?;

                // TODO: upload to telegram?

                self.update_inode_attr(ino, new_size, mtime).await?;

                log::trace!(
                    target: "vfs::file",
                    "write_file: ino={} fh={} offset={} len={} new_size={} mtime={}",
                    ino, fh, offset, data.len(), new_size, mtime
                );

                Ok(())
            } else {
                Err(error::Error::NotFound)
            }
        } else {
            Err(error::Error::NotFound)
        }
    }

    pub async fn sync_file(&self, ino: u64) -> Result<()> {
        // TODO: upload to telegram?
        log::trace!(target: "vfs::file", "sync_file: ino={}", ino);
        Ok(())
    }

    async fn init_db(&self) -> anyhow::Result<()> {
        let mut conn = self.db.acquire().await?;

        log::info!("Initialize meta tables");
        {
            let sql = "
                CREATE TABLE IF NOT EXISTS node (
                    ino INTEGER PRIMARY KEY,
                    size INTEGER DEFAULT 0 NOT NULL,
                    blocks INTEGER DEFAULT 0,
                    atime INTEGER,
                    mtime INTEGER,
                    ctime INTEGER,
                    crtime INTEGER,
                    kind INTEGER,
                    perm INTEGER,
                    nlink INTEGER DEFAULT 0,
                    uid INTEGER DEFAULT 0,
                    gid INTEGER DEFAULT 0,
                    rdev INTEGER DEFAULT 0,
                    blksize INTEGER,
                    flags INTEGER DEFAULT 0,
                    remote_id INTEGER DEFAULT 0
                )
            ";
            sqlx::query(sql).execute(&mut conn).await?;
        }
        {
            let sql = "
                CREATE TABLE IF NOT EXISTS node_tree (
                    parent_ino INTEGER,
                    child_ino INTEGER,
                    file_type INTEGER,
                    name TEXT,
                    PRIMARY KEY (parent_ino, name)
                )
            ";
            sqlx::query(sql).execute(&mut conn).await?;
        }

        log::info!("Initialize meta data");
        {
            let sql = "
                INSERT OR IGNORE INTO node (
                    ino, atime, mtime, ctime, crtime, kind, perm, nlink, blksize
                )
                VALUES (1, $1, $1, $1, $1, $2, $3, 2, $4)
            ";
            let time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32;
            sqlx::query(sql)
                .bind(time)
                .bind(libc::S_IFDIR)
                .bind(0o777)
                .bind(BLOCK_SIZE)
                .execute(&mut conn)
                .await?;
        }

        Ok(())
    }

    async fn lookup_inode(&self, parent_ino: u64, child_name: &OsStr) -> Result<Option<InodeAttr>> {
        let mut conn = self.db.acquire().await?;

        let sql = "
            SELECT
                n.ino, n.size, n.blocks, n.atime, n.mtime, n.ctime, n.crtime, n.kind, n.perm,
                n.nlink, n.uid, n.gid, n.rdev, n.blksize, n.flags, n.remote_id, nt.name
            FROM node_tree AS nt
                INNER JOIN node AS n ON nt.child_ino = n.ino
            WHERE nt.parent_ino=$1 AND nt.name=$2
        ";

        let rec = sqlx::query_as(sql)
            .bind(parent_ino as u32)
            .bind(child_name.to_str().unwrap())
            .fetch_optional(&mut conn)
            .await?;

        Ok(rec)
    }

    async fn get_inode(&self, ino: u64) -> Result<Option<InodeAttr>> {
        let mut conn = self.db.acquire().await?;

        let sql = "
            SELECT
                n.ino, n.size, n.blocks, n.atime, n.mtime, n.ctime, n.crtime, n.kind, n.perm,
                n.nlink, n.uid, n.gid, n.rdev, n.blksize, n.flags, n.remote_id, nt.name
            FROM node AS n
                LEFT JOIN node_tree AS nt ON nt.child_ino = n.ino
            WHERE n.ino=$1
        ";

        let rec = sqlx::query_as(sql)
            .bind(ino as u32)
            .fetch_optional(&mut conn)
            .await?;

        Ok(rec)
    }

    async fn get_entries(&self, ino: u32) -> Result<Vec<DirEntry>> {
        let mut conn = self.db.acquire().await?;

        let sql = "
            SELECT child_ino, file_type, name
            FROM node_tree
            WHERE parent_ino=$1
            ORDER BY name
        ";

        let recs = sqlx::query(sql)
            .bind(ino)
            .map(|row| DirEntry {
                parent_ino: ino,
                child_ino: row.get(0),
                file_type: inode::convert_file_type(row.get(1)),
                name: row.get(2),
            })
            .fetch_all(&mut conn)
            .await?;

        Ok(recs)
    }

    async fn add_inode(
        &self,
        parent_ino: u32,
        name: &str,
        kind: FileType,
        uid: u32,
        gid: u32,
        remote_id: i32,
    ) -> Result<InodeAttr> {
        let mut tx = self.db.begin().await?;

        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;

        let mut attr = InodeAttr {
            ino: 0,
            size: 0,
            blocks: 0,
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
            kind: match kind {
                FileType::Directory => libc::S_IFDIR,
                _ => libc::S_IFREG,
            },
            perm: match kind {
                FileType::Directory => 0o777,
                _ => 0o666,
            },
            nlink: match kind {
                FileType::Directory => 2,
                _ => 1,
            },
            uid,
            gid,
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
            remote_id,
            name: String::from(name),
        };

        let node_sql = "
            INSERT INTO node (
                atime, mtime, ctime, crtime, kind, perm, nlink, uid, gid, blksize, remote_id
            )
            VALUES ($1, $1, $1, $1, $2, $3, $4, $5, $6, $7, $8)
        ";

        let ino = sqlx::query(node_sql)
            .bind(time)
            .bind(attr.kind)
            .bind(attr.perm)
            .bind(attr.nlink)
            .bind(uid)
            .bind(gid)
            .bind(attr.blksize)
            .bind(attr.remote_id)
            .execute(&mut tx)
            .await?
            .last_insert_rowid();
        attr.ino = ino as u32;

        let node_tree_sql = "
            INSERT INTO node_tree
            VALUES ($1, $2, $3, $4)
        ";

        sqlx::query(node_tree_sql)
            .bind(parent_ino)
            .bind(ino)
            .bind(attr.kind)
            .bind(name)
            .execute(&mut tx)
            .await?;

        tx.commit().await?;

        Ok(attr)
    }

    async fn is_directory_empty(&self, ino: u32) -> Result<bool> {
        let mut conn = self.db.acquire().await?;

        let sql = "
            SELECT *
            FROM node_tree
            WHERE parent_ino=$1
        ";

        let rec = sqlx::query(sql)
            .bind(ino as u32)
            .fetch_optional(&mut conn)
            .await?;

        if let Some(_) = rec {
            Ok(false)
        } else {
            Ok(true)
        }
    }

    async fn delete_inode(&self, ino: u32, parent_ino: u32, name: &str) -> Result<()> {
        let mut tx = self.db.begin().await?;

        let node_tree_sql = "
            DELETE
            FROM node_tree
            WHERE parent_ino=$1 AND name=$2
        ";
        sqlx::query(node_tree_sql)
            .bind(parent_ino)
            .bind(name)
            .execute(&mut tx)
            .await?;

        let node_sql = "
            DELETE
            FROM node
            WHERE ino=$1
        ";
        sqlx::query(node_sql).bind(ino).execute(&mut tx).await?;

        let update_node_sql = "
            UPDATE node
            SET mtime=$2
            WHERE ino=$1
        ";
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        sqlx::query(update_node_sql)
            .bind(ino)
            .bind(time)
            .execute(&mut tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn update_inode_attr(&self, ino: u64, size: u64, mtime: u32) -> Result<()> {
        let mut conn = self.db.acquire().await?;

        let sql = "
            UPDATE node
            SET size=$2, blocks=$3, mtime=$4
            WHERE ino=$1
        ";

        sqlx::query(sql)
            .bind(ino as u32)
            .bind(size as u32)
            .bind((size as u32 + BLOCK_SIZE - 1) / BLOCK_SIZE)
            .bind(mtime)
            .fetch_optional(&mut conn)
            .await?;

        Ok(())
    }
}
