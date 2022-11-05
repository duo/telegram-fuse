use crate::vfs::{Error, Result};

use fuser::{FileAttr, FileType};
use grammers_client::{
    types::{Chat, Media, Message},
    Client, InputMessage,
};
use sqlx::{FromRow, Pool, Row, Sqlite, SqlitePool};
use std::{
    ffi::OsStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const BLOCK_SIZE: u32 = 512;

const DB_CONN: &str = "sqlite://fuse.db?mode=rwc";
const DB_FILE: &str = "fuse.db";
const DB_TITLE: &str = "telegram-fuse db";

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

pub struct InodeTree {
    db: Pool<Sqlite>,
    client: Client,
    chat: Chat,
}

impl InodeTree {
    pub async fn new(client: Client, chat: Chat) -> anyhow::Result<Self> {
        Self::fetch_db(&client, &chat).await?;

        let this = Self {
            db: SqlitePool::connect(DB_CONN).await?,
            client,
            chat,
        };
        this.init().await?;

        Ok(this)
    }

    pub async fn destroy(&self) -> Result<()> {
        let uploaded_file = self.client.upload_file(DB_FILE).await?;

        let message = InodeTree::get_db_message_id(&self.client, &self.chat).await?;
        if let Some(msg) = message {
            self.client
                .edit_message(
                    &self.chat,
                    msg.id(),
                    InputMessage::text(DB_TITLE).file(uploaded_file),
                )
                .await?;
        } else {
            self.client
                .send_message(&self.chat, InputMessage::text(DB_TITLE).file(uploaded_file))
                .await?;
        }
        log::info!("Upload {} to Telegram", DB_FILE);

        Ok(())
    }

    pub async fn lookup(&self, parent_ino: u64, child_name: &OsStr) -> Result<Option<InodeAttr>> {
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

    pub async fn get(&self, ino: u64) -> Result<Option<InodeAttr>> {
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

    pub async fn read_dir(&self, ino: u64) -> Result<Vec<DirEntry>> {
        let mut conn = self.db.acquire().await?;

        let sql = "
            SELECT child_ino, file_type, name
            FROM node_tree
            WHERE parent_ino=$1
            ORDER BY name
        ";

        let recs = sqlx::query(sql)
            .bind(ino as u32)
            .map(|row| DirEntry {
                parent_ino: ino as u32,
                child_ino: row.get(0),
                file_type: convert_file_type(row.get(1)),
                name: row.get(2),
            })
            .fetch_all(&mut conn)
            .await?;

        Ok(recs)
    }

    pub async fn add(
        &self,
        parent_ino: u64,
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
            .bind(parent_ino as u32)
            .bind(ino)
            .bind(attr.kind)
            .bind(name)
            .execute(&mut tx)
            .await?;

        tx.commit().await?;

        Ok(attr)
    }

    pub async fn rename(
        &self,
        parent_ino: u64,
        name: &OsStr,
        new_parent_ino: u64,
        new_name: &OsStr,
    ) -> Result<Option<i32>> {
        if parent_ino == new_parent_ino && name == new_name {
            return Ok(None);
        }

        let mut deleted_id = None;

        let old_entry = match self.get_dir(parent_ino as u32, name).await? {
            Some(e) => e,
            None => {
                return Err(Error::NotFound);
            }
        };
        let new_entry = self.get_dir(new_parent_ino as u32, new_name).await?;

        if let Some(dest_entry) = &new_entry {
            if dest_entry.file_type != old_entry.file_type {
                match dest_entry.file_type {
                    FileType::Directory => {
                        return Err(Error::IsADirectory);
                    }
                    FileType::RegularFile => {
                        return Err(Error::NotADirectory);
                    }
                    _ => {
                        return Err(Error::InvalidFileType(dest_entry.file_type));
                    }
                }
            }
            if dest_entry.file_type == FileType::Directory {
                if !self.is_directory_empty(dest_entry.child_ino as u64).await? {
                    return Err(Error::DirectoryNotEmpty);
                }
            }

            let remote_id = match self.get(dest_entry.child_ino as u64).await? {
                Some(attr) => attr.remote_id,
                None => 0,
            };

            self.delete(
                dest_entry.child_ino as u64,
                dest_entry.parent_ino,
                &dest_entry.name,
            )
            .await?;

            if remote_id != 0 {
                deleted_id = Some(remote_id);
            }
        }

        {
            let mut conn = self.db.acquire().await?;

            let sql = "
                UPDATE node_tree
                SET parent_ino=$3, name=$4
                WHERE parent_ino=$1 AND name=$2
            ";

            sqlx::query(sql)
                .bind(old_entry.parent_ino)
                .bind(old_entry.name.clone())
                .bind(new_parent_ino as u32)
                .bind(new_name.to_str().unwrap())
                .execute(&mut conn)
                .await?;

            let time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32;

            let sql = "
                UPDATE node
                SET ctime=$2
                WHERE ino=$1
            ";
            sqlx::query(sql)
                .bind(old_entry.child_ino)
                .bind(time)
                .execute(&mut conn)
                .await?;

            let sql = "
                UPDATE node
                SET ctime=$2, mtime=$2
                WHERE ino=$1
            ";
            sqlx::query(sql)
                .bind(old_entry.parent_ino)
                .bind(time)
                .execute(&mut conn)
                .await?;

            if old_entry.parent_ino != new_parent_ino as u32 {
                sqlx::query(sql)
                    .bind(new_parent_ino as u32)
                    .bind(time)
                    .execute(&mut conn)
                    .await?;
            }
        }

        Ok(deleted_id)
    }

    pub async fn is_directory_empty(&self, ino: u64) -> Result<bool> {
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

    pub async fn delete(&self, ino: u64, parent_ino: u32, name: &str) -> Result<()> {
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
        sqlx::query(node_sql)
            .bind(ino as u32)
            .execute(&mut tx)
            .await?;

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
            .bind(ino as u32)
            .bind(time)
            .execute(&mut tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }

    pub async fn update_attr(&self, ino: u64, size: u64, mtime: u32) -> Result<()> {
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
            .execute(&mut conn)
            .await?;

        Ok(())
    }

    async fn get_dir(&self, parent_ino: u32, child_name: &OsStr) -> Result<Option<DirEntry>> {
        let mut conn = self.db.acquire().await?;

        let sql = "
            SELECT child_ino, file_type, name
            FROM node_tree
            WHERE parent_ino=$1 AND name=$2
        ";

        let rec = sqlx::query(sql)
            .bind(parent_ino as u32)
            .bind(child_name.to_str().unwrap())
            .map(|row| DirEntry {
                parent_ino: parent_ino,
                child_ino: row.get(0),
                file_type: convert_file_type(row.get(1)),
                name: row.get(2),
            })
            .fetch_optional(&mut conn)
            .await?;

        Ok(rec)
    }

    async fn init(&self) -> anyhow::Result<()> {
        let mut conn = self.db.acquire().await?;

        log::info!("Initialize meta tables");
        {
            let sql = "
                CREATE TABLE IF NOT EXISTS node (
                    ino INTEGER PRIMARY KEY AUTOINCREMENT,
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

    async fn fetch_db(client: &Client, chat: &Chat) -> Result<()> {
        let message = InodeTree::get_db_message_id(client, chat).await?;
        if let Some(msg) = message {
            client
                .download_media(&msg.media().unwrap(), DB_FILE)
                .await?;
            log::info!("Download {} from Telegram", DB_FILE);
        }

        Ok(())
    }

    async fn get_db_message_id(client: &Client, chat: &Chat) -> Result<Option<Message>> {
        let mut messages = client.search_messages(chat).query(DB_TITLE);
        while let Some(message) = messages.next().await? {
            if message.text() == DB_TITLE {
                if let Some(Media::Document(document)) = message.media() {
                    if document.name() == DB_FILE {
                        return Ok(Some(message));
                    }
                }
            }
        }

        Ok(None)
    }
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
