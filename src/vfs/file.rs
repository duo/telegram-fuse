use crate::vfs::{Error, Result};

use bytes::Bytes;
use grammers_client::types::media::Uploaded;
use grammers_client::types::Chat;
use grammers_client::{Client, InputMessage};
use lru::LruCache;
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as SyncMutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

const CACHE_SIZE: usize = 1024;

struct FileCacheState {
    file_size: u64,
    file: tokio::fs::File,
    status: FileCacheStatus,
}

#[derive(Debug)]
enum FileCacheStatus {
    Ready,
    Dirty,
}

pub struct FileCache {
    remote_id: i32,
    state: Mutex<FileCacheState>,
}

impl FileCache {
    fn new(
        remote_id: i32,
        file: tokio::fs::File,
        file_size: u64,
        status: FileCacheStatus,
    ) -> Arc<Self> {
        Arc::new(Self {
            remote_id,
            state: Mutex::new(FileCacheState {
                file_size,
                file,
                status,
            }),
        })
    }

    pub async fn read(this: &Arc<Self>, offset: u64, size: usize) -> Result<Bytes> {
        let mut guard = this.state.lock().await;
        let file_size = guard.file_size;
        if file_size <= offset || size == 0 {
            return Ok(Bytes::new());
        }
        let end = guard.file_size.min(offset + size as u64);

        let mut buf = vec![0u8; (end - offset) as usize];
        guard.file.seek(SeekFrom::Start(offset)).await?;
        guard.file.read_exact(&mut buf).await?;
        Ok(buf.into())
    }

    pub async fn write(this: &Arc<Self>, offset: u64, data: &[u8]) -> Result<(u64, u32)> {
        let mut guard = this.state.lock().await;

        let mtime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;

        guard.file.seek(SeekFrom::Start(offset)).await?;
        guard.file.write_all(data).await?;

        guard.file_size = guard.file_size.max(offset + data.len() as u64);
        guard.status = FileCacheStatus::Dirty;

        Ok((guard.file_size, mtime))
    }

    pub async fn sync(this: &Arc<Self>, name: &str, client: Client, chat: &Chat) -> Result<()> {
        let mut guard = this.state.lock().await;

        match guard.status {
            FileCacheStatus::Ready => {}
            FileCacheStatus::Dirty => {
                let size = guard.file_size as usize;

                let uploaded_file: Uploaded;
                if size == 0 {
                    let buf = vec![0];
                    let mut stream = std::io::Cursor::new(buf);

                    uploaded_file = client
                        .upload_stream(&mut stream, 1, String::from(name))
                        .await?;
                } else {
                    let mut buf = vec![0u8; size];
                    guard.file.seek(SeekFrom::Start(0)).await?;
                    guard.file.read_exact(&mut buf).await?;
                    let mut stream = std::io::Cursor::new(buf);
                    uploaded_file = client
                        .upload_stream(&mut stream, size, String::from(name))
                        .await?;
                }

                client
                    .edit_message(
                        chat,
                        this.remote_id,
                        InputMessage::text(name).file(uploaded_file),
                    )
                    .await?;
            }
        }

        Ok(())
    }
}

pub struct DiskCache {
    dir: PathBuf,
    files: SyncMutex<LruCache<i32, Arc<FileCache>>>,
    client: Client,
    chat: Chat,
}

impl DiskCache {
    pub fn new(client: Client, chat: Chat) -> Self {
        Self {
            dir: PathBuf::new(),
            files: SyncMutex::new(LruCache::new(NonZeroUsize::new(CACHE_SIZE).unwrap())),
            client,
            chat,
        }
    }

    pub fn get(&self, remote_id: &i32) -> Option<Arc<FileCache>> {
        self.files.lock().unwrap().get_mut(remote_id).cloned()
    }

    pub fn remove(&self, remote_id: &i32) {
        self.files.lock().unwrap().pop(remote_id);
    }

    pub async fn open(&self, remote_id: i32) -> Result<u64> {
        if let Some(_) = self.get(&remote_id) {
            return Ok(0);
        }

        let msgs = self
            .client
            .get_messages_by_id(&self.chat, &vec![remote_id])
            .await?;

        if let Some(msg) = msgs.into_iter().nth(0) {
            if let Some(raw_msg) = msg {
                if raw_msg.text().is_empty() {
                    self.insert_empty(raw_msg.id()).await?;
                } else if let Some(media) = raw_msg.media() {
                    let tmp_file = tempfile::tempfile_in(&self.dir)?;
                    let mut file: tokio::fs::File = tmp_file.into();

                    let mut size = 0;
                    let mut download = self.client.iter_download(&media);
                    while let Some(chunk) = download.next().await? {
                        size += chunk.len();
                        file.write_all(&chunk).await?;
                    }

                    let mut files = self.files.lock().unwrap();
                    let file = FileCache::new(remote_id, file, size as u64, FileCacheStatus::Ready);
                    files.put(remote_id, file.clone());
                } else {
                    // TODO: another error?
                    return Err(Error::NotFound);
                }
                Ok(0)
            } else {
                Err(Error::NotFound)
            }
        } else {
            Err(Error::NotFound)
        }
    }

    pub async fn open_create_empty(&self, name: &str) -> Result<(u64, i32)> {
        let remote_id = self.upload_empty_file(name, None).await?;

        Ok((0, remote_id))
    }

    pub async fn delete(&self, remote_id: i32) -> Result<()> {
        self.remove(&remote_id);

        if let Err(_) = self
            .client
            .delete_messages(&self.chat, &vec![remote_id])
            .await
        {}

        Ok(())
    }

    pub async fn truncate_file(&self, remote_id: i32, new_size: u64, name: &str) -> Result<()> {
        if let None = self.get(&remote_id) {
            if new_size == 0 {
                self.upload_empty_file(name, Some(remote_id)).await?;
                return Ok(());
            } else {
                self.open(remote_id).await?;
            }
        }

        if let Some(file) = self.get(&remote_id) {
            {
                let mut guard = file.state.lock().await;
                guard.file_size = new_size;
                guard.file.set_len(new_size).await.unwrap();
                guard.status = FileCacheStatus::Dirty;
            }
            FileCache::sync(&file, name, self.client.clone(), &self.chat).await?;
        }

        Ok(())
    }

    pub async fn sync(&self, remote_id: &i32, name: &str) -> Result<()> {
        if let Some(file) = self.get(remote_id) {
            FileCache::sync(&file, name, self.client.clone(), &self.chat).await?;

            Ok(())
        } else {
            Err(Error::NotFound)
        }
    }

    async fn insert_empty(&self, remote_id: i32) -> Result<Arc<FileCache>> {
        let tmp_file = tempfile::tempfile_in(&self.dir)?;
        let mut files = self.files.lock().unwrap();
        let file = FileCache::new(remote_id, tmp_file.into(), 0, FileCacheStatus::Ready);
        files.put(remote_id, file.clone());
        Ok(file)
    }

    async fn upload_empty_file(&self, name: &str, remote_id: Option<i32>) -> Result<i32> {
        let buf = vec![0];
        let mut stream = std::io::Cursor::new(buf);

        let uploaded_file = self
            .client
            .upload_stream(&mut stream, 1, String::from(name))
            .await?;

        if let Some(id) = remote_id {
            self.client
                .edit_message(&self.chat, id, InputMessage::text("").file(uploaded_file))
                .await?;

            self.insert_empty(id).await?;

            Ok(id)
        } else {
            let msg = self
                .client
                .send_message(&self.chat, InputMessage::text("").file(uploaded_file))
                .await?;

            self.insert_empty(msg.id()).await?;

            Ok(msg.id())
        }
    }
}
