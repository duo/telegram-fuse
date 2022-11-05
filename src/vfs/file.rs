use crate::vfs::{Error, Result};

use bytes::Bytes;
use grammers_client::types::media::Uploaded;
use grammers_client::types::{Chat, Media};
use grammers_client::{Client, InputMessage};
use lru::LruCache;
use std::io::{self, SeekFrom};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as SyncMutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{watch, MutexGuard};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

const CACHE_SIZE: usize = 1024;

struct FileCacheState {
    file_size: u64,
    available_size: watch::Receiver<u64>,
    file: tokio::fs::File,
    status: FileCacheStatus,
}

#[derive(Debug)]
enum FileCacheStatus {
    Downloading {
        truncate: Option<u64>,
    },
    DownloadFailed,
    Ready,
    Dirty {
        lock_mtime: Instant,
        done_rx: watch::Receiver<bool>,
    },
    Invalidated,
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
    ) -> (Arc<Self>, watch::Sender<u64>) {
        let (tx, rx) = watch::channel(0);
        let this = Arc::new(Self {
            remote_id,
            state: Mutex::new(FileCacheState {
                file_size,
                available_size: rx,
                file,
                status,
            }),
        });
        (this, tx)
    }

    pub async fn read(this: &Arc<Self>, offset: u64, size: usize) -> Result<Bytes> {
        let mut guard = this.state.lock().await;
        let file_size = guard.file_size;
        if file_size <= offset || size == 0 {
            return Ok(Bytes::new());
        }
        let end = offset + size as u64;

        match guard.status {
            FileCacheStatus::Ready | FileCacheStatus::Dirty { .. } => {}
            FileCacheStatus::Invalidated => return Err(Error::Invalidated),
            FileCacheStatus::DownloadFailed => return Err(Error::DownloadFailed),
            FileCacheStatus::Downloading { .. } if end <= *guard.available_size.borrow() => {}
            FileCacheStatus::Downloading { .. } => {
                let mut rx = guard.available_size.clone();
                drop(guard);
                // Wait until finished or enough bytes are available.
                while rx.changed().await.is_ok() && *rx.borrow() < end {}

                guard = this.state.lock().await;
                match guard.status {
                    FileCacheStatus::Invalidated => return Err(Error::Invalidated),
                    FileCacheStatus::DownloadFailed => return Err(Error::DownloadFailed),
                    FileCacheStatus::Ready
                    | FileCacheStatus::Dirty { .. }
                    | FileCacheStatus::Downloading { .. } => {}
                }
            }
        }

        // File size should be retrieved after waiting since it may change.
        let end = end.min(guard.file_size);

        let mut buf = vec![0u8; (end - offset) as usize];
        guard.file.seek(SeekFrom::Start(offset)).await.unwrap();
        guard.file.read_exact(&mut buf).await.unwrap();
        Ok(buf.into())
    }

    async fn write(self: &Arc<Self>, offset: u64, data: &[u8]) -> Result<(u64, u32)> {
        let mut guard = self.state.lock().await;

        match guard.status {
            FileCacheStatus::Ready | FileCacheStatus::Dirty { .. } => {}
            FileCacheStatus::Invalidated => return Err(Error::Invalidated),
            FileCacheStatus::DownloadFailed => return Err(Error::DownloadFailed),
            FileCacheStatus::Downloading { .. } => {
                let mut rx = guard.available_size.clone();
                drop(guard);
                // Wait until finished.
                while rx.changed().await.is_ok() {}
                guard = self.state.lock().await;
            }
        }

        match guard.status {
            FileCacheStatus::Invalidated => return Err(Error::Invalidated),
            FileCacheStatus::DownloadFailed => return Err(Error::DownloadFailed),
            FileCacheStatus::Downloading { .. } => unreachable!(),
            FileCacheStatus::Dirty { .. } => {}
            FileCacheStatus::Ready => {
                let (done_tx, done_rx) = watch::channel(false);
                let init_lock_mtime = Instant::now();
                guard.status = FileCacheStatus::Dirty {
                    lock_mtime: init_lock_mtime,
                    done_rx,
                };
                let _ = done_tx.send(true);
            }
        }

        let mtime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;

        guard.file.seek(SeekFrom::Start(offset)).await?;
        guard.file.write_all(data).await?;

        let new_size = guard.file_size.max(offset + data.len() as u64);
        log::debug!(
            "Cached file {:?} is dirty, size: {} -> {}",
            self.remote_id,
            guard.file_size,
            new_size,
        );
        guard.file_size = new_size;

        Ok((guard.file_size, mtime))
    }

    async fn download(
        this: Arc<FileCache>,
        tx: watch::Sender<u64>,
        media: Media,
        client: Client,
        chat: Chat,
        file_size: u64,
    ) {
        log::debug!("Start downloading ({} bytes)", file_size);

        let mut pos = 0u64;

        let complete = |mut guard: MutexGuard<'_, FileCacheState>, download_size: u64| {
            log::debug!(
                "Cache {:?} is fully available (downloaded {} bytes, total {} bytes)",
                this.remote_id,
                download_size,
                guard.file_size,
            );

            match guard.status {
                FileCacheStatus::Downloading { truncate: Some(_) } => {
                    log::debug!(
                        "Pending upload for truncated file {:?}, size: {}",
                        this.remote_id,
                        guard.file_size,
                    );

                    let document = if let Media::Document(document) = media.clone() {
                        document
                    } else {
                        unreachable!();
                    };

                    this.upload(&mut guard, document.name(), &client, &chat);
                }
                FileCacheStatus::Downloading { truncate: None } => {
                    guard.status = FileCacheStatus::Ready;
                }
                _ => unreachable!(),
            }
        };

        let mut iter = client.iter_download(&media);
        loop {
            let ret = iter.next().await;

            let mut guard = this.state.lock().await;

            match ret {
                Err(_) => {
                    guard.status = FileCacheStatus::DownloadFailed;
                    return;
                }
                Ok(chunk) if chunk == None => {
                    break;
                }
                _ => (),
            }

            let mut chunk = ret.unwrap().unwrap();

            let download_size = match guard.status {
                FileCacheStatus::Downloading {
                    truncate: Some(download_size),
                } => download_size,
                // If there is no pending set_len, download should be aborted when removed from cache.
                FileCacheStatus::Downloading { truncate: None }
                    if Arc::strong_count(&this) != 1 =>
                {
                    guard.file_size
                }
                FileCacheStatus::Downloading { .. } | FileCacheStatus::Invalidated => return,
                _ => unreachable!(),
            };
            assert!(download_size <= guard.file_size);

            // Truncate extra data if `set_len` is called.
            let rest_len = download_size.saturating_sub(pos);
            if rest_len < chunk.len() as u64 {
                chunk.truncate(rest_len as usize);
            }

            if !chunk.is_empty() {
                guard.file.seek(SeekFrom::Start(pos)).await.unwrap();
                guard.file.write_all(&chunk).await.unwrap();
                pos += chunk.len() as u64;
            }
            log::trace!(
                "Write {} bytes to cache {:?}, current pos: {}, total need download: {}, file size: {}",
                chunk.len(),
                this.remote_id,
                pos,
                download_size,
                guard.file_size,
            );

            if pos < download_size {
                // We are holding `state`.
                tx.send(pos).unwrap();
            } else {
                // We are holding `state`.
                // The file size may be larger then download size due to set_len.
                // Space after data written is already zero as expected.
                tx.send(guard.file_size).unwrap();

                complete(guard, download_size);
                log::debug!("Download finished ({} bytes)", file_size);

                return;
            }
        }

        let mut guard = this.state.lock().await;
        let download_size = match guard.status {
            FileCacheStatus::Downloading { truncate } => {
                truncate.map(|sz| sz).unwrap_or(guard.file_size)
            }
            FileCacheStatus::Invalidated => return,
            _ => unreachable!(),
        };

        if pos < download_size {
            log::error!(
                "Download failed of {:?}, got {}/{}",
                this.remote_id,
                pos,
                download_size,
            );
            guard.status = FileCacheStatus::DownloadFailed;
        } else {
            // File is set to a larger length than remote side.
            complete(guard, download_size);
            log::debug!("Download finished ({} bytes)", file_size);
        }
    }

    fn upload(
        self: &Arc<Self>,
        guard: &mut MutexGuard<'_, FileCacheState>,
        name: &str,
        client: &Client,
        chat: &Chat,
    ) {
        let (done_tx, done_rx) = watch::channel(false);
        let init_lock_mtime = Instant::now();
        guard.status = FileCacheStatus::Dirty {
            lock_mtime: init_lock_mtime,
            done_rx,
        };

        let this = self.clone();
        let file_name = String::from(name);
        let tmp_client = client.clone();
        let tmp_chat = chat.clone();
        tokio::spawn(async move {
            let is_up_to_date = |status: &FileCacheStatus| matches!(status, FileCacheStatus::Dirty { lock_mtime, .. } if *lock_mtime == init_lock_mtime);

            let name = file_name;
            let chat = tmp_chat;
            let client = tmp_client;

            // Check not changed since last lock.
            let file_size = {
                let guard = this.state.lock().await;
                if !is_up_to_date(&guard.status) {
                    return;
                }
                guard.file_size
            };

            let uploaded: Uploaded;
            {
                let mut guard = this.state.lock().await;

                if !is_up_to_date(&guard.status) {
                    log::debug!("Upload of {:?} outdates", this.remote_id);
                    return;
                }

                assert_eq!(file_size, guard.file_size, "Truncation restarts uploading");
                if file_size == 0 {
                    let buf = vec![0];
                    let mut stream = std::io::Cursor::new(buf);

                    let uploaded_file = match client
                        .clone()
                        .upload_stream(&mut stream, 1, name.clone())
                        .await
                    {
                        Ok(f) => f,
                        Err(err) => {
                            log::error!(
                                "Failed to upload file of {} ({} bytes) {}",
                                this.remote_id,
                                0,
                                err,
                            );
                            // TODO: retry
                            return;
                        }
                    };
                    uploaded = uploaded_file;
                } else {
                    let mut buf = vec![0u8; file_size as usize];
                    if let Err(err) = guard.file.seek(SeekFrom::Start(0)).await {
                        log::error!("Failed to seek file {:?} {}", guard.file, err);
                        return;
                    }
                    if let Err(err) = guard.file.read_exact(&mut buf).await {
                        log::error!("Failed to read file {:?} {}", guard.file, err);
                        return;
                    }

                    drop(guard);

                    let mut stream = std::io::Cursor::new(buf);
                    let uploaded_file = match client
                        .upload_stream(&mut stream, file_size as usize, name.clone())
                        .await
                    {
                        Ok(f) => f,
                        Err(err) => {
                            log::error!(
                                "Failed to upload file of {} ({} bytes) {}",
                                this.remote_id,
                                file_size,
                                err,
                            );
                            // TODO: retry
                            return;
                        }
                    };
                    uploaded = uploaded_file;
                }
            }

            if let Err(err) = client
                .edit_message(
                    chat,
                    this.remote_id,
                    InputMessage::text(name).file(uploaded),
                )
                .await
            {
                log::error!("Failed to edit message of {} {}", this.remote_id, err,);
                return;
            } else {
                log::info!("Upload file of {} successful", this.remote_id);

                {
                    let mut guard = this.state.lock().await;
                    match guard.status {
                        FileCacheStatus::Downloading { .. } => unreachable!(),
                        FileCacheStatus::Dirty { lock_mtime, .. }
                            if lock_mtime == init_lock_mtime =>
                        {
                            guard.status = FileCacheStatus::Ready;
                        }
                        FileCacheStatus::Invalidated => {
                            log::warn!(
                                "Cache invalidated during the upload of {:?}, maybe both changed? Suppress update event",
                                this.remote_id,
                            );
                            return;
                        }
                        // Race another upload.
                        _ => {
                            log::debug!("Racing upload? Suppress update event");
                            return;
                        }
                    }
                }

                let _ = done_tx.send(true);
            }
        });
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
            log::debug!("File already cached: {}", remote_id);
            return Ok(0);
        }

        self.alloc(remote_id, None).await?;

        Ok(0)
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
        if let Some(file) = self.get(&remote_id) {
            let mut guard = file.state.lock().await;
            match guard.status {
                FileCacheStatus::Downloading { truncate } => {
                    let download_size = truncate.map(|sz| sz).unwrap_or(guard.file_size);
                    guard.status = FileCacheStatus::Downloading {
                        truncate: Some(download_size.min(new_size)),
                    };
                    guard.file_size = new_size;
                    guard.file.set_len(new_size).await.unwrap();
                    log::debug!(
                        "Pending another truncate for still downloading file {}",
                        remote_id,
                    );

                    return Ok(());
                }
                FileCacheStatus::Ready | FileCacheStatus::Dirty { .. } => {
                    log::debug!(
                        "Truncated cached file {}: {} -> {}",
                        remote_id,
                        guard.file_size,
                        new_size,
                    );
                    guard.file_size = new_size;
                    guard.file.set_len(new_size).await.unwrap();

                    //file.upload(&mut guard, name, &self.client, &self.chat);

                    return Ok(());
                }
                FileCacheStatus::DownloadFailed | FileCacheStatus::Invalidated => {}
            }
        } else if new_size == 0 {
            self.upload_empty_file(name, Some(remote_id)).await?;
            return Ok(());
        }

        self.alloc(remote_id, Some(new_size)).await?;

        Ok(())
    }

    pub async fn write_file(&self, remote_id: i32, offset: u64, data: &[u8]) -> Result<(u64, u32)> {
        if let Some(file) = self.get(&remote_id) {
            let (new_size, mtime) = file.write(offset, data).await?;
            Ok((new_size, mtime))
        } else {
            Err(Error::NotFound)
        }
    }

    pub async fn flush(&self, remote_id: i32, name: &str, block: bool) -> Result<()> {
        if let Some(file) = self.get(&remote_id) {
            let mut guard = file.state.lock().await;

            match guard.status {
                FileCacheStatus::DownloadFailed => return Err(Error::DownloadFailed),
                FileCacheStatus::Ready | FileCacheStatus::Invalidated => {
                    return Ok(());
                }
                FileCacheStatus::Downloading { .. } => {
                    let mut rx = guard.available_size.clone();
                    drop(guard);
                    while rx.changed().await.is_ok() {}
                    guard = file.state.lock().await;
                }
                FileCacheStatus::Dirty { .. } => {}
            }

            file.upload(&mut guard, name, &self.client, &self.chat);

            if block {
                loop {
                    let mut done_rx = match &mut guard.status {
                        FileCacheStatus::Downloading { .. } => unreachable!(),
                        FileCacheStatus::DownloadFailed => return Err(Error::DownloadFailed),
                        FileCacheStatus::Invalidated | FileCacheStatus::Ready => return Ok(()),
                        FileCacheStatus::Dirty { done_rx, .. } => done_rx.clone(),
                    };
                    drop(guard);

                    while done_rx.changed().await.is_ok() {}
                    // May be canceled by another modification during the upload.
                    if *done_rx.borrow() {
                        return Ok(());
                    }

                    guard = file.state.lock().await;
                }
            }

            Ok(())
        } else {
            Err(Error::NotFound)
        }
    }

    async fn alloc(&self, remote_id: i32, truncate: Option<u64>) -> Result<()> {
        let msgs = self
            .client
            .get_messages_by_id(&self.chat, &vec![remote_id])
            .await?;

        if let Some(msg) = msgs.into_iter().nth(0) {
            if let Some(raw_msg) = msg {
                if raw_msg.text().is_empty() {
                    self.insert_empty(raw_msg.id()).await?;
                } else if let Some(media) = raw_msg.media() {
                    if let Media::Document(_) = &media {
                        self.try_alloc_and_fetch(remote_id, truncate, &media)?;
                    } else {
                        return Err(Error::MediaInvalid);
                    }
                } else {
                    return Err(Error::MediaInvalid);
                }
                Ok(())
            } else {
                Err(Error::NotFound)
            }
        } else {
            Err(Error::NotFound)
        }
    }

    async fn insert_empty(&self, remote_id: i32) -> Result<Arc<FileCache>> {
        let (file, old) = {
            let mut files = self.files.lock().unwrap();
            let tmp_file = tempfile::tempfile_in(&self.dir)?;
            let (file, _) = FileCache::new(remote_id, tmp_file.into(), 0, FileCacheStatus::Ready);
            let old = files.put(remote_id, file.clone());
            (file, old)
        };
        if let Some(old) = old {
            old.state.lock().await.status = FileCacheStatus::Invalidated;
        }
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

    fn try_alloc_and_fetch(
        &self,
        remote_id: i32,
        truncate: Option<u64>,
        media: &Media,
    ) -> io::Result<Option<Arc<FileCache>>> {
        let media_size = if let Media::Document(document) = media {
            document.size() as u64
        } else {
            unreachable!();
        };
        let (file_size, download_truncate) = match truncate {
            None => (media_size, None),
            Some(new_size) => (new_size, Some(media_size.min(new_size))),
        };

        let mut files = self.files.lock().unwrap();
        if let Some(state) = files.get_mut(&remote_id) {
            return Ok(Some(state.clone()));
        }

        let tmp_file = tempfile::tempfile_in(&self.dir)?;
        tmp_file.set_len(file_size)?;

        let (file, tx) = FileCache::new(
            remote_id,
            tmp_file.into(),
            file_size,
            FileCacheStatus::Downloading {
                truncate: download_truncate,
            },
        );
        files.put(remote_id, file.clone());

        tokio::spawn(FileCache::download(
            file.clone(),
            tx,
            media.clone(),
            self.client.clone(),
            self.chat.clone(),
            file_size,
        ));

        Ok(Some(file))
    }
}
