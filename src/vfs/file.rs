use crate::vfs::Result;

use bytes::Bytes;
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

const CACHE_SIZE: usize = 128;

struct FileCacheState {
    file_size: u64,
    file: tokio::fs::File,
}
pub struct FileCache {
    state: Mutex<FileCacheState>,
}

impl FileCache {
    fn new(file: tokio::fs::File) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(FileCacheState { file_size: 0, file }),
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

    pub async fn read_all(this: &Arc<Self>) -> Result<Bytes> {
        let mut guard = this.state.lock().await;

        let mut buf = vec![0u8; guard.file_size as usize];
        guard.file.seek(SeekFrom::Start(0)).await?;
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

        Ok((guard.file_size, mtime))
    }
}

pub struct DiskCache {
    dir: PathBuf,
    files: SyncMutex<LruCache<i32, Arc<FileCache>>>,
}

impl DiskCache {
    pub fn new() -> Self {
        Self {
            dir: PathBuf::new(),
            files: SyncMutex::new(LruCache::new(NonZeroUsize::new(CACHE_SIZE).unwrap())),
        }
    }

    pub fn get(&self, remote_id: &i32) -> Option<Arc<FileCache>> {
        self.files.lock().unwrap().get_mut(remote_id).cloned()
    }

    pub async fn insert_empty(&self, remote_id: i32) -> Result<Arc<FileCache>> {
        let tmp_file = tempfile::tempfile_in(&self.dir)?;
        let mut files = self.files.lock().unwrap();
        let file = FileCache::new(tmp_file.into());
        files.put(remote_id, file.clone());
        Ok(file)
    }
}
