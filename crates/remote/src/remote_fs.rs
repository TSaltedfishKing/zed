// todo!(remove)
#![allow(unused)]

use crate::{
    protocol::{self as proto, envelope::Payload},
    SshSession,
};
use anyhow::{anyhow, Result};
use async_tar::Archive;
use fs::{CopyOptions, CreateOptions, Fs, Metadata, RemoveOptions, RenameOptions, Watcher};
use futures::{AsyncRead, Stream};
use git::repository::GitRepository;
use smol::stream::StreamExt;
use std::{
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};
use text::{LineEnding, Rope};

pub struct RemoteFs {
    session: SshSession,
}

struct RemoteWatcher {}

impl RemoteFs {
    pub fn new(session: SshSession) -> Self {
        Self { session }
    }
}

#[async_trait::async_trait]
impl Fs for RemoteFs {
    async fn create_dir(&self, path: &Path) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn create_symlink(&self, path: &Path, target: PathBuf) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn create_file(&self, path: &Path, options: CreateOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn create_file_with(
        &self,
        path: &Path,
        content: Pin<&mut (dyn AsyncRead + Send)>,
    ) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn extract_tar_file(
        &self,
        path: &Path,
        content: Archive<Pin<&mut (dyn AsyncRead + Send)>>,
    ) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn copy_file(&self, source: &Path, target: &Path, options: CopyOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn rename(&self, source: &Path, target: &Path, options: RenameOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn remove_dir(&self, path: &Path, options: RemoveOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn remove_file(&self, path: &Path, options: RemoveOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn trash_file(&self, path: &Path, _options: RemoveOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn trash_dir(&self, path: &Path, options: RemoveOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn open_sync(&self, path: &Path) -> Result<Box<dyn io::Read>> {
        Err(anyhow!("not implemented"))
    }

    async fn load(&self, path: &Path) -> Result<String> {
        let response = self
            .session
            .send(Payload::ReadFile(proto::ReadFile {
                path: path.to_string_lossy().to_string(),
            }))
            .one()
            .await?;
        if let Payload::String(response) = response {
            Ok(response)
        } else {
            Err(anyhow!("unexpected response"))
        }
    }

    async fn atomic_write(&self, path: PathBuf, data: String) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn save(&self, path: &Path, text: &Rope, line_ending: LineEnding) -> Result<()> {
        let response = self
            .session
            .send(Payload::WriteFile(proto::WriteFile {
                path: path.to_string_lossy().to_string(),
                content: text.to_string(),
                line_ending: match line_ending {
                    LineEnding::Unix => proto::write_file::LineEnding::Unix as i32,
                    LineEnding::Windows => proto::write_file::LineEnding::Windows as i32,
                },
            }))
            .next()
            .await;
        match response {
            Some(Payload::Error(error)) => Err(anyhow!("{}", error.message)),
            Some(_) => Err(anyhow!("unexpected response")),
            None => Ok(()),
        }
    }

    async fn canonicalize(&self, path: &Path) -> Result<PathBuf> {
        let response = self
            .session
            .send(Payload::Canonicalize(proto::Canonicalize {
                path: path.to_string_lossy().to_string(),
            }))
            .one()
            .await?;
        if let Payload::String(response) = response {
            Ok(response.into())
        } else {
            Err(anyhow!("unexpected response"))
        }
    }

    async fn is_file(&self, path: &Path) -> bool {
        false
    }

    async fn is_dir(&self, path: &Path) -> bool {
        false
    }

    async fn metadata(&self, path: &Path) -> Result<Option<Metadata>> {
        let response = self
            .session
            .send(Payload::Stat(proto::Stat {
                path: path.to_string_lossy().to_string(),
            }))
            .next()
            .await;
        match response {
            Some(Payload::Metadata(metadata)) => Ok(Some(Metadata {
                inode: metadata.inode,
                mtime: SystemTime::UNIX_EPOCH + Duration::from_millis(metadata.mtime),
                is_symlink: metadata.is_symlink,
                is_dir: metadata.is_dir,
            })),
            Some(Payload::Error(error)) => Err(anyhow!("{}", error.message)),
            _ => Ok(None),
        }
    }

    async fn read_link(&self, path: &Path) -> Result<PathBuf> {
        let response = self
            .session
            .send(Payload::ReadLink(proto::ReadLink {
                path: path.to_string_lossy().to_string(),
            }))
            .one()
            .await?;
        if let Payload::String(response) = response {
            Ok(response.into())
        } else {
            Err(anyhow!("unexpected response"))
        }
    }

    async fn read_dir(
        &self,
        path: &Path,
    ) -> Result<Pin<Box<dyn Send + Stream<Item = Result<PathBuf>>>>> {
        let stream = self.session.send(Payload::ReadDir(proto::ReadDir {
            path: path.to_string_lossy().to_string(),
        }));
        Ok(stream
            .filter_map(|item| match item {
                Payload::String(path) => Some(Ok(PathBuf::from(path))),
                Payload::Error(error) => Some(Err(anyhow!("{}", error.message))),
                _ => None,
            })
            .boxed())
    }

    async fn watch(
        &self,
        path: &Path,
        latency: Duration,
    ) -> (
        Pin<Box<dyn Send + Stream<Item = Vec<PathBuf>>>>,
        Arc<(dyn Watcher + 'static)>,
    ) {
        let stream = self.session.send(Payload::Watch(proto::Watch {
            path: path.to_string_lossy().to_string(),
            latency: latency.as_millis() as u64,
        }));
        (
            stream
                .filter_map(|item| match item {
                    Payload::Event(event) => {
                        Some(event.paths.into_iter().map(PathBuf::from).collect())
                    }
                    _ => None,
                })
                .boxed(),
            Arc::new(RemoteWatcher {}),
        )
    }

    fn open_repo(&self, dotgit_path: &Path) -> Option<Arc<dyn GitRepository>> {
        None
    }

    fn is_fake(&self) -> bool {
        false
    }

    async fn is_case_sensitive(&self) -> Result<bool> {
        Ok(false)
    }
}

impl Watcher for RemoteWatcher {
    fn add(&self, _path: &Path) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    fn remove(&self, _path: &Path) -> Result<()> {
        Err(anyhow!("not implemented"))
    }
}
