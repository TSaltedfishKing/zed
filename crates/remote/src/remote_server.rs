use anyhow::{anyhow, Result};
use fs::{Fs, RealFs};
use futures::channel::mpsc::{self, UnboundedSender};
use gpui::BackgroundExecutor;
use remote::protocol::{read_message, write_message, MessageId};
use rpc::proto::{self, envelope::Payload, Envelope, Error};
use smol::{io::AsyncWriteExt, stream::StreamExt, Async};
use std::{
    env, io,
    path::Path,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};
use text::LineEnding;
use util::ResultExt;

fn main() {
    env::set_var("RUST_BACKTRACE", "1");

    let (_, background) = gpui::current_platform_executors();
    let (request_tx, mut request_rx) = mpsc::unbounded();
    let (response_tx, mut response_rx) = mpsc::unbounded();

    let mut server = Server {
        fs: RealFs::new(Default::default(), None),
        executor: background.clone(),
    };

    let mut stdin = Async::new(io::stdin()).unwrap();
    let mut stdout = Async::new(io::stdout()).unwrap();

    let stdout_task = background.spawn(async move {
        let mut output_buffer = Vec::new();
        while let Some(response) = response_rx.next().await {
            write_message(&mut stdout, &mut output_buffer, response).await?;
            stdout.flush().await?;
        }
        anyhow::Ok(())
    });

    let stdin_task = background.spawn(async move {
        let mut input_buffer = Vec::new();
        loop {
            let message = match read_message(&mut stdin, &mut input_buffer).await {
                Ok(message) => message,
                Err(error) => {
                    eprintln!("error reading message: {:?}", error);
                    break;
                }
            };
            request_tx.unbounded_send(message).ok();
        }
    });

    let request_task = background.spawn(async move {
        while let Some(request) = request_rx.next().await {
            if let Some(payload) = request.payload {
                let response = Response(Arc::new(ResponseInner {
                    id: MessageId(request.id),
                    tx: response_tx.clone(),
                }));
                if let Err(error) = server.handle_message(payload, response.clone()).await {
                    response.send_error(error);
                }
            }
        }
    });

    background.block(async move {
        request_task.await;
        stdin_task.await;
        stdout_task.await.log_err();
    });
}

struct Server {
    fs: RealFs,
    executor: BackgroundExecutor,
}

#[derive(Clone)]
struct Response(Arc<ResponseInner>);

struct ResponseInner {
    id: MessageId,
    tx: UnboundedSender<Envelope>,
}

impl Server {
    async fn handle_message(&mut self, message: Payload, response: Response) -> Result<()> {
        match message {
            Payload::Ping(_) => self.ping(response),
            Payload::ReadFile(request) => self.read_file(request, response).await,
            Payload::ReadDir(request) => self.read_dir(request, response).await,
            Payload::ReadLink(request) => self.read_link(request, response).await,
            Payload::Canonicalize(request) => self.canonicalize(request, response).await,
            Payload::Stat(request) => self.stat(request, response).await,
            Payload::Watch(request) => self.watch(request, response).await,
            Payload::WriteFile(request) => self.write_file(request, response).await,
            _ => {
                response.send_error(anyhow!("unhandled request type"));
                Ok(())
            }
        }
    }

    fn ping(&self, response: Response) -> Result<()> {
        response.send(Payload::Pong(proto::Pong {}));
        Ok(())
    }

    async fn read_file(&self, request: proto::ReadFile, response: Response) -> Result<()> {
        let content = self.fs.load(Path::new(&request.path)).await?;
        response.send(Payload::String(content));
        Ok(())
    }

    async fn read_link(&self, request: proto::ReadLink, response: Response) -> Result<()> {
        let content = self.fs.read_link(Path::new(&request.path)).await?;
        response.send(Payload::String(content.to_string_lossy().to_string()));
        Ok(())
    }

    async fn canonicalize(&self, request: proto::Canonicalize, response: Response) -> Result<()> {
        let content = self.fs.canonicalize(Path::new(&request.path)).await?;
        response.send(Payload::String(content.to_string_lossy().to_string()));
        Ok(())
    }

    async fn read_dir(&self, request: proto::ReadDir, response: Response) -> Result<()> {
        let mut stream = self.fs.read_dir(Path::new(&request.path)).await?;
        self.executor
            .spawn(async move {
                while let Some(item) = stream.next().await {
                    match item {
                        Ok(path) => {
                            response.send(Payload::String(path.to_string_lossy().to_string()))
                        }
                        Err(error) => response.send_error(error),
                    }
                }
            })
            .detach();
        Ok(())
    }

    async fn watch(&self, request: proto::Watch, response: Response) -> Result<()> {
        let (mut stream, _) = self
            .fs
            .watch(
                Path::new(&request.path),
                Duration::from_millis(request.latency),
            )
            .await;
        self.executor
            .spawn(async move {
                while let Some(event) = stream.next().await {
                    response.send(Payload::Event(proto::Event {
                        paths: event
                            .into_iter()
                            .map(|path| path.to_string_lossy().to_string())
                            .collect(),
                    }))
                }
            })
            .detach();
        Ok(())
    }

    async fn stat(&self, request: proto::Stat, response: Response) -> Result<()> {
        let metadata = self.fs.metadata(Path::new(&request.path)).await?;
        if let Some(metadata) = metadata {
            let proto_metadata = proto::Metadata {
                is_dir: metadata.is_dir,
                is_symlink: metadata.is_symlink,
                mtime: metadata
                    .mtime
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                inode: metadata.inode,
            };
            response.send(Payload::Metadata(proto_metadata));
        }
        Ok(())
    }

    async fn write_file(&self, request: proto::WriteFile, _: Response) -> Result<()> {
        self.fs
            .save(
                Path::new(&request.path),
                &request.content.into(),
                if request.line_ending == proto::write_file::LineEnding::Unix as i32 {
                    LineEnding::Unix
                } else {
                    LineEnding::Windows
                },
            )
            .await
    }
}

impl Response {
    fn send(&self, payload: Payload) {
        self.0
            .tx
            .unbounded_send(Envelope {
                original_sender_id: None,
                id: 0,
                payload: Some(payload),
                responding_to: Some(self.0.id.0),
            })
            .ok();
    }

    fn send_error(&self, error: anyhow::Error) {
        self.send(Payload::Error(Error {
            code: 0,
            tags: Vec::new(),
            message: error.to_string(),
        }))
    }
}

impl Drop for ResponseInner {
    fn drop(&mut self) {
        self.tx
            .unbounded_send(Envelope {
                original_sender_id: None,
                id: 0,
                payload: None,
                responding_to: Some(self.id.0),
            })
            .ok();
    }
}
