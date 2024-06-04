pub mod protocol;

use anyhow::{anyhow, Context, Result};
use async_ssh2_lite::{AsyncSession, AsyncSessionStream};
use futures::{select_biased, AsyncReadExt as _, FutureExt as _};
use gpui::{BackgroundExecutor, Task};
use protocol::{
    envelope, message_len_from_buffer, read_message_with_len, write_message, Envelope,
    MESSAGE_LEN_SIZE,
};
use smol::{
    channel::{Receiver, Sender},
    Async,
};
use std::{
    net::{SocketAddr, TcpStream},
    path::Path,
    sync::atomic::{AtomicU32, Ordering::SeqCst},
};

const SERVER_BINARY_LOCAL_PATH: &str = "target/debug/remote_server";
const SERVER_BINARY_REMOTE_PATH: &str = "./.remote_server";
// const SERVER_BINARY_REMOTE_PATH: &str = "/Users/max/code/zed/target/debug/remote_server";

pub struct SshSession {
    next_message_id: AtomicU32,
    stdin: Sender<Envelope>,
    stdout: Receiver<Envelope>,
    _task: Task<Result<()>>,
}

#[derive(Debug, Copy, Clone)]
pub struct MessageId(#[allow(unused)] u32);

impl SshSession {
    pub async fn new(
        address: SocketAddr,
        user: &str,
        password: &str,
        executor: BackgroundExecutor,
    ) -> Result<Self> {
        let (stdin_tx, stdin_rx) = smol::channel::unbounded::<Envelope>();
        let (stdout_tx, stdout_rx) = smol::channel::unbounded::<Envelope>();

        let stream = Async::<TcpStream>::connect(address)
            .await
            .context("failed to connect to remote address")?;

        let mut session =
            AsyncSession::new(stream, None).context("failed to create ssh session")?;
        session.handshake().await.context("ssh handshake failed")?;
        session.userauth_password(user, password).await.unwrap();

        ensure_server_binary(&session).await?;

        let mut channel = session
            .channel_session()
            .await
            .context("failed to create channel")?;
        channel.exec(SERVER_BINARY_REMOTE_PATH).await?;
        let mut stderr = channel.stderr();

        let _task = executor.spawn(async move {
            let mut stdin_buffer = Vec::new();
            let mut stdout_buffer = Vec::new();
            let mut stderr_buffer = Vec::new();
            let mut stderr_offset = 0;

            loop {
                stdout_buffer.resize(MESSAGE_LEN_SIZE, 0);
                stderr_buffer.resize(stderr_offset + 1024, 0);

                select_biased! {
                    input = stdin_rx.recv().fuse() => {
                        if let Ok(input) = input {
                            write_message(&mut channel, &mut stdin_buffer, input).await?;
                        } else {
                            log::info!("input channel dropped");
                            return Ok(())
                        }
                    }

                    result = channel.read(&mut stdout_buffer).fuse() => {
                        match result {
                            Ok(len) => {
                                if len == 0 {
                                    let status = channel.exit_status()?;
                                    if status != 0 {
                                        let signal = channel.exit_signal().await?;
                                        log::info!("channel exited with status: {status:?}, signal: {:?}", signal.error_message);
                                    }
                                    return Ok(());
                                }

                                if len < stdout_buffer.len() {
                                    channel.read_exact(&mut stdout_buffer[len..]).await?;
                                }

                                let message_len = message_len_from_buffer(&stdout_buffer);
                                match read_message_with_len(&mut channel, &mut stdout_buffer, message_len).await {
                                    Ok(envelope) => {
                                        stdout_tx.send(envelope).await?;
                                    }
                                    Err(error) => {
                                        log::error!("error decoding message {error:?}");
                                    }
                                }
                            }
                            Err(error) => {
                                Err(anyhow!("error reading stdout: {error:?}"))?;
                            }
                        }
                    }

                    result = stderr.read(&mut stderr_buffer[stderr_offset..]).fuse() => {
                        match result {
                            Ok(len) => {
                                stderr_offset += len;
                                let mut start_ix = 0;
                                while let Some(ix) = stderr_buffer[start_ix..stderr_offset].iter().position(|b| b == &b'\n') {
                                    let line_ix = start_ix + ix;
                                    let content = String::from_utf8_lossy(&stderr_buffer[start_ix..line_ix]);
                                    start_ix = line_ix + 1;
                                    log::error!("stderr: {}", content);
                                }
                                stderr_buffer.drain(0..start_ix);
                                stderr_offset -= start_ix;
                            }
                            Err(error) => {
                                Err(anyhow!("error reading stderr: {error:?}"))?;
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            next_message_id: AtomicU32::new(0),
            stdin: stdin_tx,
            stdout: stdout_rx,
            _task,
        })
    }

    pub async fn send(&self, payload: envelope::Payload) -> MessageId {
        let id = self.next_message_id.fetch_add(1, SeqCst);
        self.stdin
            .send(Envelope {
                id,
                responding_to: None,
                payload: Some(payload),
            })
            .await
            .ok();
        MessageId(id)
    }

    pub async fn recv(&self) -> Option<(envelope::Payload, Option<MessageId>)> {
        let envelope = self.stdout.recv().await.ok()?;
        Some((envelope.payload?, envelope.responding_to.map(MessageId)))
    }
}

async fn ensure_server_binary<S: AsyncSessionStream + Send + Sync + 'static>(
    session: &AsyncSession<S>,
) -> Result<()> {
    let src_path = Path::new(SERVER_BINARY_LOCAL_PATH);
    let dst_path = Path::new(SERVER_BINARY_REMOTE_PATH);
    let ftp = session
        .sftp()
        .await
        .context("failed to initialize sftp channel")?;

    // let server_binary_exists = ftp
    //     .stat(dst_path)
    //     .await
    //     .map_or(false, |stats| stats.is_file());
    // if server_binary_exists {
    //     return Ok(());
    // }

    let mut src_file = smol::fs::File::open(src_path)
        .await
        .with_context(|| format!("failed to open server binary {src_path:?}"))?;
    let mut dst_file = ftp
        .create(dst_path)
        .await
        .context("failed to create server binary")?;
    let result = smol::io::copy(&mut src_file, &mut dst_file).await;
    let mut stat = ftp.stat(dst_path).await?;
    stat.perm = Some(0o755);
    ftp.setstat(dst_path, stat).await?;
    if result.is_err() {
        ftp.unlink(dst_path)
            .await
            .context("failed to remove server binary")?;
    }
    result?;

    Ok(())
}
