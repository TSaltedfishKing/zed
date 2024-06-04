use anyhow::Result;
use futures::channel::mpsc::{self, UnboundedSender};
use remote::protocol::{
    envelope::Payload, read_message, write_message, Envelope, Error, Pong, ReadFileResponse,
};
use smol::{io::AsyncWriteExt, stream::StreamExt, Async};
use std::{env, io};
use util::ResultExt;

fn main() {
    env::set_var("RUST_BACKTRACE", "1");

    let (request_tx, mut request_rx) = mpsc::unbounded();
    let (response_tx, mut response_rx) = mpsc::unbounded();
    let mut stdin = Async::new(io::stdin()).unwrap();
    let mut stdout = Async::new(io::stdout()).unwrap();

    let stdout_task = smol::spawn(async move {
        let mut output_buffer = Vec::new();
        while let Some(response) = response_rx.next().await {
            write_message(&mut stdout, &mut output_buffer, response).await?;
            stdout.flush().await?;
        }
        anyhow::Ok(())
    });

    let stdin_task = smol::spawn(async move {
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

    let request_task = smol::spawn(async move {
        while let Some(request) = request_rx.next().await {
            if let Some(payload) = request.payload {
                eprintln!("request: {:?}", payload);
                let response = match handle_message(payload, response_tx.clone()).await {
                    Ok(response) => response,
                    Err(error) => Payload::Error(Error {
                        message: error.to_string(),
                    }),
                };
                eprintln!("response: {:?}", response);
                response_tx
                    .unbounded_send(Envelope {
                        id: 0,
                        payload: Some(response),
                        responding_to: Some(request.id),
                    })
                    .ok();
            }
        }
    });

    smol::block_on(async move {
        request_task.await;
        stdin_task.await;
        stdout_task.await.log_err();
    });
}

async fn handle_message(message: Payload, _response: UnboundedSender<Envelope>) -> Result<Payload> {
    match message {
        Payload::Ping(_) => Ok(Payload::Pong(Pong {})),
        Payload::ReadFile(_) => Ok(Payload::ReadFileResponse(ReadFileResponse {
            content: "Hello, world!".to_string(),
        })),
        _ => Err(anyhow::anyhow!("unhandled message {:?}", message)),
    }
}
