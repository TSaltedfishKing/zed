use fs::Fs as _;
use gpui::App;
use remote::{RemoteFs, SshSession};
use smol::stream::StreamExt;
use std::time::Duration;

fn main() {
    env_logger::init();

    App::new().with_assets(()).run(|cx| {
        let address = ([127, 0, 0, 1], 22).try_into().unwrap();
        let executor = cx.background_executor().clone();
        cx.background_executor()
            .spawn(async move {
                let session = SshSession::new(address, "testuser", "password", executor.clone())
                    .await
                    .unwrap();
                let fs = RemoteFs::new(session);

                for i in 0.. {
                    match i % 3 {
                        0 => {
                            eprintln!("load file:");
                            let contents = fs.load(".zsh_history".as_ref()).await;
                            eprintln!("  contents: {contents:?}");
                        }
                        1 => {
                            eprintln!("read dir:");
                            let mut stream = fs.read_dir(".".as_ref()).await.unwrap();
                            while let Some(entry) = stream.next().await {
                                eprintln!("  entry: {entry:?}");
                            }
                        }
                        2 => {
                            eprintln!("stat dir:");
                            let metadata = fs.metadata(".".as_ref()).await.unwrap();
                            eprintln!("  metadata: {metadata:?}");
                        }
                        _ => {}
                    };

                    executor.timer(Duration::from_millis(100)).await;
                }
            })
            .detach();
    });
}
