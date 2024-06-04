use gpui::App;
use remote::{
    protocol::{envelope::Payload, Ping, ReadFile},
    SshSession,
};
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

                for i in 0.. {
                    let request = if i % 2 == 0 {
                        Payload::Ping(Ping {})
                    } else {
                        Payload::ReadFile(ReadFile {
                            path: "/the/path".into(),
                        })
                    };

                    dbg!(&request);
                    session.send(request).await;

                    if let Some((payload, reply_to)) = session.recv().await {
                        println!("{:?} {:?}", payload, reply_to);
                        executor.timer(Duration::from_millis(100)).await;
                    } else {
                        println!("done");
                        break;
                    }
                }
            })
            .detach();
    });
}
