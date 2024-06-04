fn main() {
    let mut build = prost_build::Config::new();
    build
        .compile_protos(&["proto/remote.proto"], &["proto"])
        .unwrap();
}
