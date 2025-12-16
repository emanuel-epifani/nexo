mod server;
mod features;
mod debug;
mod utils;

#[tokio::main]
async fn main() {
    server::network::start().await;
}
