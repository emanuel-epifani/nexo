mod server;
mod features;
mod debug;

#[tokio::main]
async fn main() {
    server::network::start().await;
}
