mod server;
mod features;

#[tokio::main]
async fn main() {
    server::network::start().await;
}
