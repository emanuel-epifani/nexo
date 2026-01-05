fn main() {
    // Re-run this build script if the frontend build directory changes
    // This ensures that 'cargo build' picks up changes in 'dashboard-web/dist'
    // when using rust-embed.
    println!("cargo:rerun-if-changed=dashboard-web/dist");
}

