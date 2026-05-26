use std::path::Path;
use std::process::Command;

// Always rebuild the frontend on every `cargo build`. No file observation,
fn main() {
    let dashboard_dir = "dashboard";
    if !Path::new(dashboard_dir).exists() {
        println!("cargo:warning=Frontend directory not found. Skipping frontend build.");
        return;
    }

    if !Path::new(&format!("{}/node_modules", dashboard_dir)).exists() {
        let status = Command::new("npm")
            .args(["install"])
            .current_dir(dashboard_dir)
            .status()
            .expect("npm not found in PATH \u{2014} install Node.js >= 18 to build the dashboard");
        if !status.success() {
            panic!("Frontend dependency installation failed");
        }
    }

    let status = Command::new("npm")
        .args(["run", "build"])
        .current_dir(dashboard_dir)
        .status()
        .expect("npm not found in PATH \u{2014} install Node.js >= 18 to build the dashboard");
    if !status.success() {
        panic!("Frontend build failed");
    }
}