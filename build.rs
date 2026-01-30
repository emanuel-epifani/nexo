use std::process::Command;
use std::path::Path;

fn main() {
    // 1. Monitoriamo i file del frontend per rebuildare solo se cambia qualcosa
    println!("cargo:rerun-if-changed=dashboard/src");
    println!("cargo:rerun-if-changed=dashboard/public");
    println!("cargo:rerun-if-changed=dashboard/index.html");
    println!("cargo:rerun-if-changed=dashboard/package.json");
    println!("cargo:rerun-if-changed=dashboard/vite.config.ts");

    let dashboard_dir = "dashboard";

    // Verifica che la cartella esista (per sicurezza)
    if !Path::new(dashboard_dir).exists() {
        println!("cargo:warning=Frontend directory not found. Skipping frontend build.");
        return;
    }

    // 2. Eseguiamo 'npm install' solo se node_modules non esiste (o se vuoi essere sicuro sempre)
    // Nota: puoi commentare questa parte se le dipendenze non cambiano spesso per velocizzare
    if !Path::new(&format!("{}/node_modules", dashboard_dir)).exists() {
        println!("cargo:warning=Installing frontend dependencies...");
        let status = Command::new("npm")
            .args(["install"])
            .current_dir(dashboard_dir)
            .status()
            .expect("Failed to run npm install");

        if !status.success() {
            panic!("Frontend dependency installation failed");
        }
    }

    // 3. Eseguiamo 'npm run build'
    println!("cargo:warning=Building frontend assets...");
    let status = Command::new("npm")
        .args(["run", "build"])
        .current_dir(dashboard_dir)
        .status()
        .expect("Failed to run npm run build");

    if !status.success() {
        panic!("Frontend build failed");
    }

    // 4. Istruiamo Cargo di ricontrollare se la dist cambia (per rust-embed)
    println!("cargo:rerun-if-changed=dashboard/dist");
}