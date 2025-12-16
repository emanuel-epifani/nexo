use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;
use once_cell::sync::Lazy;

static TIMERS: Lazy<Mutex<HashMap<&'static str, Instant>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub fn time_start(label: &'static str) {
    TIMERS.lock().unwrap().insert(label, Instant::now());
}

pub fn time_end(label: &'static str) {
    if let Some(start) = TIMERS.lock().unwrap().remove(label) {
        println!("{}: {:?}", label, start.elapsed());
    }
}
