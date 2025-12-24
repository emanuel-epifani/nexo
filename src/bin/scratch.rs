use std::sync::{Arc, Mutex};
use std::thread;

fn main() {
    // 1. Creazione (Lucchetto dentro il Contenitore)
    let contatore = Arc::new(Mutex::new(0));
    let mut handles = vec![];

    for _ in 0..10 {
        // 2. Clona l'Arc (aumenta il riferimento, non copia il dato)
        let contatore_clone = Arc::clone(&contatore);

        let handle = thread::spawn(move || {
            // 3. Chiedi la chiave (Lock)
            let mut num = contatore_clone.lock().unwrap();
            // 4. Modifica in sicurezza
            *num += 1;
        }); // <-- Qui la chiave viene restituita automaticamente!

        handles.push(handle);
    }

    for h in handles { h.join().unwrap(); }
    println!("Risultato: {}", *contatore.lock().unwrap());
}