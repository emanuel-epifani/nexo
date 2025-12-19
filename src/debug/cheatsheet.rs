




fn types() {
    // -------------------- INTEGERS (SIGNED / UNSIGNED) --------------------
    // Signed integers
    let a: i8  = -128;               // range: -128 .. 127
    let b: i16 = -32_768;            // range: -32_768 .. 32_767
    let c: i32 = -2_147_483_648;     // range: ~ -2.1 billion .. +2.1 billion
    let d: i64 = -9_223_372_036_854_775_808; // range: ~ -9.22e18 .. +9.22e18
    let e: isize = 0;                // pointer-sized (32 or 64 bit, platform dependent)
    // Unsigned integers
    let f: u8  = 255;                // range: 0 .. 255
    let g: u16 = 65_535;             // range: 0 .. 65_535
    let h: u32 = 4_294_967_295;      // range: 0 .. ~4.29 billion
    let i: u64 = 18_446_744_073_709_551_615; // range: 0 .. ~1.84e19
    let j: usize = 0;                // pointer-sized, used for indexing
    // All integer types are Copy
    let x = c;
    let y = x; // bitwise COPY

    // -------------------- FLOATING --------------------
    let a: f32 = 3.15; // 32-bit IEEE-754
    let b: f64 = 3.151592653589793; // 64-bit IEEE-754 (default)
    let c = a; // COPY
    let d = b; // COPY

    // ------------------- BOOL / CHAR -------------------
    let t: bool = true;   // 1 byte
    let f = t;            // COPY
    let c: char = '🔥';   // 4 bytes (Unicode scalar value)
    let d = c;            // COPY

    // STRING slice -> (vista su un dato) -> stack
    let s1: &str = "hello";
    let s2: &str = s1; //copy reference

    // OWNED STRING (oggetto valorizzato a runtime) -> heap
    let s3: String = String::from("hello");
    let s4: String = s3; //moved ownership


    // ARRAY (a dimensione fissa) -> stack
    let mut students: [&str;2] = ["ema","luig"];

    // VECTOR (dimensione editabile) -> heap
    let mut teachers: Vec<&str> = vec!["Emilia", "Giovanni"];

}


/*
   OWNERSHIP
   Rust guarantees memory safety through ownership and borrowing rules.
   One owner at time can exist for every value.
   1) CLONE clone() -> new copy new allocation
   2) MOVE → transfer the ownership
   3) BORROW (&T, &mut T) → who can access (read or read-write) without owning
   4) SLICE (&[T], &str) → a borrow of portion of the data, tied to the original lifetime
 */

// CLONE: explicit deep copy (heap allocation)
fn clone() {
    let s1 = String::from("hello");
    let s2 = s1.clone(); // allocates NEW heap memory and copies the data

    println!("s1: {}", s1);
    println!("s2: {}", s2);
}

// MOVE: ownership transferred to the function
fn takes_ownership(s: String) {
    println!("{}", s);
} // s is DROPPED here (end of scope)

// BORROW immutable (&T): read-only access
fn borrows(s: &str) {
    println!("{}", s);
} // immutable borrow ENDS here; the OWNER is still valid

// BORROW mutable (&mut T): read + write access
fn borrows_mut(s: &mut String) {
    s.push_str("!");
} // mutable borrow ENDS here; the OWNER is still valid

// SLICE (&[T], &str): borrow a portion of data
fn slice() {
    let a = [1, 2, 3, 4];
    let s: &[i32] = &a[1..3]; // slice into array (print: [2, 3])

    let v = vec![10, 20, 30, 40];
    let vs: &[i32] = &v[0..2]; // slice into Vec (print: [10, 20])
}

/*  STRUCT + IMPL * TRAIT
    - struct = dati
    - impl = metodi
    - trati = interface
 */
fn oop() {

    /*
    struct Queue {
        size: usize,
    }

    impl Queue {
        // fn push(&mut self) { … }
    }

    trait Storage {
        // fn put(&mut self, k: Key, v: Value);
    }
    */
}

/*
Option<T>, Result<T, E>
 */
fn errors() {

}

/*
    Enums + pattern matching
    Rusti ti costringe a gestire ogni possibile valore dell'enum (se add un tipo, tutti gl iswitch/match daranno errore)
 */
fn enums_pattern_matching() {
    enum Command {
        Publish { topic: String, payload: Vec<u8> },
        Subscribe { topic: String },
        Ack { id: u64 },
    }
    let cmd : Command = Command::Subscribe { topic :String::from("dsds")};

    match cmd {
        Command::Publish { topic, payload } => { … }
        Command::Subscribe { topic } => { … }
        // Command::Ack { id } => { … },
        _ => {} //wildcard, tutti i casi non gestiti passano da qua
    }
}


/*
    Error handling reale (Result<T, E>)
    In Rust
     - una funzione può riuscire → Ok(value)
     - oppure fallire → Err(error)

    E il chiamante DEVE gestirlo. (scegli te dove applicarlo, si I/O, input esterni ecc, no code safatey)

    Devi interiorizzare
    - Result come flusso normale
    - ? come early return (? propaga l'errore automaticamente)
    - errori come valori, non eccezioni

    ? propaga l’errore così com’è (x prototipazione o vuoi solo propagare)
    map_err serve quando vuoi CAMBIARE l’errore (quando conosci e vuoi gestire quello specifico caso)

 */
fn error_handling() {

    enum Result<T,E> {
        Ok(T),
        Err(E)
    }

    fn handle_message(raw: &[u8]) -> Result<Message, ProtocolError> {
        let header = parse_header(raw)?;
        let payload = parse_payload(raw)?;
        Ok(Message::Publish { header, payload })
    }

    /*
         ? operatore di propagazione dell'errore (può esser usato solo dentor una funzione che ritorna Result)
     */
    fn read_port() -> Result<u16, String> {
        // se il file non esiste → ritorna subito Err
        let config = std::fs::read_to_string("config.txt")
            .map_err(|_| "cannot read config")?;
        // se il parse fallisce → ritorna subito Err
        let port = config.trim().parse::<u16>()
            .map_err(|_| "invalid port")?;
        // se tutto ok → Ok(port)
        Ok(port)
    }

    fn read_port() -> Result<u16, String> {
        let config = std::fs::read_to_string("config.txt")?;
        let port = config.trim().parse()?;
        Ok(port)
    }


}


/*
    Concorrenza MINIIMA
    - Arc<T>
    - Mutex<T> / RwLock<T>
    - thread / task
 */



/*
    Vec<u8> come payload
    - Vec<u8>  // bytes, non testo

 */