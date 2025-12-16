




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

    // --------- ARRAY (FIXED SIZE, same types, STACK) ---------
    let a: [i32; 3] = [1, 2, 3];
    let b = a; // COPY because i32 is Copy and size is known at compile time
    // Arrays live on the stack if their size is known and reasonable
    // Arrays do NOT grow and do NOT allocate heap

    // --------- TUPLE (FIXED SIZE, heterogeneous types, stack/heap)---------
    // Tuple of Copy types → Copy
    let t1: (i32, bool) = (42, true);
    let t2 = t1; // COPY
    // Tuple containing non-Copy types → NOT Copy
    let t3: (String, i32) = (String::from("hello"), 5);
    let t4 = t3; // MOVE because String is not Copy
    // println!("{:?}", t3); // ❌ moved
    println!("{}", (t4.0));

    // --------- VECTOR (GROWABLE SIZE, same types, HEAP) ---------
    let v1: Vec<i32> = vec![1, 2, 3]; // heap allocation
    let v2 = v1; // MOVE (Vec owns heap)
    // println!("{:?}", v1); // ❌ moved
    println!("{:?}", v2);

    // --------- STRING SLICE (&str) ---------
    let s1: &str = "hello"; // string literal, static memory in binary
    let s2: &str = s1;      // COPY (pointer + length)
    // &str does NOT own data, does NOT allocate heap

    // --------- OWNED STRING (String) ---------
    let s1: String = String::from("hello"); // heap allocation
    let s2: String = s1; // MOVE (ptr, len, cap copied; s1 invalidated)
    // println!("{}", s1); // ❌ ERROR: value moved
    println!("{}", s2);
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

[Tipo comando: 1 byte] [Lunghezza Payload: 4 byte] [Payload...]

Ogni Request & Response ha questa struttura:
[  CMD/STATUS (1 byte)  ] [  LENGTH (4 byte BE)  ] [  PAYLOAD (N bytes)  ]
Mapping Comandi (Request)
0x01: PING (Payload vuoto)
0x02: KV_SET -> Payload: [KeyLen 4B][Key][Value]
0x03: KV_GET -> Payload: [Key]
0x04: KV_DEL -> Payload: [Key]

Mapping Status (Response)
0x00: OK (Payload opzionale)
0x01: ERROR (Payload: messaggio errore stringa)
0x02: NULL (Es. chiave non trovata)

// Protocollo Binario Custom: INFINITAMENTE più semplice
pub fn parse_frame(buf: &mut &[u8]) -> Result<Command, Error> {
    // 1. Controllo header (1 byte tipo + 4 byte lunghezza = 5 byte)
    if buf.len() < 5 { return Err(Incomplete); }

    // 2. Leggi header (matematica pura, niente scansioni)
    let cmd_type = buf[0];
    let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;

    // 3. Controllo se ho tutto il payload
    if buf.len() < 5 + len { return Err(Incomplete); }

    // 4. Prendo i dati (Zero Copy)
    let payload = &buf[5..5+len];
    *buf = &buf[5+len..]; // Avanzo cursore

    // 5. Mappo il comando
    match cmd_type {
        1 => Ok(Command::Pub(payload)),
        2 => Ok(Command::Sub(payload)),
        _ => Err(Invalid)
    }
}
 */