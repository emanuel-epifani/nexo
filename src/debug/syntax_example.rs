// ===============================
// RUST CORE TYPES + MOVE / BORROW CHEAT SHEET (EXTENDED)
// Target audience: coming from TypeScript / JS
// Everything is VALID Rust unless commented otherwise
// Focus: low-level behavior, stack vs heap, copy vs move
// ===============================


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

// --------- DEEP COPY (clone) ---------
fn string_clone() {
    let s1 = String::from("hello");
    let s2 = s1.clone(); // deep copy: new heap allocation

    println!("s1: {}", s1);
    println!("s2: {}", s2);
}



// MOVE: ownership trasferita alla funzione
fn takes_ownership(s: String) {
    println!("{}", s);
} // s has been DROPPED here (end scope)

// BORROW (&): reference immutabile
fn borrows(s: &String) {
    println!("{}", s);
} // s has been BORROWED immutably (read-only)

// BORROW MUT (&mut): reference mutabile
fn borrows_mut(s: &mut String) {
    s.push_str("!");
} // s has been BORROWED mutably (read + write)


// SLICE [T], &[T], &str: reference on a portion of data (reference + length + link to original data) without take ownership on that
fn slice_array() {
    let a = [1, 2, 3, 4];
    let s: &[i32] = &a[1..3]; // slice: pointer + length
}

fn slice_vector() {
    let v = vec![10, 20, 30, 40];
    let vs: &[i32] = &v[0..2]; // slice into Vec
}

