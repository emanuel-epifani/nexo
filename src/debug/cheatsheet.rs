/* Minimo indispensabile per padroneggiare RUST
    Enums + pattern matching

    Concorrenza MINIIMA
    - Arc<T>
    - Mutex<T> / RwLock<T>
    - thread / task

    Error handling reale (Result<T, E>)
    - Result come flusso normale
    - ? come early return
    - errori come valori, non eccezioni

    Vec<u8> come payload
    - Vec<u8>  // bytes, non testo
    
    
    Regola empirica:
    - assenza normale (topic non trovato) → Option<T>
    - fallimento (messaggio invalido) → Result<T, E>



    


 */



/*

 */




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
    ( Arc<Mutex<State>> / Arc<Mutex<T>> ) x gestire concorrenza
    abbiamo uno stato del broker
    contiene un contatore messaggi
    più parti del codice devono modificarlo
    - lo condividiamo con Arc (Atomic Reference Counted - xmette a + thread di possedere stesso oggetto)
    - lo proteggiamo con Mutex (Mutual Exclusion - garantisce che solo un thread ala volta possa modificarlo)

    Avvolgi: Metti il dato in un Mutex, e il Mutex in un Arc.
    Clona: Per ogni thread, fai un .clone() dell' Arc. Ottieni un nuovo "permesso" per lo stesso dato.
    Accedi: Dentro il thread, chiami .lock().
    Usa: Una volta ottenuto il lock, Rust ti permette di modificare il dato. Quando la variabile del lock esce dallo scope (fine del blocco), la chiave viene restituita automaticamente.



 */

/*
    // KV store (Redis-like):
    // - accesso per chiave indipendente
    // - molte operazioni concorrenti
    // -> DashMap

    // Queue (FIFO):
    // - ordine globale
    // - push/pop atomici
    // -> Mutex<VecDeque<T>>

    // MQTT router:
    // - tantissime publish (read)
    // - poche subscribe/unsubscribe (write)
    // -> RwLock<Router>

    // Stream (Kafka-like):
    // - append-only
    // - ordine per topic
    // - writer serializzato
    // -> Mutex<Vec<T>>

 */
fn concorrenza() {
    use std::sync::{Mutex, RwLock};
    use dashmap::DashMap;

    // 1️⃣ Mutex<T>
    // - 1 thread alla volta
    // - lock globale sulla struttura
    // - ideale per stato piccolo o ordine globale
    fn mutex_example() {
        let counter = Mutex::new(0);

        {
            let mut value = counter.lock().unwrap(); // unwrap OK solo per demo
            *value += 1;
        } // lock released here

        {
            let mut value = counter.lock().unwrap();
            *value += 1;
        }

        println!("{}", *counter.lock().unwrap());
    }

    // 2️⃣ RwLock<T>
    // - N reader in parallelo
    // - 1 writer esclusivo (blocca tutti)
    fn rwlock_example() {
        let data = RwLock::new(vec![1, 2, 3]);

        {
            let read_guard = data.read().unwrap();
            println!("first = {}", read_guard[0]);
        }

        {
            let mut write_guard = data.write().unwrap();
            write_guard.push(4);
        }

        println!("{:?}", data.read().unwrap());
    }

    // 3️⃣ DashMap<K, V>
    // - HashMap concorrente shardata
    // - lock automatico per shard/bucket
    // - get/set su chiavi diverse vanno in parallelo
    fn dashmap_example() {

        let map = DashMap::new();

        map.insert("a", 10);
        map.insert("b", 20);

        if let Some(v) = map.get("a") {
            println!("{}", *v);
        }

        map.entry("a").and_modify(|v| *v += 1);

        println!("{}", *map.get("a").unwrap());
    }

}



/*
    Struct (Dati) + Impl (Logica) + Trait (Comportamento condiviso)
    Separazione: I dati (struct) sono separati dalla logica (impl). Puoi avere più blocchi impl per la stessa struct (es. uno per i metodi base, uno per un Trait specifico).
        - self esplicito: A differenza di this in TS che è implicito, in Rust devi dichiarare come vuoi usare l'istanza:
        - &self: Solo lettura.
        - &mut self: Lettura e Scrittura.
        - self: "Consumo" (l'oggetto viene distrutto o spostato alla fine del metodo).
 */
fn oop() {
    // 1. STRUCT: Solo i Dati (lo Stato)
    // Niente logica qui, solo la forma dei dati.
    struct Wallet {
        owner: String,
        balance: f64,
    }

    // 2. TRAIT: L'Interfaccia (il Contratto)
    // Definisce COSA si può fare, ma non COME.
    // Equivalente a 'interface' in TS.
    trait Reportable {
        fn summary(&self) -> String;
    }

    // 3. IMPL: L'Implementazione dei Metodi (la Logica)
    // Qui leghi la logica ai dati della struct.
    impl Wallet {
        // "Metodo statico" / Costruttore (in TS: static new() o constructor)
        // Non prende 'self' come parametro.
        fn new(owner: &str) -> Self {
            Self {
                owner: owner.to_string(),
                balance: 0.0,
            }
        }

        // Metodo di istanza che LEGGE lo stato (in TS: getBalance())
        // &self = prende l'istanza in prestito sola lettura (borrowing immutabile)
        fn get_balance(&self) -> f64 {
            self.balance
        }

        // Metodo di istanza che MODIFICA lo stato (in TS: deposit())
        // &mut self = prende l'istanza in prestito esclusivo per modifica (borrowing mutabile)
        fn deposit(&mut self, amount: f64) {
            self.balance += amount;
            println!("Depositati {} per {}", amount, self.owner);
        }
    }

    // 4. IMPL di un TRAIT: Adesione al contratto
    // Implementiamo l'interfaccia 'Reportable' per 'Wallet'
    impl Reportable for Wallet {
        fn summary(&self) -> String {
            format!("Wallet di {}: Totale €{}", self.owner, self.balance)
        }
    }

    fn main() {
        // Istanziazione
        let mut my_wallet = Wallet::new("Emanuele");

        // Chiamata metodo mutabile
        my_wallet.deposit(100.0);

        // Chiamata metodo di lettura
        println!("Saldo attuale: {}", my_wallet.get_balance());

        // Uso del Trait
        println!("{}", my_wallet.summary());
    }
}


