
Enums & Pattern Matching (match)
Structs & Traits (al posto di Classi & Interfacce)
Async/Await & Tokio (Concorrenza)
Consistenza Arc<Mutex>



Modulo	Struttura Dati	Razionale (Il "Perché")
KV	DashMap<String, Entry>	Velocità Pura. Le chiavi sono indipendenti. Non serve bloccare "A" se scrivo "B".
Queue	DashMap<String, Mutex<VecDeque>>	Ibrido. Code diverse sono parallele (DashMap), ma dentro una coda l'ordine è sacro (Mutex).
Stream	DashMap<String, RwLock<Vec<Msg>>>	Append-Only. Tanti consumer leggono la storia passata insieme (RwLock Read), solo uno scrive in coda (RwLock Write).
MQTT	RwLock<Node> (Topic Trie)	Read-Heavy. Il routing (chi riceve cosa?) cambia raramente (Subscribe), ma viene letto sempre (Publish).