
1. Quali problemi risolve una Queue nei microservizi?
Oltre al semplice "spostare dati", una coda risolve tre problemi critici:
- Peak Shaving (Livellamento del carico): Se hai un picco di 10.000 ordini al secondo, ma il tuo database ne regge solo 1.000, la coda fa da "polmone". Accetta tutto subito e permette ai worker di smaltire con calma senza far crashare il sistema.
- Disaccoppiamento Temporale: Il Producer (chi invia) non deve aspettare che il Consumer (chi elabora) sia online. Se il worker è spento per manutenzione, la coda accumula i messaggi e glieli dà appena si riaccende.
- Garanzia di Esecuzione (Il punto chiave di RabbitMQ): Se un worker sta processando un pagamento e il server salta per un blackout, il messaggio non deve andare perso.


Esempio SQS AWS:

1. L'ACK è "Automatico" (Success-based)
   Quando usi una Lambda come trigger di SQS (come in gpsMessagesConsumer), non c'è una chiamata esplicita ack().
   Successo: Se la funzione Lambda termina correttamente (senza lanciare eccezioni), AWS considera i messaggi come processati e li elimina automaticamente dalla coda.
   Fallimento: Se la Lambda lancia un errore (es. un crash, un timeout o un throw), AWS non elimina i messaggi. Questi tornano visibili nella coda dopo un periodo chiamato Visibility Timeout per essere riprocessati.
2. Se più client fanno "subscribe"?
   In SQS, se hai più "client" (istanze della Lambda), AWS gestisce la distribuzione in modo che un messaggio sia consegnato a un solo consumer alla volta.
   Quando una Lambda riceve un messaggio, SQS lo mette in stato "In Flight" e lo nasconde agli altri.
   Nessun altro client vedrà quel messaggio finché non scade il Visibility Timeout o finché la Lambda non fallisce.
   Quindi, non c'è rischio che due Lambda processino lo stesso identico messaggio contemporaneamente (a meno che il processing non duri più del timeout impostato).
3. Cosa succede in caso di crash?
   Se la Lambda crasha a metà dell'esecuzione:
   At-least-once delivery: SQS garantisce che il messaggio venga consegnato almeno una volta. Se la Lambda non conferma il successo (perché è crashata), il messaggio riappare nella coda dopo il timeout e verrà ri-assegnato a un'altra istanza.
   Dead Letter Queue (DLQ): Se un messaggio continua a far crashare la Lambda (es. dati malformati), dopo un numero massimo di tentativi (solitamente configurato nel CDK) viene spostato in una DLQ, una coda di "scarti" che può essere monitorata per il debug, evitando che un messaggio corrotto blocchi la coda all'infinito.
   In sintesi:
   Il sistema è progettato per essere resiliente: se un dato arriva ma il servizio muore prima di finire, il dato non va perso, ma viene ri-processato non appena il servizio (o una nuova istanza) torna disponibile. L'unica cosa a cui bisogna stare attenti è l'idempotenza (es. se crashi dopo aver scritto sul DB ma prima dell'ACK, al secondo giro riscriverai sul DB lo stesso dato).


Ecco cosa ha senso implementare in Nexo per replicare quel flusso:
1. Le Features: Cosa deve succedere?
- Safety (At-least-once delivery): Il messaggio non deve sparire quando viene letto. Deve sparire solo quando il worker dice "Ok, ho finito" (ACK).
- Visibility Timeout: Mentre un worker lavora, nessun altro deve vedere quel messaggio. Ma se il worker sparisce, il messaggio deve "resuscitare" dopo X secondi.
- Competing Consumers: Più worker possono chiedere messaggi contemporaneamente. Nexo deve distribuirli senza darli mai doppi.
- DLQ (Dead Letter Queue): Se un messaggio fallisce troppe volte (es. 5 crash), deve essere spostato in una "coda dei cattivi" per non bloccare il sistema all'infinito.


```rust
use uuid::Uuid;
use bytes::Bytes;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
   /// Identificativo unico del messaggio
   pub id: Uuid,

   /// Il contenuto informativo (payload)
   pub payload: Bytes,

   /// Livello di priorità (0-255). Default: 0
   pub priority: u8,

   /// Numero di tentativi di consegna effettuati (per logica DLQ)
   pub attempts: u32,

   /// Timestamp di creazione (Unix ms). Usato per calcolare il "Lag"
   pub created_at: u64,

   /// Timestamp in cui il messaggio tornerà visibile se non riceve ACK (Unix ms)
   /// Durante lo stato "In-Flight", questo valore è nel futuro.
   pub visible_at: u64,

   /// Timestamp opzionale per i messaggi posticipati (Delayed Jobs)
   pub delayed_until: Option<u64>,
}

impl Message {
   pub fn new(payload: Bytes, priority: u8, delay_ms: Option<u64>) -> Self {
      let now = std::time::SystemTime::now()
              .duration_since(std::time::UNIX_EPOCH)
              .unwrap()
              .as_millis() as u64;

      let delayed_until = delay_ms.map(|d| now + d);

      Self {
         id: Uuid::new_v4(),
         payload,
         priority,
         attempts: 0,
         created_at: now,
         visible_at: 0, // Inizialmente non è in-flight
         delayed_until,
      }
   }
}

struct QueueInner {
   // 1. DATI (Source of Truth)
   registry: HashMap<Uuid, Message>,

   // 2. STATO "READY" (u8 = priority)
   // Dove i messaggi aspettano il POP.
   ready: BTreeMap<u8, VecDeque<Uuid>>, //queue vera e propria dove producer PUSH e consumer POP

   // 3. STATO "DELAYED" (u64 = delay)
   // Messaggi inviati con "delay" o in attesa di retry (backoff).
   delayed: BTreeMap<u64, Uuid>,

   // 4. STATO "IN-FLIGHT" (u64 = Timeout)
   // Messaggi estratti ma in attesa di ACK.
   in_flight: BTreeMap<u64, Uuid>,

   // 5. SUBSCRIBE (Blocking Pops)
   // Una coda di "canali" (Sender) di client che stanno aspettando un messaggio.
   // Invece di fare polling, restano appesi qui.
   waiting_consumers: VecDeque<tokio::sync::oneshot::Sender<Message>>,
}


```

l Flusso del Dato (Sincronizzazione Totale)
Grazie al singolo Mutex che avvolge questa struct, ogni passaggio è una transazione atomica.

A. Il PUSH (Invio)
   Il dato entra nel registry.
   C'è un delay? Lo metto in delayed.
   Non c'è delay?
   Controllo waiting_consumers: c'è qualcuno in attesa?
   SÌ: Invio il messaggio direttamente al primo consumer nel canale. Lo sposto subito in in_flight.
   NO: Lo metto nel bucket ready della sua priorità.

B. Il POP / SUBSCRIBE (Ricezione)
   Coda non vuota: Estraggo dal bucket ready più alto. Calcolo il timeout, lo metto in in_flight. Restituisco il dato.
   Coda vuota: Creo un canale oneshot, metto il Sender in waiting_consumers e restituisco al client il Receiver. Il client farà .await senza consumare CPU.

C. L'ACK (Conferma)
   Uso l'ID per trovare il messaggio nel registry.
   Rimuovo l'ID da in_flight e dal registry. Fine.

D. IL REAPER (Il "Vigile" del tempo)
   Ogni secondo, il Reaper fa due scansioni rapidissime (grazie ai BTree):
   Scan delayed: Se il tempo è passato, sposta il messaggio in ready (o lo invia a un waiting_consumer).
   Scan in_flight: Se il timeout è scaduto:
   Aumenta attempts.
   attempts < 5? Lo rimette in ready (il messaggio "resuscita").
   attempts >= 5? Chiama il manager per spostarlo nella coda _dlq e lo cancella da qui.


Perché questa struttura non si "scorda" i pezzi? (Allineamento)
   Le strutture sono allineate perché l'ID del messaggio esiste in un solo posto "attivo" alla volta (oltre al registro):
   O è in delayed
   O è in ready
   O è in in_flight
   (O è nelle mani di un consumer)
   Il registry è l'unico che contiene sempre tutto finché il messaggio non viene confermato o perso.
   È flessibile per il futuro?
   Assolutamente. Vuoi aggiungere il Rate Limiting? Aggiungi un timestamp last_pop e una regola nel POP. Vuoi la Deduplicazione? Aggiungi una HashSet di hash(payload) nel QueueInner.


-------
- se un consumer si sottoscrive a una coda prima che arrivi un dato, poi quando arriva quel dato lu iverrà notificato, ma rimarrà comunque con un channel aperto su rust per il prox messaggio su quella queue? oppure viene elminiato? per come lo intendo io voglio che rimanga presente in rust tra i waiting_consumers, in pratica ogni client una volta che fa subscribe deve rimanere in ascolto per sempre
- se N consumer si sottoscrivono prima che arrivno un dato, adesso quel dato a quale consumer viene spedito?