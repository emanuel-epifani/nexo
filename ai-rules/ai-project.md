NEXO - all in one broker high performance for scale up

partiamo col dire che la priorità assoluta di questo progetto è fornire un broker all in one che magari non scalerà a livelli di netflix, ma sia "enough" per il 90% dei progetto che si presentano nella vita di tutti i giorni lato backend ai comuni mortali/aziende di consulenza ecc (un pò come nodejs, non ottimo per niente, ma enough per quasi tutto, e  infatti proprio per questo voglio fare l'sdk per node, primo perchè lo usano in scaleup che secondo me è il mio target, secondo perchè è abbastanza lento da poter giovare di una cosa del genere come il mio broker).
nexo nasce per non essere distribuito su più nodi, quindi tutto in un unica istanza che deve sfruttare l'hardware fino all'ultimo nanosecondo per "posticipare" quanto più il momento in cui "serve qualcos'altro". se sai già che nel best scenario scalerai stile netflix probabilmente non prenderai nexo, ma se sai già che il worst scenario di carico è per esempio 10000device iot, conosci già quanto ti serve (e fin li secondo me un backend serio in rust fa miracoli), e se ti absta nexo parti direttamente con quello che può fare tutto.
per ottimizzare fin da subito questa vision estrema ho puntato ovunque potessi a maggior throughput (multithread ovunque) rispetto alla minor latenza.
ulteriore priorità assoluta (forse ancora più delle performance) deve essere la developer experience di chi usa questo sdk. 1 broker che fa tutto "super comodo ma comunque abbastanza performante", con una sdk super super intuitivo e flessibile. questo è il motivo per cui nasce nexo.
e infatti la mia idea è 1 broker qualunque funzionalità con 1 sola configutazione:

- STORE → cache in memory stile redis con n strutture dati (x ora c’è solo MAP x in futuro vedremo)
- QUEUE → message queues per work/job con persistenza
- PUBSUB → Pub/Sub realtime with hierarchical topics stile mqtt realtime fanin fanout (deafult Volatile, ma persistenza per retained value)
- STREAM → Append-only event-logs stile kafka con persistenza (Persistente, Log, Replay)
- DASHBOARD FE -> esposta automaticamente su una porta quando avvii il container docker che fa una request ed ogni broker ritorna una snaphsto a quell'istante dello stato di tutti i broker (lo store, le queue, topics ecc) super comodo per debuggare in locale mentre sviluppi (non da usare in produzione come metriche) anche senza dover installare mille tool diversi stile datagrip, plugin vscode o altro ecc. scarichi il container, 1 sdk  e hai tutto, cache, queue, mqtt, topic, e una dashboard web per vedere stato dati esposto automaticamente.


SERVER RUST (Data consistency/Robustness > Throughput > Latency)
- tutta la logica di connessioni/socket/parsing ecc dentro -> src/server
- tutta la logica di business dentro -> src/brokers (ogni broker deve avere logica di business quanto più isolato all'interno di se stesso)
- ogni modulo deve avere un mod.rs che fa solo da exporter. esporta solo il necessario dal modulo (se facade pattern ci sarà una struct finale con cui interagire e metodi/struct dell'implementazioen interna non esportati)
- ogni broker dovrebeb esser testato in tutte le sue features in un file dentro tests/*, (ogni broker 1 file, tipi diversi di feature raggrupapti in mod nidificati)
- tutti i test dei broker dovrebbero interagrie solo con il corrispettivo manager, cosia da poter testare il flusso quanto più e2e possibile, e poter piegare il runtime del flow in base alle config passate al manager


SDK TYPESCRIPT (devexperience over all)
- max develoepr experience (senza leggere la docs deve essere super intuitivo capire come si usa e cosa può fare)
- facade con un paio di classi di export (1 per tipo di broker), e tutta la logica di socket/parse ecc centralizzata nella send o pochi altri metodi estranei alla logica di business (all'interno dei quali poter fare ottimizzazioni estreme anche poco comprensibili)
- quando non compromette performance, prediligi codice pulito (switch anzichè molti if else if ecc)

TEST VITEST (business logic first)
- ogni file di test deve contenere solo logia di business, tutto cio da fare prima e dopo ogni file configurato in global-setup (in pratica nel beforeaall x ogni file build binario in --release mode, run server, e alla fine di ogni tet kill server per pulizia massima) quindi basta runnare il singolo file di test che automaticamente fa la builde runna prima il server, non runnare il server prima con comandi indipendenti
- la connessione di nexo viene già istanziata in singleton in nexo.ts, dopodichè riusiamo quel'istanza per testare i broker per avere i fiel di test super puliti, a meno che non serva un secondo client per logiche di test, non istanziare un nuvoo client dentro i file di test
- i test dei broker dovrebbero seguire questo patter, 1 macro describe per runnare tutto, e poi N describe dentro (uno x ogni feature), e n it() all'interno di ogni describe per testare ogni angolo diverso della stessa fetures. l'ordine dovrebbe essere: tutte le features ovvie con test semplici/happy path, edge case improbabili, edge case possibili dovuti a race condition con multithread ecc, edge case possibili dovuti a connection/reconnct strane. no test di performance, quelli in un altro file (tutti i performance raggruppati per describe che riflette il broker)
- i nomi dei test describe/it dovrebbero essere molto descrittivi e chiarire esattamente cosa stanno testando (raggruppati per macrofunzionalità, con parole maisucolo che fanno subito capire la key features testate)
- a meno che connect e disconnect non facciano parte della logica di dusines necessaria a testare la features (es reconnect automatica ecc) evita disconnect dentro i test e riutiliza la stessa istanza di nexo già configurato in nexo.ts

APPROCCIO ALLO SVILUPPO
- facciamo brainstorming su strutture dati/design pattern da poter seguire per esppletare tutte le features

GENERAL
- non installare mai dipendenze sensa avermi prima consultati e aver valutato assieme (anzi, laddove possibiel toglierne valutiamo se fattibile. less is more)
- non fare assunzioni, se qualche pezzo dell'implementazione ha più possibilità e non ho pensato a tutte, non supporre, fermami e chiedi esplicitamente come gestire quella situazione nel codice
- se facciamo refactor di alcune logiche, non lasciare mai refusi (ne codice ne commenti) sulla vecchia implementazione
- mai usare magic number/value hardcoded nel codice, ma mettili sempre sotto variabili/costanti parlanti e modificabili
- non lasciare refusi di codice/commenti/todo ecc di vecchie logiche/retrocompatibilità. se stiamo facendo un refactor il codice che rimane deve esser unicamente quello che rispecchia la logica attuale
- qualunque test dovrebbe esser equanto più deterministico e non probabilistico (non sleep/timeout random a caso, se quelal funzione avviene ogni interval_function_x, aspettiamo esattametne interval_function_x + qualcosa per avere test dinamici)
- i test devono esser pensati per essere indipendenti tra loro per esser atomici e runnabili in maniera indipendnete, e non dovrebbero creare problemi di refusi a fine test
- i test devono coprire TUTTE le features implementate, nel modo quanto più diretto possibile (non sempre è possibile, ma è il goal)


parla in italiano nella chat
scrivi sempre codice e commenti in inglese (i commenti dovrebbero essere uno snapshot del codice in quel preciso momento, non dovrebbero avere riferimento a spiegazione storica del tipo "non usiamo più la hasmpia qui <--, removed costant, changed function, not yet implemented ecc..")


