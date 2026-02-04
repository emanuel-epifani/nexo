# Copia e incolla questo nel terminale mentre il server gira
while true; do
    # Cerca il PID del processo 'nexo' più recente
    PID=$(pgrep -n nexo)

    if [ -z "$PID" ]; then
        echo "Nexo non è in esecuzione..."
    else
        # Estrae RSS (in KB), converte in MB e stampa
        RAM_KB=$(ps -o rss= -p $PID)
        RAM_MB=$(echo "scale=2; $RAM_KB / 1024" | bc)
        echo "$(date '+%H:%M:%S') -> PID: $PID | RAM Reale (RSS): ${RAM_MB} MB"
    fi
    sleep 3
done