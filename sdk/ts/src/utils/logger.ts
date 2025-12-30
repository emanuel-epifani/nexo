
enum LogLevel {
    DEBUG = 0,
    INFO = 1,
    WARN = 2,
    ERROR = 3,
    NONE = 4
}

class Logger {
    private readonly level: LogLevel;
    private prefix: string = '[SDK]';

    constructor() {
        // Legge la variabile d'ambiente NEXO_LOG (come Rust) o LOG_LEVEL
        const envLevel = process.env.NEXO_LOG?.toUpperCase() || process.env.LOG_LEVEL?.toUpperCase() || 'ERROR';
        this.level = this.parseLevel(envLevel);
    }

    private parseLevel(lvl: string): LogLevel {
        switch (lvl) {
            case 'TRACE': // Mappiamo trace su debug per semplicità
            case 'DEBUG': return LogLevel.DEBUG;
            case 'INFO': return LogLevel.INFO;
            case 'WARN': return LogLevel.WARN;
            case 'ERROR': return LogLevel.ERROR;
            case 'OFF': return LogLevel.NONE;
            default: return LogLevel.ERROR; // Default sicuro (silenzioso)
        }
    }

    debug(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.DEBUG) {
            // Ciano per debug
            console.debug(`\x1b[36m${this.prefix} [DEBUG]\x1b[0m ${msg}`, ...args);
        }
    }

    info(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.INFO) {
            // Verde per info
            console.log(`\x1b[32m${this.prefix} [INFO]\x1b[0m ${msg}`, ...args);
        }
    }

    warn(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.WARN) {
            // Giallo per warn
            console.warn(`\x1b[33m${this.prefix} [WARN]\x1b[0m ${msg}`, ...args);
        }
    }

    error(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.ERROR) {
            // Rosso per error
            console.error(`\x1b[31m${this.prefix} [ERROR]\x1b[0m ${msg}`, ...args);
        }
    }
}

// Singleton instance per uso interno
export const logger = new Logger();

