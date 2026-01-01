
enum LogLevel {
    TRACE = 0,
    DEBUG = 1,
    INFO = 2,
    WARN = 3,
    ERROR = 4,
    NONE = 5
}

class Logger {
    private readonly level: LogLevel;

    constructor() {
        const envLevel = process.env.NEXO_LOG?.toUpperCase() || process.env.LOG_LEVEL?.toUpperCase() || 'ERROR';
        this.level = this.parseLevel(envLevel);
    }

    private parseLevel(lvl: string): LogLevel {
        switch (lvl) {
            case 'TRACE': return LogLevel.TRACE;
            case 'DEBUG': return LogLevel.DEBUG;
            case 'INFO': return LogLevel.INFO;
            case 'WARN': return LogLevel.WARN;
            case 'ERROR': return LogLevel.ERROR;
            case 'OFF': return LogLevel.NONE;
            default: return LogLevel.ERROR;
        }
    }

    trace(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.TRACE) {
            // Magenta per trace (simile a viola)
            console.debug(`\x1b[35mTRACE\x1b[0m [SDK] ${msg}`, ...args);
        }
    }

    debug(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.DEBUG) {
            // Ciano per examples
            console.debug(`\x1b[36mDEBUG\x1b[0m [SDK] ${msg}`, ...args);
        }
    }

    info(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.INFO) {
            // Verde per info
            console.log(`\x1b[32m INFO\x1b[0m [SDK] ${msg}`, ...args);
        }
    }

    warn(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.WARN) {
            // Giallo per warn
            console.warn(`\x1b[33m WARN\x1b[0m [SDK] ${msg}`, ...args);
        }
    }

    error(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.ERROR) {
            // Rosso per error
            console.error(`\x1b[31mERROR\x1b[0m [SDK] ${msg}`, ...args);
        }
    }
}

export const logger = new Logger();
