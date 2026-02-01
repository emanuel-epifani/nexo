export type LogHandler = (level: string, message: string, ...args: any[]) => void;

export enum LogLevel {
    TRACE = 0,
    DEBUG = 1,
    INFO = 2,
    WARN = 3,
    ERROR = 4,
    NONE = 5
}

export class Logger {
    private readonly level: LogLevel;
    private readonly handler: LogHandler;

    constructor(options?: { handler?: LogHandler, level?: string }) {
        const envLevel = options?.level || process.env.NEXO_LOG?.toUpperCase() || 'ERROR';
        this.level = this.parseLevel(envLevel);
        this.handler = options?.handler || this.defaultHandler;
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

    private defaultHandler(level: string, msg: string, ...args: any[]) {
        const timestamp = new Date().toISOString();
        const prefix = `[SDK] [${timestamp}] ${level}`;

        switch (level) {
            case 'ERROR': console.error(prefix, msg, ...args); break;
            case 'WARN': console.warn(prefix, msg, ...args); break;
            default: console.log(prefix, msg, ...args); break;
        }
    }

    trace(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.TRACE) {
            this.handler('TRACE', msg, ...args);
        }
    }

    debug(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.DEBUG) {
            this.handler('DEBUG', msg, ...args);
        }
    }

    info(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.INFO) {
            this.handler('INFO', msg, ...args);
        }
    }

    warn(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.WARN) {
            this.handler('WARN', msg, ...args);
        }
    }

    error(msg: string, ...args: any[]) {
        if (this.level <= LogLevel.ERROR) {
            this.handler('ERROR', msg, ...args);
        }
    }
}
