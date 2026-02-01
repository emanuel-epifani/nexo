import { RefreshCw } from 'lucide-react'

export function NavCard({ label, desc, active, onClick, icon, onRefresh, isRefreshing }: any) {
    return (
        <div
            onClick={onClick}
            className={`
                group relative flex items-center h-20 w-full rounded-sm transition-all duration-200 cursor-pointer overflow-hidden
                ${active
                ? 'bg-card border-2 border-primary shadow-lg ring-1 ring-primary/20'
                : 'bg-card/80 border-2 border-border/60 hover:bg-card hover:border-primary/50'
            }
            `}
        >
            {/* CONTENT AREA */}
            <div className="flex flex-1 items-center gap-4 px-6 h-full">
                <div className={`
                    p-2.5 rounded-full transition-colors shrink-0
                    ${active ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground group-hover:text-foreground'}
                `}>
                    {icon}
                </div>

                <div className="flex flex-col items-start text-left min-w-0">
                    <span className={`text-base font-mono font-bold tracking-tight leading-tight ${active ? 'text-foreground' : 'text-muted-foreground'}`}>
                        {label}
                    </span>
                    <span className="text-[10px] font-mono text-muted-foreground uppercase tracking-wider truncate">
                        {desc}
                    </span>
                </div>
            </div>

            {/* REFRESH STRIP (Vertical button) */}
            <button
                onClick={(e) => {
                    e.stopPropagation()
                    onRefresh()
                }}
                disabled={isRefreshing}
                className={`
                    h-full w-12 flex items-center justify-center border-l-2 transition-colors outline-none
                    ${active
                    ? 'border-primary hover:bg-muted text-muted-foreground hover:text-foreground '
                    : 'border-border/50 hover:bg-muted text-muted-foreground hover:text-foreground'
                }
                `}
            >
                <RefreshCw className={`h-4 w-4 ${isRefreshing ? 'animate-spin' : ''}`} />
            </button>

        </div>
    )
}
