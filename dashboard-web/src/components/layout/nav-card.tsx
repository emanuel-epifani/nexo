import { RefreshCw } from 'lucide-react'

export function NavCard({ label, desc, active, onClick, icon, onRefresh, isRefreshing }: any) {
    return (
        <div
            onClick={onClick}
            className={`
                group relative flex items-center h-20 w-full rounded-sm border transition-all duration-200 cursor-pointer overflow-hidden
                ${active
                ? 'bg-slate-800 border-slate-600 shadow-md'
                : 'bg-slate-900/40 border-slate-800 hover:bg-slate-800/40 hover:border-slate-700'
            }
            `}
        >
            {/* CONTENT AREA */}
            <div className="flex flex-1 items-center gap-4 px-6 h-full">
                <div className={`
                    p-2.5 rounded-full transition-colors shrink-0
                    ${active ? 'bg-indigo-500/10 text-indigo-400' : 'bg-slate-900 text-slate-600 group-hover:text-slate-500'}
                `}>
                    {icon}
                </div>

                <div className="flex flex-col items-start text-left min-w-0">
                    <span className={`text-base font-mono font-bold tracking-tight leading-tight ${active ? 'text-white' : 'text-slate-400'}`}>
                        {label}
                    </span>
                    <span className="text-[10px] font-mono text-slate-600 uppercase tracking-wider truncate">
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
                    h-full w-12 flex items-center justify-center border-l transition-colors outline-none
                    ${active
                    ? 'border-slate-700 hover:bg-slate-700 text-slate-400 hover:text-white'
                    : 'border-slate-800 hover:bg-slate-800 text-slate-600 hover:text-slate-400'
                }
                `}
            >
                <RefreshCw className={`h-4 w-4 ${isRefreshing ? 'animate-spin' : ''}`} />
            </button>

            {/* Active Indicator */}
            {active && (
                <div className="absolute bottom-0 left-0 right-0 h-[2px] bg-indigo-500" />
            )}
        </div>
    )
}
