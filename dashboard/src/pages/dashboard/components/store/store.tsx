import { useState, useMemo, useRef, useEffect } from "react"
import { useVirtualizer } from '@tanstack/react-virtual'
import { StoreBrokerSnapshot, KeyDetail } from "./types"
import { Input } from "@/components/ui/input"
import { 
    Database, 
    Search, 
    Clock, 
    List,
    Layers,
    FileJson,
    Binary
} from "lucide-react"

interface Props {
  data: StoreBrokerSnapshot
}

type StructureType = 'hashmap' | 'list' | 'set' 

export function StoreView({ data }: Props) {
  const [activeStructure, setActiveStructure] = useState<StructureType>('hashmap')

  return (
      <div className="flex h-full gap-0 border border-slate-800 rounded bg-slate-900/20 overflow-hidden font-mono text-sm">
          
          {/* SIDEBAR: Navigation */}
          <div className="w-48 flex flex-col border-r border-slate-800 bg-slate-950/50">
              <div className="p-3 border-b border-slate-800 text-[10px] font-bold text-slate-500 uppercase tracking-widest">
                  DATA_TYPES
              </div>
              <div className="p-2 space-y-0.5">
                  <NavButton 
                      label="MAP"
                      icon={<Database className="h-3.5 w-3.5" />} 
                      count={data.keys.length}
                      active={activeStructure === 'hashmap'}
                      onClick={() => setActiveStructure('hashmap')}
                  />
                  <NavButton 
                      label="LIST" 
                      icon={<List className="h-3.5 w-3.5" />} 
                      count={0}
                      active={activeStructure === 'list'}
                      onClick={() => setActiveStructure('list')}
                      disabled
                  />
                  <NavButton 
                      label="SET" 
                      icon={<Layers className="h-3.5 w-3.5" />} 
                      count={0}
                      active={activeStructure === 'set'}
                      onClick={() => setActiveStructure('set')}
                      disabled
                  />
              </div>
          </div>

          {/* MAIN AREA: Browser */}
          <div className="flex-1 min-w-0">
             <StoreBrowser data={data} structure={activeStructure} />
          </div>

      </div>
  )
}

function NavButton({ label, icon, count, active, onClick, disabled }: any) {
    return (
        <button
            onClick={onClick}
            disabled={disabled}
            className={`
                flex items-center justify-between w-full px-3 py-2 rounded-sm transition-all duration-200 group text-xs
                ${active 
                    ? 'bg-slate-800 text-white font-medium' 
                    : 'text-slate-400 hover:bg-slate-900 hover:text-slate-200'
                }
                ${disabled && 'opacity-40 cursor-not-allowed hover:bg-transparent'}
            `}
        >
            <div className="flex items-center gap-2.5">
                {icon}
                <span>{label}</span>
            </div>
            {count > 0 && (
                <span className={`text-[9px] px-1.5 py-0.5 rounded-sm ${active ? 'bg-slate-700 text-white' : 'bg-slate-900 text-slate-500'}`}>
                    {count}
                </span>
            )}
        </button>
    )
}

function StoreBrowser({ data, structure }: { data: StoreBrokerSnapshot, structure: StructureType }) {
  const [inputValue, setInputValue] = useState("")
  const [filter, setFilter] = useState("")
  const [selectedKey, setSelectedKey] = useState<KeyDetail | null>(null)
  const parentRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const timer = setTimeout(() => setFilter(inputValue), 300)
    return () => clearTimeout(timer)
  }, [inputValue])

  // Quick filter logic with early exit at 1000 results
  const FILTER_LIMIT = 1000
  const filteredKeys = useMemo(() => {
    const lowerFilter = filter.toLowerCase()
    const result: KeyDetail[] = []
    
    for (const k of data.keys) {
      if (k.key.toLowerCase().includes(lowerFilter)) {
        result.push(k)
        if (result.length >= FILTER_LIMIT) break
      }
    }
    
    return result
  }, [data.keys, filter])

  const rowVirtualizer = useVirtualizer({
    count: filteredKeys.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 33, // Approximate row height (px-4 py-2 text-xs border-b)
    overscan: 5,
  })

  useEffect(() => {
    if (!selectedKey && filteredKeys.length > 0) {
        setSelectedKey(filteredKeys[0])
    }
  }, [filteredKeys, selectedKey])

  return (
    <div className="flex h-full">
      {/* COLUMN 1: Key List */}
      <div className="w-[320px] flex flex-col border-r border-slate-800 bg-slate-900/10 flex-shrink-0">
        {/* Search Bar */}
        <div className="p-3 border-b border-slate-800">
            <div className="relative">
                <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-slate-500" />
                <Input 
                    placeholder={`FILTER ${structure.toUpperCase()}...`}
                    value={inputValue}
                    onChange={(e) => setInputValue(e.target.value)}
                    className="h-9 pl-8 bg-slate-950 border-slate-800 text-xs font-mono placeholder:text-slate-600 focus-visible:ring-1 focus-visible:ring-slate-700 focus-visible:border-slate-700"
                />
            </div>
        </div>

        {/* List Container */}
        <div ref={parentRef} className="flex-1 overflow-y-auto w-full contain-strict">
            {filteredKeys.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-12 text-center px-4">
                    <Search className="h-6 w-6 text-slate-800 mb-3" />
                    <p className="text-xs text-slate-600">NO_MATCHES</p>
                </div>
            ) : (
                <div
                    style={{
                        height: `${rowVirtualizer.getTotalSize()}px`,
                        width: '100%',
                        position: 'relative',
                    }}
                >
                    {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                        const k = filteredKeys[virtualRow.index]
                        return (
                            <button
                                key={k.key}
                                onClick={() => setSelectedKey(k)}
                                style={{
                                    position: 'absolute',
                                    top: 0,
                                    left: 0,
                                    width: '100%',
                                    height: `${virtualRow.size}px`,
                                    transform: `translateY(${virtualRow.start}px)`,
                                }}
                                className={`text-left px-4 py-2 text-xs border-b border-slate-800/50 transition-colors truncate font-mono ${
                                    selectedKey?.key === k.key 
                                    ? "bg-slate-800 text-white" 
                                    : "hover:bg-slate-900/50 text-slate-400 hover:text-slate-200"
                                }`}
                            >
                                {k.key}
                            </button>
                        )
                    })}
                </div>
            )}
        </div>
        
        {/* Footer Count */}
        <div className="p-2 border-t border-slate-800 text-[10px] text-slate-500 text-center uppercase">
            {filter === "" 
              ? `${data.keys.length} KEYS`
              : filteredKeys.length >= FILTER_LIMIT 
                ? `${FILTER_LIMIT}+ MATCHES / ${data.keys.length} KEYS`
                : `${filteredKeys.length} / ${data.keys.length} KEYS`
            }
        </div>
      </div>

      {/* COLUMN 2: Value Inspector */}
      <div className="flex-1 flex flex-col bg-slate-950/30 overflow-hidden">
        {selectedKey ? (
            <>
                <div className="p-4 border-b border-slate-800 flex justify-between items-center bg-slate-900/20">
                    <div className="flex items-center gap-2">
                        <h2 className="text-sm font-bold text-slate-100">{selectedKey.key}</h2>
                    </div>
                    <div className="flex items-center gap-4 text-[10px] text-slate-500 uppercase tracking-wide">
                        <span className="flex items-center gap-1.5">
                            <Clock className="h-3 w-3" /> 
                            {new Date(selectedKey.expires_at).toLocaleString()}
                        </span>
                    </div>
                </div>

                <div className="flex-1 overflow-auto">
                    <div className="p-6 min-w-0">
                        <pre className="text-xs text-slate-300 font-mono leading-relaxed whitespace-pre min-w-0">
                            {tryFormatJson(selectedKey.value)}
                        </pre>
                    </div>
                </div>
                
                 {/* Footer Metadata */}
                <div className="px-4 py-2 border-t border-slate-800 bg-slate-900/20 text-[10px] text-slate-500 flex justify-between font-mono uppercase">
                    <div className="flex items-center gap-2">
                        <Binary className="h-3 w-3" />
                        <span>SIZE: {new Blob([selectedKey.value]).size} BYTES</span>
                    </div>
                    <div className="flex items-center gap-2">
                        <FileJson className="h-3 w-3" />
                        <span>{selectedKey.value.startsWith('0x') ? 'BINARY DATA' : 'UTF-8 TEXT'}</span>
                    </div>
                </div>
            </>
        ) : (
            <div className="flex-1 flex flex-col items-center justify-center text-slate-700">
                <Database className="h-12 w-12 opacity-20 mb-4" />
                <p className="text-xs font-mono uppercase tracking-widest opacity-50">NO_KEY_SELECTED</p>
            </div>
        )}
      </div>
    </div>
  )
}

function tryFormatJson(str: string) {
    try {
        if (str.startsWith("{") || str.startsWith("[")) {
            return JSON.stringify(JSON.parse(str), null, 2)
        }
    } catch (e) {}
    return str
}
