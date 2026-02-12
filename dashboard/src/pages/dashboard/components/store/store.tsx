import { useState, useEffect } from "react"
import { useQuery } from '@tanstack/react-query'
import { StoreBrokerSnapshot, KeyDetail } from "./types"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { 
    Database, 
    Search, 
    Clock, 
    List,
    Layers,
    FileJson,
    Binary,
    Loader2,
    AlertCircle,
    ChevronLeft,
    ChevronRight
} from "lucide-react"

type StructureType = 'hashmap' | 'list' | 'set' 

export function StoreView() {
  const [activeStructure, setActiveStructure] = useState<StructureType>('hashmap')
  const [inputValue, setInputValue] = useState("")
  const [debouncedSearch, setDebouncedSearch] = useState("")
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const [limit] = useState(500)
  const [offset, setOffset] = useState(0)
  const [selectedKey, setSelectedKey] = useState<KeyDetail | null>(null)

  // Reset offset when search changes
  useEffect(() => {
    setOffset(0)
  }, [debouncedSearch])
  
  // Debounce search input
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedSearch(inputValue), 300)
    return () => clearTimeout(timer)
  }, [inputValue])

  const { data, isLoading, error, refetch, isRefetching } = useQuery({
    queryKey: ['store-snapshot', debouncedSearch, limit, offset],
    queryFn: async () => {
      const params = new URLSearchParams()
      if (debouncedSearch) params.append('search', debouncedSearch)
      if (limit) params.append('limit', limit.toString())
      if (offset) params.append('offset', offset.toString())
      
      const response = await fetch(`/api/store?${params.toString()}`)
      if (!response.ok) throw new Error('Failed to fetch store')
      return response.json() as Promise<StoreBrokerSnapshot>
    }
  })

  // Reset selection if list changes and current selection is gone
  useEffect(() => {
      if (selectedKey && data?.keys && !data.keys.find(k => k.key === selectedKey.key)) {
          setSelectedKey(null)
      }
  }, [data?.keys, selectedKey])

  if (error) {
    return (
        <div className="flex h-full flex-col items-center justify-center gap-4 text-destructive p-8 border-2 border-destructive/20 rounded-lg bg-destructive/5 m-4">
            <AlertCircle className="h-12 w-12" />
            <div className="text-center">
                <h3 className="font-bold">ERROR_LOADING_STORE</h3>
                <p className="text-xs opacity-70 mt-1">{(error as Error).message}</p>
            </div>
            <Button variant="outline" size="sm" onClick={() => refetch()}>
                TRY_AGAIN
            </Button>
        </div>
    )
  }

  const startRange = offset + 1
  const endRange = Math.min(offset + limit, data?.total || 0)
  const hasNext = offset + limit < (data?.total || 0)
  const hasPrev = offset > 0

  return (
      <div className="flex h-full gap-0 border-2 border-border rounded-sm bg-panel overflow-hidden font-mono text-sm">
          
          {/* SIDEBAR: Navigation */}
          <div className="w-48 flex flex-col border-r-2 border-border bg-sidebar">
              <div className="p-3 border-b-2 border-border text-xs font-bold text-muted-foreground uppercase tracking-widest flex justify-between items-center">
                  <span>DATA_TYPES</span>
                  {isLoading || isRefetching ? (
                      <Loader2 className="h-3 w-3 animate-spin text-primary" />
                  ) : (
                      <div className="h-3 w-3 rounded-full bg-green-500/50" />
                  )}
              </div>
              <div className="p-2 space-y-0.5">
                  <NavButton 
                      label="MAP"
                      icon={<Database className="h-3.5 w-3.5" />} 
                      count={data?.total || 0}
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
          <div className="flex-1 min-w-0 relative flex h-full">
             {isLoading && !data ? (
                 <div className="absolute inset-0 flex items-center justify-center bg-background/50 z-10 backdrop-blur-[1px]">
                     <div className="flex flex-col items-center gap-2">
                        <Loader2 className="h-8 w-8 animate-spin text-primary" />
                        <span className="text-xs text-muted-foreground">LOADING_DATA...</span>
                     </div>
                 </div>
             ) : null}
             
             {/* COLUMN 1: Key List */}
             <div className="w-[320px] flex flex-col border-r-2 border-border bg-sidebar flex-shrink-0">
               {/* Search Bar */}
               <div className="p-3 border-b-2 border-border space-y-2 bg-background/50">
                   <div className="relative">
                       <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                       <Input 
                           placeholder={`SERVER SEARCH (${activeStructure.toUpperCase()})...`}
                           value={inputValue}
                           onChange={(e) => setInputValue(e.target.value)}
                           className="h-9 pl-8 bg-background border-border text-xs font-mono placeholder:text-muted-foreground shadow-none focus-visible:ring-0 focus-visible:border-primary"
                       />
                       {(debouncedSearch !== inputValue || isRefetching) && (
                           <div className="absolute right-2.5 top-2.5">
                               <Loader2 className="h-3.5 w-3.5 animate-spin text-primary" />
                           </div>
                       )}
                   </div>
                   <div className="flex justify-between text-[10px] text-muted-foreground px-1 uppercase font-semibold">
                       <span>Page {Math.floor(offset / limit) + 1} of {Math.ceil((data?.total || 0) / limit)}</span>
                       <span>Showing {data?.total ? startRange : 0}-{endRange} of {data?.total || 0}</span>
                   </div>
               </div>
       
               {/* List Container */}
               <div className="flex-1 overflow-y-auto w-full contain-strict custom-scrollbar">
                   {!data || data.keys.length === 0 ? (
                       <div className="flex flex-col items-center justify-center py-12 text-center px-4">
                           <Search className="h-6 w-6 text-muted-foreground mb-3" />
                           <p className="text-xs text-muted-foreground">NO_RESULTS_FOUND</p>
                       </div>
                   ) : (
                       <div className="flex flex-col w-full">
                           {data.keys.map((k) => (
                               <button
                                   key={k.key}
                                   onClick={() => setSelectedKey(k)}
                                   className={`text-left px-4 py-2 text-xs border-b border-border/60 transition-colors truncate font-mono h-[33px] flex items-center ${
                                       selectedKey?.key === k.key 
                                       ? "bg-secondary text-foreground font-medium border-l-2 border-l-primary pl-[14px]" 
                                       : "hover:bg-muted/50 text-muted-foreground hover:text-foreground"
                                   }`}
                               >
                                   {k.key}
                               </button>
                           ))}
                       </div>
                   )}
               </div>
               
               {/* Footer Pagination */}
               <div className="p-2 border-t border-border bg-background/50 flex flex-col gap-2">
                   <div className="flex gap-1">
                       <Button 
                           variant="outline" 
                           size="sm" 
                           className="h-7 flex-1 text-xs gap-1" 
                           disabled={!hasPrev || isRefetching}
                           onClick={() => setOffset(Math.max(0, offset - limit))}
                       >
                           <ChevronLeft className="h-3 w-3" /> PREV
                       </Button>
                       <Button 
                           variant="outline" 
                           size="sm" 
                           className="h-7 flex-1 text-xs gap-1" 
                           disabled={!hasNext || isRefetching}
                           onClick={() => setOffset(offset + limit)}
                       >
                           NEXT <ChevronRight className="h-3 w-3" />
                       </Button>
                   </div>
               </div>
             </div>
       
             {/* COLUMN 2: Value Inspector */}
             <div className="flex-1 flex flex-col bg-content overflow-hidden">
               {selectedKey ? (
                   <>
                       <div className="p-4 border-b border-border flex justify-between items-center bg-section-header">
                           <div className="flex items-center gap-2">
                               <h2 className="text-sm font-bold text-foreground">{selectedKey.key}</h2>
                           </div>
                           <div className="flex items-center gap-4 text-xs text-muted-foreground uppercase tracking-wide">
                               <span className="flex items-center gap-1.5">
                                   <Clock className="h-3 w-3" /> 
                                   expire at: {new Date(selectedKey.exp_at).toLocaleString()}
                               </span>
                           </div>
                       </div>
       
                       <div className="flex-1 overflow-auto custom-scrollbar">
                           <div className="p-6 min-w-0">
                               <pre className="text-xs text-foreground font-mono leading-relaxed whitespace-pre min-w-0">
                                   {typeof selectedKey.value === 'string' ? selectedKey.value : JSON.stringify(selectedKey.value, null, 2)}
                               </pre>
                           </div>
                       </div>
                       
                        {/* Footer Metadata */}
                       <div className="px-4 py-2 border-t border-border bg-section-header text-xs text-muted-foreground flex justify-between font-mono uppercase">
                           <div className="flex items-center gap-2">
                               <Binary className="h-3 w-3" />
                               <span>SIZE: {new Blob([typeof selectedKey.value === 'string' ? selectedKey.value : JSON.stringify(selectedKey.value)]).size} BYTES</span>
                           </div>
                           <div className="flex items-center gap-2">
                               <FileJson className="h-3 w-3" />
                               <span>{typeof selectedKey.value === 'string' && selectedKey.value.startsWith('0x') ? 'BINARY DATA' : 'UTF-8 TEXT'}</span>
                           </div>
                       </div>
                   </>
               ) : (
                   <div className="flex-1 flex flex-col items-center justify-center text-muted-foreground/50">
                       <Database className="h-12 w-12 opacity-20 mb-4" />
                       <p className="text-xs font-mono uppercase tracking-widest opacity-50">NO_KEY_SELECTED</p>
                   </div>
               )}
             </div>
          </div>
      </div>
  )
}

interface NavButtonProps {
  label: string
  icon: React.ReactNode
  count: number
  active: boolean
  onClick: () => void
  disabled?: boolean
}

function NavButton({ label, icon, count, active, onClick, disabled }: NavButtonProps) {
    return (
        <button
            onClick={onClick}
            disabled={disabled}
            className={`
                flex items-center justify-between w-full px-3 py-2 rounded-sm transition-all duration-200 group text-xs
                ${active 
                    ? 'bg-secondary text-foreground font-medium' 
                    : 'text-muted-foreground hover:bg-muted hover:text-foreground'
                }
                ${disabled && 'opacity-40 cursor-not-allowed hover:bg-transparent'}
            `}
        >
            <div className="flex items-center gap-2.5">
                {icon}
                <span>{label}</span>
            </div>
            {count > 0 && (
                <span className={`text-xs px-1.5 py-0.5 rounded-sm ${active ? 'bg-primary/20 text-foreground' : 'bg-muted text-muted-foreground'}`}>
                    {count}
                </span>
            )}
        </button>
    )
}
