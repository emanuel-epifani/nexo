import { useState, useMemo, useEffect } from "react"
import { useQuery } from "@tanstack/react-query"
import { QueueBrokerSnapshot, QueueSummary, MessageSummary, ScheduledMessageSummary, DlqMessageSummary, PaginatedMessages, PaginatedDlqMessages } from "./types"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { 
    Search, 
    MessageSquare,
    RefreshCw,
    AlertCircle,
    Box,
    AlertTriangle,
    Copy,
    Check,
    ChevronLeft,
    ChevronRight,
} from "lucide-react"
import { Badge } from "@/components/ui/badge"

interface Props {
  data: QueueBrokerSnapshot
}

const PAGE_SIZE = 50

export function QueueView({ data }: Props) {
  const [filter, setFilter] = useState("")
  const [selectedQueueName, setSelectedQueueName] = useState<string | null>(null)
  
  // View Mode: 'traffic' (Active) or 'dlq' (Dead Letter)
  const [viewMode, setViewMode] = useState<'traffic' | 'dlq'>('traffic')

  const [selectedMessageId, setSelectedMessageId] = useState<string | null>(null)
  const [copiedId, setCopiedId] = useState<string | null>(null)
  
  // Message State Filter (for Traffic view)
  const [messageState, setMessageState] = useState<'Pending' | 'InFlight' | 'Scheduled'>('Pending')
  
  // Pagination State
  const [offset, setOffset] = useState(0)
  
  // Reset pagination when changing context
  useEffect(() => {
      setOffset(0)
      setSelectedMessageId(null)
  }, [selectedQueueName, viewMode, messageState])

  const filteredQueues = useMemo(() => {
      return (data || []).filter(q => q.name.toLowerCase().includes(filter.toLowerCase()))
  }, [data, filter])

  const selectedQueue = useMemo(() => {
      return (data || []).find((q: QueueSummary) => q.name === selectedQueueName)
  }, [data, selectedQueueName])

  // Fetch paginated messages
  const { data: paginatedData, isLoading } = useQuery({
    queryKey: ['queue-messages', selectedQueueName, viewMode, messageState, offset],
    queryFn: async () => {
        if (!selectedQueueName) return null
        
        const stateParam = viewMode === 'dlq' ? 'dlq' : messageState.toLowerCase()
        const res = await fetch(`/api/queue/${selectedQueueName}/messages?state=${stateParam}&offset=${offset}&limit=${PAGE_SIZE}`)
        
        if (!res.ok) throw new Error('Failed to fetch messages')
        
        if (viewMode === 'dlq') {
            return await res.json() as PaginatedDlqMessages
        } else {
            return await res.json() as PaginatedMessages
        }
    },
    enabled: !!selectedQueueName,
  })

  // Get selected message details
  const selectedMessage = useMemo(() => {
      if (!paginatedData || !selectedMessageId) return undefined
      return paginatedData.messages.find(m => m.id === selectedMessageId)
  }, [paginatedData, selectedMessageId])

  // Copy to clipboard helper
  const copyToClipboard = (text: string, id: string) => {
      navigator.clipboard.writeText(text)
      setCopiedId(id)
      setTimeout(() => setCopiedId(null), 2000)
  }

  const messages = paginatedData?.messages || []
  const total = paginatedData?.total || 0
  const hasMore = offset + PAGE_SIZE < total

  return (
      <div className="flex h-full gap-0 border-2 border-border rounded-sm bg-panel overflow-hidden font-mono text-sm">
          
          {/* SIDEBAR: Queues List */}
          <div className="w-[320px] flex flex-col border-r-2 border-border bg-sidebar">
              {/* HEADER */}
              <div className="p-3 border-b-2 border-border bg-sidebar">
                  <h2 className="text-xs font-bold uppercase text-muted-foreground mb-2">QUEUES ({filteredQueues.length})</h2>
                  <div className="relative">
                      <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                      <Input 
                          placeholder="FILTER..."
                          value={filter}
                          onChange={(e) => setFilter(e.target.value)}
                          className="h-9 pl-8 bg-background border-border text-xs font-mono placeholder:text-muted-foreground focus-visible:ring-1 focus-visible:ring-ring"
                      />
                  </div>
              </div>

              {/* LIST */}
              <ScrollArea className="flex-1">
                  <div className="p-0">
                      {filteredQueues.length === 0 ? (
                          <div className="py-8 text-center text-xs text-muted-foreground italic">
                              NO_QUEUES_FOUND
                          </div>
                      ) : (
                          filteredQueues.map((q) => {
                              const hasDlq = q.dlq > 0
                              const totalPending = q.pending
                              
                              return (
                                  <div
                                      key={q.name}
                                      onClick={() => setSelectedQueueName(q.name)}
                                      className={`
                                          group flex items-center justify-between px-4 py-3 border-b border-border/50 cursor-pointer transition-all
                                          ${selectedQueueName === q.name ? 'bg-secondary' : 'hover:bg-muted/50'}
                                      `}
                                  >
                                      <div className="flex items-center gap-3 overflow-hidden">
                                          <div className={`p-1.5 rounded relative ${
                                              selectedQueueName === q.name 
                                                ? 'bg-primary/20 text-foreground'
                                                : 'bg-muted text-muted-foreground'
                                          }`}>
                                              <MessageSquare className="h-3.5 w-3.5" />
                                              {hasDlq && (
                                                  <div className="absolute -top-1 -right-1 h-2.5 w-2.5 bg-destructive rounded-full border border-sidebar animate-pulse" />
                                              )}
                                          </div>
                                          <span className={`font-mono text-xs truncate ${selectedQueueName === q.name ? 'text-foreground font-bold' : 'text-muted-foreground'}`}>
                                              {q.name}
                                          </span>
                                      </div>

                                      <div className="flex items-center gap-2">
                                          {hasDlq && (
                                              <div className="flex items-center gap-1 text-xs text-destructive font-bold bg-destructive/10 px-1.5 py-0.5 rounded">
                                                  <AlertTriangle className="h-3 w-3" />
                                                  {q.dlq}
                                              </div>
                                          )}
                                          {totalPending > 0 && (
                                              <Badge variant="outline" className="h-4 px-1.5 text-xs rounded-sm border border-status-pending bg-status-pending/10 text-status-pending">
                                                  {totalPending}
                                              </Badge>
                                          )}
                                      </div>
                                  </div>
                              )
                          })
                      )}
                  </div>
              </ScrollArea>
          </div>

          {/* CENTER AREA: Messages Table */}
          <div className="w-[550px] bg-content flex flex-col min-w-0 border-r-2 border-border">
             {selectedQueue ? (
                 <div className="flex flex-col h-full">
                     {/* Header Stats & Tabs */}
                     <div className="p-3 border-b-2 border-border bg-section-header">
                         <div className="flex justify-between items-center mb-3">
                             <h2 className="text-xs font-bold text-foreground">{selectedQueue.name}</h2>
                             
                             <Tabs value={viewMode} onValueChange={(v) => setViewMode(v as 'traffic' | 'dlq')} className="h-7">
                                <TabsList className="h-7 bg-muted/50 p-0.5">
                                    <TabsTrigger value="traffic" className="h-6 text-[10px] px-3 data-[state=active]:bg-background">
                                        TRAFFIC
                                    </TabsTrigger>
                                    <TabsTrigger value="dlq" className="h-6 text-[10px] px-3 data-[state=active]:bg-destructive data-[state=active]:text-destructive-foreground">
                                        DLQ {selectedQueue.dlq > 0 && `(${selectedQueue.dlq})`}
                                    </TabsTrigger>
                                </TabsList>
                             </Tabs>
                         </div>

                         {viewMode === 'traffic' ? (
                             <div className="flex gap-1">
                                 <FilterButton label="Pending" count={selectedQueue.pending} active={messageState === 'Pending'} onClick={() => setMessageState('Pending')} />
                                 <FilterButton label="InFlight" count={selectedQueue.inflight} active={messageState === 'InFlight'} onClick={() => setMessageState('InFlight')} />
                                 <FilterButton label="Scheduled" count={selectedQueue.scheduled} active={messageState === 'Scheduled'} onClick={() => setMessageState('Scheduled')} />
                             </div>
                         ) : (
                             <div className="h-7 flex items-center text-xs text-destructive font-bold bg-destructive/5 px-2 rounded border border-destructive/20">
                                 <AlertTriangle className="h-3.5 w-3.5 mr-2" />
                                 DEAD LETTER QUEUE
                             </div>
                         )}
                     </div>

                     {/* Messages Table - Simple Map */}
                     <div className="flex-1 overflow-y-auto bg-background flex flex-col">
                        {isLoading ? (
                            <div className="flex-1 flex flex-col items-center justify-center text-muted-foreground">
                                <RefreshCw className="h-6 w-6 animate-spin mb-4 opacity-50" />
                                <p className="text-xs font-mono uppercase tracking-widest opacity-50">LOADING...</p>
                            </div>
                        ) : messages.length === 0 ? (
                            <div className="flex-1 flex flex-col items-center justify-center text-muted-foreground">
                                <Box className="h-12 w-12 opacity-20 mb-4" />
                                <p className="text-xs font-mono uppercase tracking-widest opacity-50">
                                    {viewMode === 'traffic' ? 'NO_MESSAGES' : 'NO_ERRORS_FOUND'}
                                </p>
                            </div>
                        ) : (
                            <div className="flex-1 flex flex-col">
                                {/* Table Header */}
                                {viewMode === 'traffic' ? (
                                    <div className="sticky top-0 bg-section-header border-b border-border px-3 py-2 grid grid-cols-[1fr_80px_80px] gap-2 text-xs font-bold uppercase text-muted-foreground z-10 shadow-sm">
                                        <div>ID</div>
                                        <div className="text-center">Priority</div>
                                        <div className="text-center">Attempts</div>
                                    </div>
                                ) : (
                                    <div className="sticky top-0 bg-section-header border-b border-border px-3 py-2 grid grid-cols-[120px_1fr_60px_60px] gap-2 text-xs font-bold uppercase text-muted-foreground z-10 shadow-sm">
                                        <div>Failed At</div>
                                        <div>Reason</div>
                                        <div className="text-center">Tries</div>
                                        <div className="text-right">ID</div>
                                    </div>
                                )}

                                {/* Simple List */}
                                <div className="flex-1">
                                    {messages.map((msg: any) => {
                                        const isSelected = selectedMessageId === msg.id
                                        
                                        if (viewMode === 'dlq') {
                                            const dlqMsg = msg as DlqMessageSummary
                                            return (
                                                <div
                                                    key={msg.id}
                                                    onClick={() => setSelectedMessageId(msg.id)}
                                                    className={`
                                                        grid grid-cols-[120px_1fr_60px_60px] gap-2 px-3 py-2 border-b border-border/50 cursor-pointer transition-all items-center h-9
                                                        ${isSelected ? 'bg-destructive/10 border-destructive/20' : 'hover:bg-destructive/5'}
                                                    `}
                                                >
                                                    <span className="font-mono text-xs text-muted-foreground truncate">
                                                        {new Date(dlqMsg.created_at).toLocaleTimeString()}
                                                    </span>
                                                    <span className="font-mono text-xs text-destructive truncate font-medium" title={dlqMsg.failure_reason}>
                                                        {dlqMsg.failure_reason}
                                                    </span>
                                                    <span className="font-mono text-xs text-center text-muted-foreground">
                                                        {dlqMsg.attempts}
                                                    </span>
                                                    <span className="font-mono text-xs text-right opacity-40 truncate">
                                                        ...{dlqMsg.id.slice(-4)}
                                                    </span>
                                                </div>
                                            )
                                        }

                                        const trafficMsg = msg as MessageSummary
                                        return (
                                            <div
                                                key={msg.id}
                                                onClick={() => setSelectedMessageId(msg.id)}
                                                className={`
                                                    grid grid-cols-[1fr_80px_80px] gap-2 px-3 py-2 border-b border-border/50 cursor-pointer transition-all items-center h-9
                                                    ${isSelected ? 'bg-secondary' : 'hover:bg-muted/50'}
                                                `}
                                            >
                                                <span className={`font-mono text-sm truncate ${isSelected ? 'text-foreground font-bold' : 'text-muted-foreground'}`} title={msg.id}>
                                                    {msg.id}
                                                </span>
                                                <div className={`text-center text-xs ${trafficMsg.priority > 0 ? 'text-status-pending font-bold' : 'text-muted-foreground'}`}>
                                                    {trafficMsg.priority}
                                                </div>
                                                <div className={`text-center text-xs ${trafficMsg.attempts > 0 ? 'text-destructive font-bold' : 'text-muted-foreground'}`}>
                                                    {trafficMsg.attempts}
                                                </div>
                                            </div>
                                        )
                                    })}
                                </div>
                            </div>
                        )}
                     </div>

                     {/* Pagination Controls */}
                     <div className="p-2 border-t-2 border-border bg-section-header flex items-center justify-between flex-shrink-0">
                         <div className="text-xs text-muted-foreground font-mono">
                             {total > 0 ? (
                                 <span>{offset + 1}-{Math.min(offset + PAGE_SIZE, total)} of {total}</span>
                             ) : (
                                 <span>0 items</span>
                             )}
                         </div>
                         <div className="flex gap-1">
                             <button
                                 disabled={offset === 0 || isLoading}
                                 onClick={() => setOffset(o => Math.max(0, o - PAGE_SIZE))}
                                 className="h-7 px-2 flex items-center gap-1 text-xs font-mono uppercase bg-background border border-border rounded hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                             >
                                 <ChevronLeft className="h-3 w-3" />
                                 Prev
                             </button>
                             <button
                                 disabled={!hasMore || isLoading}
                                 onClick={() => setOffset(o => o + PAGE_SIZE)}
                                 className="h-7 px-2 flex items-center gap-1 text-xs font-mono uppercase bg-background border border-border rounded hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                             >
                                 Next
                                 <ChevronRight className="h-3 w-3" />
                             </button>
                         </div>
                     </div>
                 </div>
             ) : (
                <div className="flex-1 flex flex-col items-center justify-center text-muted-foreground">
                    <p className="text-xs font-mono uppercase tracking-widest opacity-50">SELECT_QUEUE</p>
                </div>
             )}
          </div>

          {/* RIGHT PANEL: Message Details (Expanded) */}
          <div className="flex-1 flex flex-col border-l border-border bg-content min-w-0">
              {selectedMessage ? (
                  <div className="flex flex-col h-full">
                      
                      {/* DLQ Context Header */}
                      {viewMode === 'dlq' && 'failure_reason' in selectedMessage && (
                          <div className="p-4 bg-destructive/10 border-b border-destructive/20">
                              <div className="flex items-center gap-2 mb-2 text-destructive font-bold text-xs uppercase">
                                  <AlertCircle className="h-4 w-4" />
                                  Failure Reason
                              </div>
                              <p className="font-mono text-sm text-foreground break-words mb-3">
                                  {(selectedMessage as DlqMessageSummary).failure_reason}
                              </p>
                              <div className="flex gap-4 text-xs text-muted-foreground font-mono">
                                  <span>Failed: {new Date((selectedMessage as DlqMessageSummary).created_at).toLocaleString()}</span>
                                  <span>Attempts: {selectedMessage.attempts}</span>
                              </div>
                          </div>
                      )}

                      {/* Header - Compact */}
                      <div className="p-3 border-b border-border bg-section-header flex-shrink-0">
                          <div className="flex items-center justify-between mb-2">
                              <h3 className="text-xs font-bold uppercase text-muted-foreground">MESSAGE ID</h3>
                              <button
                                  onClick={() => copyToClipboard(selectedMessage.id, selectedMessage.id)}
                                  className="p-1 hover:bg-muted rounded transition-colors"
                                  title="Copy Message ID"
                              >
                                  {copiedId === selectedMessage.id ? (
                                      <Check className="h-3 w-3 text-status-success" />
                                  ) : (
                                      <Copy className="h-3 w-3 text-muted-foreground" />
                                  )}
                              </button>
                          </div>
                          <div className="bg-muted/50 p-2 rounded border border-border break-all">
                              <span className="font-mono text-sm text-foreground">{selectedMessage.id}</span>
                          </div>
                      </div>

                      {/* Delivery Time - Only for Scheduled */}
                      {viewMode === 'traffic' && messageState === 'Scheduled' && 'next_delivery_at' in selectedMessage && (
                          <div className="p-3 border-b border-border bg-section-header">
                              <h3 className="text-xs font-bold uppercase text-muted-foreground mb-2">DELIVERY AT</h3>
                              <div className="bg-panel p-2 rounded border border-border">
                                  <span className="font-mono text-sm text-foreground">{(selectedMessage as ScheduledMessageSummary).next_delivery_at}</span>
                              </div>
                          </div>
                      )}

                      {/* Payload - Full Space */}
                      <div className="flex-1 flex flex-col min-h-0 p-3">
                          <h4 className="text-sm font-bold uppercase text-muted-foreground mb-2 flex-shrink-0">PAYLOAD</h4>
                          <ScrollArea className="flex-1 border border-border rounded bg-muted/50">
                              <div className="p-3">
                                  <pre className="font-mono text-sm text-foreground whitespace-pre-wrap break-words">
                                      {typeof selectedMessage.payload === 'string' 
                                          ? selectedMessage.payload 
                                          : JSON.stringify(selectedMessage.payload, null, 2)}
                                  </pre>
                              </div>
                          </ScrollArea>
                      </div>
                  </div>
              ) : (
                  <div className="flex-1 flex flex-col items-center justify-center text-muted-foreground">
                      <p className="text-xs font-mono uppercase tracking-widest opacity-50">SELECT_MESSAGE</p>
                  </div>
              )}
          </div>

      </div>
  )
}

interface FilterButtonProps {
  label: string
  count: number
  active: boolean
  onClick: () => void
}

function FilterButton({ label, count, active, onClick }: FilterButtonProps) {
    return (
        <button 
            onClick={onClick}
            className={`
                px-3 py-1 text-xs font-mono uppercase rounded-sm border transition-all flex items-center gap-2
                ${active 
                    ? 'bg-secondary border-border text-foreground' 
                    : 'bg-transparent border-transparent text-muted-foreground hover:bg-muted hover:text-foreground'
                }
            `}
        >
            {label}
            {count > 0 && <span className={`opacity-60 ${active ? 'text-foreground' : ''}`}>({count})</span>}
        </button>
    )
}

