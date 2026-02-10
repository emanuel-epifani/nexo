import { useState, useMemo, useRef, useEffect } from "react"
import { useVirtualizer } from "@tanstack/react-virtual"
import { QueueBrokerSnapshot, QueueSummary, MessageSummary, ScheduledMessageSummary, DlqMessageSummary } from "./types"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { 
    Search, 
    MessageSquare,
    RefreshCw,
    Clock,
    AlertCircle,
    Box,
    AlertTriangle,
    Copy,
    Check,
} from "lucide-react"
import { Badge } from "@/components/ui/badge"

interface Props {
  data: QueueBrokerSnapshot
}

export function QueueView({ data }: Props) {
  const [filter, setFilter] = useState("")
  const [selectedQueueName, setSelectedQueueName] = useState<string | null>(null)
  
  // View Mode: 'traffic' (Active) or 'dlq' (Dead Letter)
  const [viewMode, setViewMode] = useState<'traffic' | 'dlq'>('traffic')

  const [selectedMessageId, setSelectedMessageId] = useState<string | null>(null)
  const [copiedId, setCopiedId] = useState<string | null>(null)
  
  // Message State Filter (for Traffic view)
  const [messageState, setMessageState] = useState<'Pending' | 'InFlight' | 'Scheduled'>('Pending')
  
  // Sort State
  const [sortColumn, setSortColumn] = useState<'priority' | 'attempts' | null>(null)
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc')
  
  // Virtualization ref
  const parentRef = useRef<HTMLDivElement>(null)

  // Reset selection when switching queues
  useEffect(() => {
      setSelectedMessageId(null)
      // Default to traffic view when switching queues, unless the queue only has DLQ items? 
      // Better to stick to 'traffic' default or keep current viewMode if user wants to check DLQ of multiple queues.
      // Let's keep viewMode persistent for now, but reset if queue has no DLQ? No, let user see empty DLQ.
  }, [selectedQueueName])

  const filteredQueues = useMemo(() => {
      return (data || []).filter(q => q.name.toLowerCase().includes(filter.toLowerCase()))
  }, [data, filter])

  const selectedQueue = useMemo(() => {
      return (data || []).find((q: QueueSummary) => q.name === selectedQueueName)
  }, [data, selectedQueueName])

  // Get messages for current state and apply sorting
  const filteredMessages = useMemo(() => {
      if (!selectedQueue) return []
      
      let messages: MessageSummary[] = []

      if (viewMode === 'traffic') {
          if (messageState === 'Pending') messages = selectedQueue.pending || []
          else if (messageState === 'InFlight') messages = selectedQueue.inflight || []
          else messages = selectedQueue.scheduled || []
      } else {
          // DLQ Mode
          messages = selectedQueue.dlq || []
      }
      
      // Apply sorting
      if (sortColumn) {
          messages = [...messages].sort((a, b) => {
              let aVal = sortColumn === 'priority' ? a.priority : a.attempts
              let bVal = sortColumn === 'priority' ? b.priority : b.attempts
              
              if (sortDirection === 'asc') {
                  return aVal - bVal
              } else {
                  return bVal - aVal
              }
          })
      } else if (viewMode === 'dlq') {
          // Default sort for DLQ: Most recent first
          messages = [...messages].sort((a, b) => {
              const aTime = (a as DlqMessageSummary).created_at || 0
              const bTime = (b as DlqMessageSummary).created_at || 0
              return bTime - aTime
          })
      }
      
      return messages
  }, [selectedQueue, viewMode, messageState, sortColumn, sortDirection])

  // Handle sort column click
  const handleSortClick = (column: 'priority' | 'attempts') => {
      if (sortColumn === column) {
          setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc')
      } else {
          setSortColumn(column)
          setSortDirection('desc')
      }
  }

  // Get selected message details
  const selectedMessage = useMemo(() => {
      if (!selectedQueue || !selectedMessageId) return undefined
      
      // Search in all lists to be safe (or just the active view's list)
      const allLists = [
          ...(selectedQueue.pending || []),
          ...(selectedQueue.inflight || []),
          ...(selectedQueue.scheduled || []),
          ...(selectedQueue.dlq || [])
      ]
      
      return allLists.find(m => m.id === selectedMessageId)
  }, [selectedQueue, selectedMessageId])

  // Copy to clipboard helper
  const copyToClipboard = (text: string, id: string) => {
      navigator.clipboard.writeText(text)
      setCopiedId(id)
      setTimeout(() => setCopiedId(null), 2000)
  }

  // Virtualization setup
  const virtualizer = useVirtualizer({
      count: filteredMessages.length,
      getScrollElement: () => parentRef.current,
      estimateSize: () => 36, // h-9 = 36px
      overscan: 10,
  })

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
                              const hasDlq = (q.dlq || []).length > 0
                              const totalPending = (q.pending || []).length
                              
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
                                                  {(q.dlq || []).length}
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
                                        DLQ {(selectedQueue.dlq || []).length > 0 && `(${(selectedQueue.dlq || []).length})`}
                                    </TabsTrigger>
                                </TabsList>
                             </Tabs>
                         </div>

                         {viewMode === 'traffic' ? (
                             <div className="flex gap-1">
                                 <FilterButton label="Pending" count={(selectedQueue.pending || []).length} active={messageState === 'Pending'} onClick={() => setMessageState('Pending')} />
                                 <FilterButton label="InFlight" count={(selectedQueue.inflight || []).length} active={messageState === 'InFlight'} onClick={() => setMessageState('InFlight')} />
                                 <FilterButton label="Scheduled" count={(selectedQueue.scheduled || []).length} active={messageState === 'Scheduled'} onClick={() => setMessageState('Scheduled')} />
                             </div>
                         ) : (
                             <div className="h-7 flex items-center text-xs text-destructive font-bold bg-destructive/5 px-2 rounded border border-destructive/20">
                                 <AlertTriangle className="h-3.5 w-3.5 mr-2" />
                                 DEAD LETTER QUEUE
                             </div>
                         )}
                     </div>

                     {/* Messages Table - Virtualized */}
                     <div ref={parentRef} className="flex-1 overflow-y-auto bg-background">
                        {filteredMessages.length === 0 ? (
                            <div className="h-full flex flex-col items-center justify-center text-muted-foreground">
                                <Box className="h-12 w-12 opacity-20 mb-4" />
                                <p className="text-xs font-mono uppercase tracking-widest opacity-50">
                                    {viewMode === 'traffic' ? 'NO_MESSAGES' : 'NO_ERRORS_FOUND'}
                                </p>
                            </div>
                        ) : (
                            <div className="p-0">
                                {/* Table Header */}
                                {viewMode === 'traffic' ? (
                                    <div className="sticky top-0 bg-section-header border-b border-border px-3 py-2 grid grid-cols-[1fr_80px_80px_80px] gap-2 text-xs font-bold uppercase text-muted-foreground z-10 shadow-sm">
                                        <div>ID</div>
                                        <div className="text-center">Status</div>
                                        <div className="text-center cursor-pointer hover:text-foreground" onClick={() => handleSortClick('priority')}>Priority</div>
                                        <div className="text-center cursor-pointer hover:text-foreground" onClick={() => handleSortClick('attempts')}>Attempts</div>
                                    </div>
                                ) : (
                                    <div className="sticky top-0 bg-section-header border-b border-border px-3 py-2 grid grid-cols-[120px_1fr_60px_60px] gap-2 text-xs font-bold uppercase text-muted-foreground z-10 shadow-sm">
                                        <div>Failed At</div>
                                        <div>Reason</div>
                                        <div className="text-center">Tries</div>
                                        <div className="text-right">ID</div>
                                    </div>
                                )}

                                {/* Virtual List */}
                                <div style={{
                                    height: `${virtualizer.getTotalSize()}px`,
                                    width: '100%',
                                    position: 'relative',
                                }}>
                                    {virtualizer.getVirtualItems().map((virtualItem) => (
                                        <div
                                            key={filteredMessages[virtualItem.index].id}
                                            data-index={virtualItem.index}
                                            style={{
                                                position: 'absolute',
                                                top: 0,
                                                left: 0,
                                                width: '100%',
                                                transform: `translateY(${virtualItem.start}px)`,
                                            }}
                                        >
                                            {(() => {
                                                const msg = filteredMessages[virtualItem.index]
                                                const isSelected = selectedMessageId === msg.id
                                                
                                                if (viewMode === 'dlq') {
                                                    const dlqMsg = msg as DlqMessageSummary
                                                    return (
                                                        <div
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

                                                return (
                                                    <div
                                                        onClick={() => setSelectedMessageId(msg.id)}
                                                        className={`
                                                            grid grid-cols-[1fr_80px_80px_80px] gap-2 px-3 py-2 border-b border-border/50 cursor-pointer transition-all items-center h-9
                                                            ${isSelected ? 'bg-secondary' : 'hover:bg-muted/50'}
                                                        `}
                                                    >
                                                        <span className={`font-mono text-sm truncate ${isSelected ? 'text-foreground font-bold' : 'text-muted-foreground'}`} title={msg.id}>
                                                            {msg.id}
                                                        </span>
                                                        <div className="flex justify-center">
                                                            <StatusBadgeCompact state={msg.state} />
                                                        </div>
                                                        <div className={`text-center text-xs ${msg.priority > 0 ? 'text-status-pending font-bold' : 'text-muted-foreground'}`}>
                                                            {msg.priority}
                                                        </div>
                                                        <div className={`text-center text-xs ${msg.attempts > 0 ? 'text-destructive font-bold' : 'text-muted-foreground'}`}>
                                                            {msg.attempts}
                                                        </div>
                                                    </div>
                                                )
                                            })()}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}
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

interface StatusBadgeCompactProps {
  state: string
}

function StatusBadgeCompact({ state }: StatusBadgeCompactProps) {
    let color = "bg-muted text-muted-foreground border-border"
    let icon = <Clock className="h-2 w-2" />

    if (state === 'InFlight') {
        color = "bg-status-processing/10 text-status-processing border-status-processing/30"
        icon = <RefreshCw className="h-2 w-2 animate-spin duration-[3s]" />
    } else if (state === 'Pending') {
        color = "bg-status-pending/10 text-status-pending border-status-pending/30"
        icon = <AlertCircle className="h-2 w-2" />
    } else if (state === 'Scheduled') {
        color = "bg-secondary text-foreground border-border"
        icon = <Clock className="h-2 w-2" />
    }

    return (
        <div className={`flex items-center gap-1 px-1 py-0.5 rounded-sm border text-xs ${color}`}>
            {icon}
            <span className="uppercase font-bold">{state}</span>
        </div>
    )
}
