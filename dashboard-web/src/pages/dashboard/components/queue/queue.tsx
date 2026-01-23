import { useState, useMemo, useEffect } from "react"
import { QueueBrokerSnapshot, QueueSummary, MessageSummary } from "./types"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { 
    Search, 
    MessageSquare,
    RefreshCw,
    Clock,
    AlertCircle,
    Box,
    AlertTriangle,
    Ban,
    Copy,
    Check,
    ArrowUp,
    ArrowDown,
} from "lucide-react"
import { Badge } from "@/components/ui/badge"

interface Props {
  data: QueueBrokerSnapshot
}

export function QueueView({ data }: Props) {
  const [activeTab, setActiveTab] = useState<'active' | 'dlq'>('active')
  const [filter, setFilter] = useState("")
  const [selectedQueueName, setSelectedQueueName] = useState<string | null>(null)
  const [selectedMessageId, setSelectedMessageId] = useState<string | null>(null)
  const [copiedId, setCopiedId] = useState<string | null>(null)
  
  // Message Filter State
  const [messageFilter, setMessageFilter] = useState<'All' | 'Pending' | 'InFlight' | 'Scheduled'>('All')
  
  // Sort State
  const [sortColumn, setSortColumn] = useState<'priority' | 'attempts' | null>(null)
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc')

  // Reset selection when switching Main Tabs
  useEffect(() => {
      setSelectedQueueName(null)
      setSelectedMessageId(null)
  }, [activeTab])

  // Split queues into Active vs DLQ
  const { activeQueues, dlqQueues } = useMemo(() => {
      const active: QueueSummary[] = []
      const dlq: QueueSummary[] = []

      data.queues.forEach((q: QueueSummary) => {
          if (q.name.endsWith('_dlq')) {
              if (q.pending_count > 0 || q.inflight_count > 0 || q.scheduled_count > 0) {
                  dlq.push(q)
              }
          } else {
              active.push(q)
          }
      })
      
      return { activeQueues: active, dlqQueues: dlq }
  }, [data.queues])

  const currentList = activeTab === 'active' ? activeQueues : dlqQueues

  const filteredQueues = useMemo(() => {
      return currentList.filter(q => q.name.toLowerCase().includes(filter.toLowerCase()))
  }, [currentList, filter])

  const selectedQueue = useMemo(() => 
      data.queues.find((q: QueueSummary) => q.name === selectedQueueName),
  [data.queues, selectedQueueName])

  // Filter and Sort Messages inside selected queue
  const filteredMessages = useMemo(() => {
      if (!selectedQueue) return []
      
      let messages = messageFilter === 'All' 
          ? selectedQueue.messages 
          : selectedQueue.messages.filter((m: MessageSummary) => m.state === messageFilter)
      
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
      }
      
      return messages
  }, [selectedQueue, messageFilter, sortColumn, sortDirection])

  // Handle sort column click
  const handleSortClick = (column: 'priority' | 'attempts') => {
      if (sortColumn === column) {
          // Toggle direction if same column
          setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc')
      } else {
          // Set new column with desc as default
          setSortColumn(column)
          setSortDirection('desc')
      }
  }

  // Get selected message details
  const selectedMessage = useMemo(() => 
      selectedQueue?.messages.find((m: MessageSummary) => m.id === selectedMessageId),
  [selectedQueue, selectedMessageId])

  // Copy to clipboard helper
  const copyToClipboard = (text: string, id: string) => {
      navigator.clipboard.writeText(text)
      setCopiedId(id)
      setTimeout(() => setCopiedId(null), 2000)
  }

  // Parse payload JSON safely
  const parsedPayload = useMemo(() => {
      if (!selectedMessage?.payload_preview) return null
      try {
          return JSON.parse(selectedMessage.payload_preview)
      } catch {
          return null
      }
  }, [selectedMessage?.payload_preview])

  return (
      <div className="flex h-full gap-0 border border-slate-800 rounded bg-slate-900/20 overflow-hidden font-mono text-sm">
          
          {/* SIDEBAR: Queues List */}
          <div className="w-[320px] flex flex-col border-r border-slate-800 bg-slate-950/50">
              {/* TABS */}
              <div className="flex border-b border-slate-800">
                  <button 
                      onClick={() => setActiveTab('active')}
                      className={`flex-1 py-3 text-[10px] font-bold uppercase tracking-wider transition-colors ${activeTab === 'active' ? 'bg-slate-800 text-white' : 'text-slate-500 hover:text-slate-300'}`}
                  >
                      Active ({activeQueues.length})
                  </button>
                  <div className="w-[1px] bg-slate-800" />
                  <button 
                      onClick={() => setActiveTab('dlq')}
                      className={`flex-1 py-3 text-[10px] font-bold uppercase tracking-wider transition-colors flex items-center justify-center gap-2 ${activeTab === 'dlq' ? 'bg-rose-950/30 text-rose-400' : 'text-slate-500 hover:text-slate-300'}`}
                  >
                      DLQ / Errors ({dlqQueues.length})
                      {dlqQueues.length > 0 && <AlertTriangle className="h-3 w-3 text-rose-500" />}
                  </button>
              </div>

              {/* SEARCH */}
              <div className="p-3 border-b border-slate-800">
                  <div className="relative">
                      <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-slate-500" />
                      <Input 
                          placeholder={activeTab === 'active' ? "FILTER_QUEUES..." : "FILTER_DLQ..."}
                          value={filter}
                          onChange={(e) => setFilter(e.target.value)}
                          className="h-9 pl-8 bg-slate-950 border-slate-800 text-xs font-mono placeholder:text-slate-600 focus-visible:ring-1 focus-visible:ring-slate-700"
                      />
                  </div>
              </div>

              {/* LIST */}
              <ScrollArea className="flex-1">
                  <div className="p-0">
                      {filteredQueues.length === 0 ? (
                          <div className="py-8 text-center text-xs text-slate-600 italic">
                              {activeTab === 'active' ? 'NO_ACTIVE_QUEUES' : 'NO_ERRORS_FOUND'}
                          </div>
                      ) : (
                          filteredQueues.map((q) => (
                              <div
                                  key={q.name}
                                  onClick={() => setSelectedQueueName(q.name)}
                                  className={`
                                      group flex items-center justify-between px-4 py-3 border-b border-slate-800/50 cursor-pointer transition-all
                                      ${selectedQueueName === q.name ? 'bg-slate-800' : 'hover:bg-slate-900/50'}
                                  `}
                              >
                                  <div className="flex items-center gap-3 overflow-hidden">
                                      <div className={`p-1.5 rounded ${
                                          selectedQueueName === q.name 
                                            ? (activeTab === 'dlq' ? 'bg-rose-900 text-rose-200' : 'bg-slate-700 text-white') 
                                            : 'bg-slate-900 text-slate-500'
                                      }`}>
                                          {activeTab === 'dlq' ? <Ban className="h-3.5 w-3.5" /> : <MessageSquare className="h-3.5 w-3.5" />}
                                      </div>
                                      <span className={`font-mono text-xs truncate ${selectedQueueName === q.name ? 'text-white font-bold' : 'text-slate-300'}`}>
                                          {q.name}
                                      </span>
                                  </div>

                                  <div className="flex items-center gap-2">
                                      {q.pending_count > 0 && (
                                          <Badge variant="outline" className={`h-4 px-1.5 text-[9px] rounded-sm border ${activeTab === 'dlq' ? 'border-rose-900 bg-rose-950/30 text-rose-500' : 'border-amber-900 bg-amber-950/30 text-amber-500'}`}>
                                              {q.pending_count}
                                          </Badge>
                                      )}
                                  </div>
                              </div>
                          ))
                      )}
                  </div>
              </ScrollArea>
          </div>

          {/* CENTER AREA: Messages Table */}
          <div className="w-[500px] bg-slate-950/30 flex flex-col min-w-0 border-r border-slate-800">
             {selectedQueue ? (
                 <div className="flex flex-col h-full">
                     {/* Header Stats */}
                     <div className="p-3 border-b border-slate-800 bg-slate-900/20">
                         <div className="flex justify-between items-center mb-3">
                             <h2 className={`text-xs font-bold ${activeTab === 'dlq' ? 'text-rose-400' : 'text-slate-100'}`}>{selectedQueue.name}</h2>
                             <div className="flex gap-3">
                                 <StatBadge label="PENDING" value={selectedQueue.pending_count} color="text-amber-500" />
                                 <StatBadge label="IN_FLIGHT" value={selectedQueue.inflight_count} color="text-blue-400" />
                                 <StatBadge label="SCHEDULED" value={selectedQueue.scheduled_count} color="text-slate-400" />
                             </div>
                         </div>

                         {/* MESSAGE FILTERS TABS */}
                         <div className="flex gap-1">
                             <FilterButton label="All" count={selectedQueue.messages.length} active={messageFilter === 'All'} onClick={() => setMessageFilter('All')} />
                             <FilterButton label="Pending" count={selectedQueue.messages.filter((m: MessageSummary) => m.state === 'Pending').length} active={messageFilter === 'Pending'} onClick={() => setMessageFilter('Pending')} />
                             <FilterButton label="InFlight" count={selectedQueue.messages.filter((m: MessageSummary) => m.state === 'InFlight').length} active={messageFilter === 'InFlight'} onClick={() => setMessageFilter('InFlight')} />
                             <FilterButton label="Scheduled" count={selectedQueue.messages.filter((m: MessageSummary) => m.state === 'Scheduled').length} active={messageFilter === 'Scheduled'} onClick={() => setMessageFilter('Scheduled')} />
                         </div>
                     </div>

                     {/* Messages Table */}
                     <ScrollArea className="flex-1">
                        {filteredMessages.length === 0 ? (
                            <div className="h-full flex flex-col items-center justify-center text-slate-700">
                                <Box className="h-12 w-12 opacity-20 mb-4" />
                                <p className="text-xs font-mono uppercase tracking-widest opacity-50">NO_MESSAGES_FOUND</p>
                            </div>
                        ) : (
                            <div className="p-0">
                                {/* Table Header */}
                                <div className="sticky top-0 bg-slate-900 border-b border-slate-800 px-3 py-2 grid grid-cols-[1fr_80px_80px_80px] gap-2 text-[9px] font-bold uppercase text-slate-500">
                                    <div>ID</div>
                                    <div className="text-center">Status</div>
                                    <button 
                                        onClick={() => handleSortClick('priority')}
                                        className="text-center hover:text-slate-300 transition-colors flex items-center justify-center gap-1"
                                        title="Sort by Priority"
                                    >
                                        <span>Priority</span>
                                        {sortColumn === 'priority' && (
                                            sortDirection === 'desc' 
                                                ? <ArrowDown className="h-3 w-3" />
                                                : <ArrowUp className="h-3 w-3" />
                                        )}
                                    </button>
                                    <button 
                                        onClick={() => handleSortClick('attempts')}
                                        className="text-center hover:text-slate-300 transition-colors flex items-center justify-center gap-1"
                                        title="Sort by Attempts"
                                    >
                                        <span>Attempts</span>
                                        {sortColumn === 'attempts' && (
                                            sortDirection === 'desc' 
                                                ? <ArrowDown className="h-3 w-3" />
                                                : <ArrowUp className="h-3 w-3" />
                                        )}
                                    </button>
                                </div>

                                {/* Table Rows */}
                                {filteredMessages.map((msg: MessageSummary) => (
                                    <div
                                        key={msg.id}
                                        onClick={() => setSelectedMessageId(msg.id)}
                                        className={`
                                            grid grid-cols-[1fr_80px_80px_80px] gap-2 px-3 py-2 border-b border-slate-800/50 cursor-pointer transition-all items-center
                                            ${selectedMessageId === msg.id ? 'bg-slate-800' : 'hover:bg-slate-900/50'}
                                        `}
                                    >
                                        <span className={`font-mono text-[9px] truncate ${selectedMessageId === msg.id ? 'text-white font-bold' : 'text-slate-400'}`} title={msg.id}>
                                            {msg.id}
                                        </span>
                                        <div className="flex justify-center">
                                            <StatusBadgeCompact state={msg.state} />
                                        </div>
                                        <div className={`text-center text-[9px] ${msg.priority > 0 ? 'text-amber-500 font-bold' : 'text-slate-600'}`}>
                                            {msg.priority}
                                        </div>
                                        <div className={`text-center text-[9px] ${msg.attempts > 0 ? 'text-rose-400 font-bold' : 'text-slate-600'}`}>
                                            {msg.attempts}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                     </ScrollArea>
                 </div>
             ) : (
                <div className="flex-1 flex flex-col items-center justify-center text-slate-700">
                    <p className="text-xs font-mono uppercase tracking-widest opacity-50">SELECT_QUEUE</p>
                </div>
             )}
          </div>

          {/* RIGHT PANEL: Message Details (Expanded) */}
          <div className="flex-1 flex flex-col border-l border-slate-800 bg-slate-950/50 min-w-0">
              {selectedMessage ? (
                  <div className="flex flex-col h-full">
                      {/* Header - Compact */}
                      <div className="p-3 border-b border-slate-800 bg-slate-900/20 flex-shrink-0">
                          <div className="flex items-center justify-between mb-2">
                              <h3 className="text-[10px] font-bold uppercase text-slate-400">ID</h3>
                              <button
                                  onClick={() => copyToClipboard(selectedMessage.id, selectedMessage.id)}
                                  className="p-1 hover:bg-slate-800 rounded transition-colors"
                                  title="Copy Message ID"
                              >
                                  {copiedId === selectedMessage.id ? (
                                      <Check className="h-3 w-3 text-green-500" />
                                  ) : (
                                      <Copy className="h-3 w-3 text-slate-500" />
                                  )}
                              </button>
                          </div>
                          <div className="bg-slate-900/50 p-2 rounded border border-slate-800 break-all">
                              <span className="font-mono text-[8px] text-slate-300">{selectedMessage.id}</span>
                          </div>
                      </div>

                      {/* Payload - Full Space */}
                      <div className="flex-1 flex flex-col min-h-0 p-3">
                          <h4 className="text-[10px] font-bold uppercase text-slate-400 mb-2 flex-shrink-0">PAYLOAD</h4>
                          <ScrollArea className="flex-1 border border-slate-800 rounded bg-slate-900/50">
                              <div className="p-3">
                                  {parsedPayload ? (
                                      <pre className="font-mono text-[8px] text-slate-300 whitespace-pre-wrap break-words">
                                          {JSON.stringify(parsedPayload, null, 2)}
                                      </pre>
                                  ) : (
                                      <div className="font-mono text-[8px] text-slate-400 whitespace-pre-wrap break-words">
                                          {selectedMessage.payload_preview}
                                      </div>
                                  )}
                              </div>
                          </ScrollArea>
                      </div>
                  </div>
              ) : (
                  <div className="flex-1 flex flex-col items-center justify-center text-slate-700">
                      <p className="text-xs font-mono uppercase tracking-widest opacity-50">SELECT_MESSAGE</p>
                  </div>
              )}
          </div>

      </div>
  )
}

function StatBadge({ label, value, color }: any) {
    return (
        <div className="flex items-center gap-2 text-[10px] font-mono">
            <span className="text-slate-600">{label}:</span>
            <span className={`font-bold ${value > 0 ? color : 'text-slate-600'}`}>{value}</span>
        </div>
    )
}

function FilterButton({ label, count, active, onClick }: any) {
    return (
        <button 
            onClick={onClick}
            className={`
                px-3 py-1 text-[10px] font-mono uppercase rounded-sm border transition-all flex items-center gap-2
                ${active 
                    ? 'bg-slate-800 border-slate-600 text-slate-200' 
                    : 'bg-transparent border-transparent text-slate-500 hover:bg-slate-900 hover:text-slate-400'
                }
            `}
        >
            {label}
            {count > 0 && <span className={`opacity-60 ${active ? 'text-white' : ''}`}>({count})</span>}
        </button>
    )
}

function StatusBadge({ state }: { state: string }) {
    let color = "bg-slate-800 text-slate-400 border-slate-700"
    let icon = <Clock className="h-3 w-3" />

    if (state === 'InFlight') {
        color = "bg-blue-950/30 text-blue-400 border-blue-900/50"
        icon = <RefreshCw className="h-3 w-3 animate-spin duration-[3s]" />
    } else if (state === 'Pending') {
        color = "bg-amber-950/30 text-amber-500 border-amber-900/50"
        icon = <AlertCircle className="h-3 w-3" />
    } else if (state === 'Scheduled') {
        color = "bg-slate-800 text-slate-300 border-slate-700"
        icon = <Clock className="h-3 w-3" />
    }

    return (
        <div className={`flex items-center gap-1.5 px-1.5 py-0.5 rounded-sm border ${color} w-fit`}>
            {icon}
            <span className="text-[9px] uppercase font-bold">{state}</span>
        </div>
    )
}

function StatusBadgeCompact({ state }: { state: string }) {
    let color = "bg-slate-800 text-slate-400 border-slate-700"
    let icon = <Clock className="h-2 w-2" />

    if (state === 'InFlight') {
        color = "bg-blue-950/30 text-blue-400 border-blue-900/50"
        icon = <RefreshCw className="h-2 w-2 animate-spin duration-[3s]" />
    } else if (state === 'Pending') {
        color = "bg-amber-950/30 text-amber-500 border-amber-900/50"
        icon = <AlertCircle className="h-2 w-2" />
    } else if (state === 'Scheduled') {
        color = "bg-slate-800 text-slate-300 border-slate-700"
        icon = <Clock className="h-2 w-2" />
    }

    return (
        <div className={`flex items-center gap-1 px-1 py-0.5 rounded-sm border text-[8px] ${color}`}>
            {icon}
            <span className="uppercase font-bold">{state}</span>
        </div>
    )
}
