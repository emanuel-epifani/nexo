import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { QueueBrokerSnapshot, QueueSummary } from "@/lib/types"
import { useState } from "react"
import { ChevronRight, Box } from "lucide-react"

interface Props {
  data: QueueBrokerSnapshot
}

export function QueueList({ data }: Props) {
  if (data.queues.length === 0) {
     return (
      <div className="border border-dashed border-slate-800 rounded p-8 text-center text-sm text-slate-500 font-mono">
        NO_ACTIVE_QUEUES
      </div>
    )
  }

  return (
    <div className="space-y-4">
        {data.queues.map(queue => (
            <QueueCard key={queue.name} queue={queue} />
        ))}
    </div>
  )
}

function QueueCard({ queue }: { queue: QueueSummary }) {
    const [expanded, setExpanded] = useState(false)

    return (
        <div className="rounded border border-slate-800 bg-slate-900/30 overflow-hidden">
            <div 
                className="flex items-center justify-between p-3 cursor-pointer hover:bg-slate-800/50 transition-colors" 
                onClick={() => setExpanded(!expanded)}
            >
                <div className="flex items-center gap-3">
                        <div className={`text-slate-500 transition-transform duration-200 ${expanded ? 'rotate-90' : ''}`}>
                            <ChevronRight className="h-4 w-4" />
                        </div>
                        <div className="font-mono font-medium text-sm text-slate-200">{queue.name}</div>
                </div>
                
                <div className="flex items-center gap-6 text-xs font-mono">
                        <div className="flex items-center gap-2">
                            <span className="text-slate-500">PENDING:</span>
                            <span className={queue.pending_count > 0 ? "text-amber-400 font-bold" : "text-slate-400"}>
                                {queue.pending_count}
                            </span>
                        </div>
                        <div className="flex items-center gap-2">
                            <span className="text-slate-500">IN_FLIGHT:</span>
                            <span className="text-slate-400">{queue.inflight_count}</span>
                        </div>
                        <div className="flex items-center gap-2">
                            <span className="text-slate-500">SCHEDULED:</span>
                            <span className="text-slate-400">{queue.scheduled_count}</span>
                        </div>
                </div>
            </div>

            {expanded && (
                <div className="border-t border-slate-800 bg-slate-950/50 p-4">
                    <h4 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3 flex items-center gap-2">
                        <Box className="h-3 w-3" /> Messages ({queue.messages.length})
                    </h4>
                    {queue.messages.length === 0 ? (
                        <div className="text-xs text-slate-600 font-mono italic">QUEUE_EMPTY</div>
                    ) : (
                        <div className="rounded border border-slate-800 overflow-hidden">
                            <Table>
                                <TableHeader className="bg-slate-900 border-b border-slate-800">
                                    <TableRow className="hover:bg-transparent">
                                        <TableHead className="h-8 font-mono text-[10px] uppercase text-slate-500">ID</TableHead>
                                        <TableHead className="h-8 font-mono text-[10px] uppercase text-slate-500">Payload</TableHead>
                                        <TableHead className="h-8 font-mono text-[10px] uppercase text-slate-500 w-[100px]">State</TableHead>
                                        <TableHead className="h-8 font-mono text-[10px] uppercase text-slate-500 w-[60px]">Pri</TableHead>
                                        <TableHead className="h-8 font-mono text-[10px] uppercase text-slate-500 w-[60px]">Att</TableHead>
                                        <TableHead className="h-8 font-mono text-[10px] uppercase text-slate-500 w-[140px]">Next Run</TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {queue.messages.map(msg => (
                                        <TableRow key={msg.id} className="text-xs border-slate-800 hover:bg-slate-900">
                                            <TableCell className="font-mono text-[10px] text-slate-500" title={msg.id}>
                                                {msg.id.slice(0, 8)}
                                            </TableCell>
                                            <TableCell>
                                                <code className="text-[10px] text-slate-300">
                                                    {msg.payload_preview}
                                                </code>
                                            </TableCell>
                                            <TableCell>
                                                <Badge variant="outline" className={`
                                                    rounded-sm text-[10px] px-1.5 py-0 h-4 border-0 font-mono uppercase font-normal
                                                    ${msg.state === 'Pending' ? 'bg-slate-800 text-slate-400' : ''}
                                                    ${msg.state === 'InFlight' ? 'bg-amber-950/30 text-amber-500' : ''}
                                                    ${msg.state === 'Scheduled' ? 'bg-blue-950/30 text-blue-400' : ''}
                                                `}>
                                                    {msg.state}
                                                </Badge>
                                            </TableCell>
                                            <TableCell className="font-mono text-slate-400">{msg.priority}</TableCell>
                                            <TableCell className="font-mono text-slate-400">{msg.attempts}</TableCell>
                                            <TableCell className="text-slate-500 text-[10px] font-mono">
                                                {msg.next_delivery_at ? new Date(msg.next_delivery_at).toLocaleTimeString() : '-'}
                                            </TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </div>
                    )}
                </div>
            )}
        </div>
    )
}
