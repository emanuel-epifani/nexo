import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { StreamBrokerSnapshot, TopicSummary } from "@/lib/types"
import { Users, HardDrive } from "lucide-react"

interface Props {
  data: StreamBrokerSnapshot
}

export function StreamView({ data }: Props) {
  return (
    <div className="grid grid-cols-1 gap-6">
      {data.topics.map(topic => (
          <TopicDetail key={topic.name} topic={topic} />
      ))}
    </div>
  )
}

function TopicDetail({ topic }: { topic: TopicSummary }) {
    return (
        <div className="rounded border border-slate-800 bg-slate-900/30">
            {/* Header */}
            <div className="p-4 border-b border-slate-800 flex items-center justify-between">
                <div className="flex items-center gap-3">
                    <div className="p-1.5 rounded bg-slate-800 text-slate-400">
                        <HardDrive className="h-4 w-4" />
                    </div>
                    <div>
                        <h3 className="text-sm font-bold font-mono text-slate-200">{topic.name}</h3>
                        <div className="text-[10px] font-mono text-slate-500 uppercase mt-0.5">
                            HIGH_WATERMARK: {topic.total_messages.toLocaleString()}
                        </div>
                    </div>
                </div>
                <div className="flex items-center gap-2">
                    <Users className="h-3 w-3 text-slate-600" />
                    <span className="text-xs font-mono text-slate-400">{topic.consumer_groups.length} GROUPS</span>
                </div>
            </div>

            {/* Content */}
            <div className="p-4">
                {topic.consumer_groups.length === 0 ? (
                    <div className="text-xs font-mono text-slate-600 italic">NO_CONSUMER_GROUPS</div>
                ) : (
                    <div className="space-y-4">
                        {topic.consumer_groups.map(group => (
                            <div key={group.name} className="space-y-2">
                                <div className="flex items-center justify-between text-xs">
                                    <span className="font-mono font-bold text-slate-400">{group.name}</span>
                                    <span className="font-mono text-[10px] text-slate-600">{group.members.length} MEMBERS</span>
                                </div>
                                
                                <div className="rounded border border-slate-800 overflow-hidden">
                                    <Table>
                                        <TableHeader className="bg-slate-900 border-b border-slate-800">
                                            <TableRow className="hover:bg-transparent h-7">
                                                <TableHead className="h-7 font-mono text-[10px] uppercase text-slate-500">Client ID</TableHead>
                                                <TableHead className="h-7 font-mono text-[10px] uppercase text-slate-500 text-right">Offset</TableHead>
                                                <TableHead className="h-7 font-mono text-[10px] uppercase text-slate-500 text-right w-[100px]">Lag</TableHead>
                                            </TableRow>
                                        </TableHeader>
                                        <TableBody>
                                            {group.members.length === 0 ? (
                                                 <TableRow className="hover:bg-transparent border-0">
                                                    <TableCell colSpan={3} className="text-center text-[10px] text-slate-600 py-2 font-mono italic">INACTIVE_GROUP</TableCell>
                                                </TableRow>
                                            ) : (
                                                group.members.map(member => (
                                                    <TableRow key={member.client_id} className="border-slate-800 hover:bg-slate-900 h-8">
                                                        <TableCell className="font-mono text-[10px] text-slate-400 py-1">{member.client_id}</TableCell>
                                                        <TableCell className="font-mono text-[10px] text-slate-300 text-right py-1">{member.current_offset}</TableCell>
                                                        <TableCell className="text-right py-1">
                                                            {member.lag > 0 ? (
                                                                <Badge variant="destructive" className="rounded-sm h-4 px-1.5 text-[10px] font-mono border-0 bg-rose-950 text-rose-500 hover:bg-rose-900">
                                                                    -{member.lag}
                                                                </Badge>
                                                            ) : (
                                                                <span className="text-[10px] font-mono text-emerald-600">SYNCED</span>
                                                            )}
                                                        </TableCell>
                                                    </TableRow>
                                                ))
                                            )}
                                        </TableBody>
                                    </Table>
                                </div>
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    )
}
