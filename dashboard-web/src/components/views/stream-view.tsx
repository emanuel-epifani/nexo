import { useState, useMemo } from "react"
import { StreamBrokerSnapshot } from "@/lib/types"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { 
    Search, 
    Activity,
    HardDrive,
    Users,
    ArrowRight
} from "lucide-react"
import { Badge } from "@/components/ui/badge"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

interface Props {
  data: StreamBrokerSnapshot
}

export function StreamView({ data }: Props) {
  const [filter, setFilter] = useState("")
  const [selectedTopicName, setSelectedTopicName] = useState<string | null>(null)

  // Default selection
  useMemo(() => {
      if (!selectedTopicName && data.topics.length > 0) {
          setSelectedTopicName(data.topics[0].name)
      }
  }, [data.topics, selectedTopicName])

  const filteredTopics = useMemo(() => {
      return data.topics.filter(t => t.name.toLowerCase().includes(filter.toLowerCase()))
  }, [data.topics, filter])

  const selectedTopic = useMemo(() => 
      data.topics.find(t => t.name === selectedTopicName),
  [data.topics, selectedTopicName])

  if (data.topics.length === 0) {
      return (
        <div className="flex h-[400px] border border-dashed border-slate-800 rounded items-center justify-center text-slate-600 font-mono text-xs">
            NO_ACTIVE_TOPICS
        </div>
      )
  }

  return (
      <div className="flex h-[calc(100vh-280px)] gap-0 border border-slate-800 rounded bg-slate-900/20 overflow-hidden font-mono text-sm">
          
          {/* SIDEBAR: Topics List */}
          <div className="w-[320px] flex flex-col border-r border-slate-800 bg-slate-950/50">
              <div className="p-3 border-b border-slate-800">
                  <div className="relative">
                      <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-slate-500" />
                      <Input 
                          placeholder="FILTER_TOPICS..."
                          value={filter}
                          onChange={(e) => setFilter(e.target.value)}
                          className="h-9 pl-8 bg-slate-950 border-slate-800 text-xs font-mono placeholder:text-slate-600 focus-visible:ring-1 focus-visible:ring-slate-700"
                      />
                  </div>
              </div>

              <ScrollArea className="flex-1">
                  <div className="p-0">
                      {filteredTopics.map((t) => (
                          <div
                              key={t.name}
                              onClick={() => setSelectedTopicName(t.name)}
                              className={`
                                  group flex items-center justify-between px-4 py-3 border-b border-slate-800/50 cursor-pointer transition-all
                                  ${selectedTopicName === t.name ? 'bg-slate-800' : 'hover:bg-slate-900/50'}
                              `}
                          >
                              <div className="flex items-center gap-3 overflow-hidden">
                                  <div className={`p-1.5 rounded ${selectedTopicName === t.name ? 'bg-slate-700 text-white' : 'bg-slate-900 text-slate-500'}`}>
                                      <Activity className="h-3.5 w-3.5" />
                                  </div>
                                  <span className={`font-mono text-xs truncate ${selectedTopicName === t.name ? 'text-white font-bold' : 'text-slate-300'}`}>
                                      {t.name}
                                  </span>
                              </div>

                              <div className="flex items-center gap-2">
                                  {t.consumer_groups.length > 0 && (
                                      <Badge variant="outline" className="h-4 px-1.5 text-[9px] border-slate-700 bg-slate-800 text-slate-400 rounded-sm">
                                          {t.consumer_groups.length} GRP
                                      </Badge>
                                  )}
                              </div>
                          </div>
                      ))}
                  </div>
              </ScrollArea>
          </div>

          {/* MAIN AREA: Consumer Groups Table */}
          <div className="flex-1 bg-slate-950/30 flex flex-col min-w-0">
             {selectedTopic ? (
                 <div className="flex flex-col h-full">
                     {/* Header Stats */}
                     <div className="p-4 border-b border-slate-800 bg-slate-900/20 flex justify-between items-center">
                         <div className="flex items-center gap-2">
                             <h2 className="text-sm font-bold text-slate-100">{selectedTopic.name}</h2>
                         </div>
                         <div className="flex gap-4 items-center">
                             <div className="flex items-center gap-2 text-[10px] font-mono bg-slate-900 border border-slate-800 px-2 py-1 rounded">
                                 <HardDrive className="h-3 w-3 text-slate-500" />
                                 <span className="text-slate-400">OFFSET:</span>
                                 <span className="text-slate-200 font-bold">{selectedTopic.total_messages.toLocaleString()}</span>
                             </div>
                         </div>
                     </div>

                     {/* Groups List */}
                     <div className="flex-1 overflow-auto bg-slate-950/10 p-6">
                        {selectedTopic.consumer_groups.length === 0 ? (
                            <div className="h-full flex flex-col items-center justify-center text-slate-700">
                                <Users className="h-12 w-12 opacity-20 mb-4" />
                                <p className="text-xs font-mono uppercase tracking-widest opacity-50">NO_CONSUMER_GROUPS</p>
                            </div>
                        ) : (
                            <div className="space-y-6">
                                {selectedTopic.consumer_groups.map(group => (
                                    <div key={group.name} className="border border-slate-800 rounded bg-slate-900/20 overflow-hidden">
                                        <div className="px-4 py-2 bg-slate-900/50 border-b border-slate-800 flex justify-between items-center">
                                            <div className="flex items-center gap-2">
                                                <Users className="h-3.5 w-3.5 text-indigo-400" />
                                                <span className="font-mono text-xs font-bold text-slate-300">{group.name}</span>
                                            </div>
                                            <span className="text-[10px] text-slate-500 uppercase">{group.members.length} MEMBERS</span>
                                        </div>
                                        
                                        <Table>
                                            <TableHeader className="bg-slate-900/30 border-b border-slate-800">
                                                <TableRow className="hover:bg-transparent">
                                                    <TableHead className="h-7 font-mono text-[10px] uppercase text-slate-500">Client ID</TableHead>
                                                    <TableHead className="h-7 font-mono text-[10px] uppercase text-slate-500 text-right">Offset</TableHead>
                                                    <TableHead className="h-7 font-mono text-[10px] uppercase text-slate-500 text-right w-[120px]">Lag</TableHead>
                                                </TableRow>
                                            </TableHeader>
                                            <TableBody>
                                                {group.members.map(member => (
                                                    <TableRow key={member.client_id} className="border-slate-800 hover:bg-slate-900/30">
                                                        <TableCell className="font-mono text-[10px] text-slate-400 py-1.5">
                                                            {member.client_id}
                                                        </TableCell>
                                                        <TableCell className="font-mono text-[10px] text-slate-300 text-right py-1.5">
                                                            {member.current_offset.toLocaleString()}
                                                        </TableCell>
                                                        <TableCell className="text-right py-1.5">
                                                            {member.lag > 0 ? (
                                                                <div className="inline-flex items-center gap-1.5 text-rose-500 font-mono text-[10px]">
                                                                    <span>-{member.lag}</span>
                                                                    <ArrowRight className="h-3 w-3" />
                                                                </div>
                                                            ) : (
                                                                <span className="text-[10px] font-mono text-emerald-600 bg-emerald-950/20 px-1.5 py-0.5 rounded border border-emerald-900/30">SYNCED</span>
                                                            )}
                                                        </TableCell>
                                                    </TableRow>
                                                ))}
                                                {group.members.length === 0 && (
                                                    <TableRow className="hover:bg-transparent">
                                                        <TableCell colSpan={3} className="text-center py-4 text-[10px] text-slate-600 italic">
                                                            NO_ACTIVE_MEMBERS
                                                        </TableCell>
                                                    </TableRow>
                                                )}
                                            </TableBody>
                                        </Table>
                                    </div>
                                ))}
                            </div>
                        )}
                     </div>
                 </div>
             ) : (
                <div className="flex-1 flex flex-col items-center justify-center text-slate-700">
                    <p className="text-xs font-mono uppercase tracking-widest opacity-50">SELECT_TOPIC</p>
                </div>
             )}
          </div>

      </div>
  )
}
