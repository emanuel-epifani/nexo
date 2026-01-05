import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { StreamBrokerSnapshot, TopicSummary } from "@/lib/types"
import { Badge } from "@/components/ui/badge"
import { Users, HardDrive } from "lucide-react"

interface Props {
  data: StreamBrokerSnapshot
}

export function StreamView({ data }: Props) {
  return (
    <div className="space-y-6">
       <h2 className="text-2xl font-bold tracking-tight">Event Streaming</h2>
      {data.topics.map(topic => (
          <TopicDetail key={topic.name} topic={topic} />
      ))}
    </div>
  )
}

function TopicDetail({ topic }: { topic: TopicSummary }) {
    return (
        <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
                <div className="space-y-1">
                    <CardTitle className="text-xl font-mono">{topic.name}</CardTitle>
                    <div className="flex gap-2 text-sm text-muted-foreground">
                        <span className="flex items-center gap-1"><HardDrive className="h-3 w-3" /> {topic.partitions_count} Partitions</span>
                        <span>•</span>
                        <span>{topic.total_messages.toLocaleString()} msgs</span>
                    </div>
                </div>
            </CardHeader>
            <CardContent>
                <h4 className="mb-2 text-sm font-semibold flex items-center gap-2">
                    <Users className="h-4 w-4" /> Consumer Groups ({topic.consumer_groups.length})
                </h4>
                {topic.consumer_groups.length === 0 ? (
                    <div className="text-sm text-muted-foreground italic">No active consumer groups.</div>
                ) : (
                    <div className="space-y-4">
                        {topic.consumer_groups.map(group => (
                            <div key={group.name} className="border rounded-md p-4 bg-muted/10">
                                <div className="flex items-center justify-between mb-4">
                                    <div className="flex items-center gap-2">
                                        <span className="font-semibold">{group.name}</span>
                                        {group.pending_messages > 0 ? (
                                            <Badge variant="destructive">Lag: {group.pending_messages}</Badge>
                                        ) : (
                                            <Badge variant="outline" className="text-green-600 bg-green-50 border-green-200">Synced</Badge>
                                        )}
                                    </div>
                                    <span className="text-xs text-muted-foreground">{group.connected_clients} clients connected</span>
                                </div>
                                
                                <Table>
                                    <TableHeader>
                                        <TableRow className="bg-muted/50">
                                            <TableHead className="h-8">Client ID</TableHead>
                                            <TableHead className="h-8">Assigned Partitions</TableHead>
                                        </TableRow>
                                    </TableHeader>
                                    <TableBody>
                                        {group.members.length === 0 ? (
                                             <TableRow>
                                                <TableCell colSpan={2} className="text-center text-xs text-muted-foreground py-2">No members connected (Group inactive)</TableCell>
                                            </TableRow>
                                        ) : (
                                            group.members.map(member => (
                                                <TableRow key={member.client_id}>
                                                    <TableCell className="font-mono text-xs">{member.client_id}</TableCell>
                                                    <TableCell className="text-xs">
                                                        <div className="flex gap-1">
                                                            {member.partitions_assigned.map(p => (
                                                                <span key={p} className="inline-block px-1.5 py-0.5 bg-secondary rounded border text-[10px]">
                                                                    P-{p}
                                                                </span>
                                                            ))}
                                                        </div>
                                                    </TableCell>
                                                </TableRow>
                                            ))
                                        )}
                                    </TableBody>
                                </Table>
                            </div>
                        ))}
                    </div>
                )}
            </CardContent>
        </Card>
    )
}

