import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { StreamBrokerSnapshot, TopicSummary, PartitionInfo } from "@/lib/types"

interface Props {
  data: StreamBrokerSnapshot
}

export function StreamList({ data }: Props) {
  if (data.topics.length === 0) {
    return (
      <Card>
        <CardHeader>
           <CardTitle>Stream Topics</CardTitle>
        </CardHeader>
        <CardContent>
            <div className="text-sm text-muted-foreground">No topics created yet.</div>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Stream Topics</CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Topic Name</TableHead>
              <TableHead>Partitions</TableHead>
              <TableHead>Total Messages</TableHead>
              <TableHead>Active Consumers</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.topics.map((topic: TopicSummary) => {
              // Calculate total messages across all partitions
              const totalMessages = topic.partitions.reduce((sum: number, partition: PartitionInfo) => sum + partition.messages.length, 0);
              // Collect all unique consumers across all partitions
              const allConsumers = new Set<string>();
              topic.partitions.forEach((partition: PartitionInfo) => {
                partition.current_consumers.forEach((consumer: string) => allConsumers.add(consumer));
              });

              return (
                <TableRow key={topic.name}>
                  <TableCell className="font-medium">{topic.name}</TableCell>
                  <TableCell>
                    <Badge variant="outline">{topic.partitions.length}</Badge>
                  </TableCell>
                  <TableCell>{totalMessages.toLocaleString()}</TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-1">
                      {allConsumers.size === 0 ? (
                        <span className="text-muted-foreground text-xs italic">None</span>
                      ) : (
                        Array.from(allConsumers).map(consumer => (
                          <Badge key={consumer} variant="secondary" className="text-xs">
                            {consumer}
                          </Badge>
                        ))
                      )}
                    </div>
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}
