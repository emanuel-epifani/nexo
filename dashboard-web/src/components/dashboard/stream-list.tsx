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
import { StreamBrokerSnapshot } from "@/lib/types"

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
              <TableHead>Consumer Groups</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.topics.map((topic) => (
              <TableRow key={topic.name}>
                <TableCell className="font-medium">{topic.name}</TableCell>
                <TableCell>{topic.partitions_count}</TableCell>
                <TableCell>{topic.total_messages.toLocaleString()}</TableCell>
                <TableCell>
                  <div className="flex flex-wrap gap-2">
                    {topic.consumer_groups.length === 0 ? (
                      <span className="text-muted-foreground text-xs italic">None</span>
                    ) : (
                      topic.consumer_groups.map(g => (
                        <Badge key={g.name} variant="secondary" className="text-xs">
                          {g.name} (Lag: {g.pending_messages})
                        </Badge>
                      ))
                    )}
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}

