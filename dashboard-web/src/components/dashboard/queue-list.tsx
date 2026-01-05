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
import { QueueBrokerSnapshot } from "@/lib/types"

interface Props {
  data: QueueBrokerSnapshot
}

export function QueueList({ data }: Props) {
  if (data.queues.length === 0) {
     return (
      <Card>
        <CardHeader>
           <CardTitle>Message Queues</CardTitle>
        </CardHeader>
        <CardContent>
            <div className="text-sm text-muted-foreground">No queues active.</div>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Message Queues</CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Queue Name</TableHead>
              <TableHead>Pending</TableHead>
              <TableHead>In-Flight</TableHead>
              <TableHead>Scheduled</TableHead>
              <TableHead>Consumers</TableHead>
              <TableHead>Status</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.queues.map((queue) => (
              <TableRow key={queue.name}>
                <TableCell className="font-medium">{queue.name}</TableCell>
                <TableCell>{queue.pending_count}</TableCell>
                <TableCell>{queue.inflight_count}</TableCell>
                <TableCell>{queue.scheduled_count}</TableCell>
                <TableCell>{queue.consumers_waiting}</TableCell>
                <TableCell>
                    {queue.pending_count > 100 ? (
                        <Badge variant="destructive">High Load</Badge>
                    ) : (
                        <Badge variant="outline" className="border-green-500 text-green-600 bg-green-50">Healthy</Badge>
                    )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}

