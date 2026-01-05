import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Activity, Database, Layers, MessageSquare } from "lucide-react"
import { BrokersSnapshot } from "@/lib/types"

interface Props {
  data: BrokersSnapshot
}

export function OverviewStats({ data }: Props) {
  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Stream Topics</CardTitle>
          <Activity className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{data.stream.total_topics}</div>
          <p className="text-xs text-muted-foreground">
            {data.stream.total_active_groups} active groups
          </p>
        </CardContent>
      </Card>
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Active Queues</CardTitle>
          <Layers className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{data.queue.queues.length}</div>
          <p className="text-xs text-muted-foreground">
             queues configured
          </p>
        </CardContent>
      </Card>
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">KV Keys</CardTitle>
          <Database className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{data.store.total_keys}</div>
          <p className="text-xs text-muted-foreground">
            {data.store.expiring_keys} keys with TTL
          </p>
        </CardContent>
      </Card>
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">PubSub Clients</CardTitle>
          <MessageSquare className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{data.pubsub.active_clients}</div>
          <p className="text-xs text-muted-foreground">
            connected now
          </p>
        </CardContent>
      </Card>
    </div>
  )
}

