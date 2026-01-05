import { useSystemState } from '@/hooks/use-system-state'
import { OverviewStats } from '@/components/dashboard/overview-stats'
import { StreamList } from '@/components/dashboard/stream-list'
import { QueueList } from '@/components/dashboard/queue-list'
import { StoreView } from '@/components/views/store-view'
import { StreamView } from '@/components/views/stream-view'
import { PubSubView } from '@/components/views/pubsub-view'
import { Loader2, ServerCrash } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"

export default function DashboardPage() {
  const { data, isLoading, error, refetch } = useSystemState()

  if (isLoading) {
    return (
      <div className="flex h-screen items-center justify-center bg-background">
        <Loader2 className="h-12 w-12 animate-spin text-primary" />
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex h-screen flex-col items-center justify-center gap-4 bg-background">
        <ServerCrash className="h-12 w-12 text-destructive" />
        <h2 className="text-xl font-semibold">Failed to load dashboard</h2>
        <p className="text-muted-foreground">Is the Nexo server running?</p>
        <Button onClick={() => refetch()}>Try Again</Button>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-muted/40 p-8">
      <div className="mx-auto max-w-7xl space-y-8">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Nexo Dashboard</h1>
            <p className="text-muted-foreground mt-1">
              Server Time: {new Date(data.server_time).toLocaleString()} • Uptime: {formatUptime(data.uptime_seconds)}
            </p>
          </div>
          <Button variant="outline" size="sm" onClick={() => refetch()}>
            Refresh
          </Button>
        </div>

        <Tabs defaultValue="overview" className="space-y-4">
          <TabsList>
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="store">KV Store</TabsTrigger>
            <TabsTrigger value="queue">Queues</TabsTrigger>
            <TabsTrigger value="stream">Streams</TabsTrigger>
            <TabsTrigger value="pubsub">Pub/Sub</TabsTrigger>
          </TabsList>

          <TabsContent value="overview" className="space-y-8">
            <OverviewStats data={data.brokers} />
            <div className="grid gap-8 md:grid-cols-1">
                <StreamList data={data.brokers.stream} />
                <QueueList data={data.brokers.queue} />
            </div>
          </TabsContent>

          <TabsContent value="store">
            <StoreView data={data.brokers.store} />
          </TabsContent>

          <TabsContent value="queue">
             {/* Queue View reuses the QueueList but maybe we can expand it later */}
             <div className="space-y-4">
                <h2 className="text-2xl font-bold tracking-tight">Message Queues</h2>
                <QueueList data={data.brokers.queue} />
             </div>
          </TabsContent>

          <TabsContent value="stream">
            <StreamView data={data.brokers.stream} />
          </TabsContent>

          <TabsContent value="pubsub">
            <PubSubView data={data.brokers.pubsub} />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  )
}

function formatUptime(seconds: number): string {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    return `${h}h ${m}m ${s}s`;
}

