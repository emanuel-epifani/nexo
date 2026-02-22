import { useState } from 'react'
import { useQueries } from '@tanstack/react-query'
import { StoreView } from './components/store/store'
import { QueueView } from './components/queue/queue'
import { StreamView } from './components/stream/stream'
import { PubSubView } from './components/pubsub/pubsub'
import { NavCard } from '@/components/layout/nav-card'
import { 
    Loader2, 
    ServerCrash, 
    Database, 
    MessageSquare, 
    Radio, 
    Activity,
} from 'lucide-react'

export function DashboardPage() {
  const [activeTab, setActiveTab] = useState<'store' | 'queue' | 'stream' | 'pubsub'>('store')

  const [queueQuery, streamQuery] = useQueries({
    queries: [
      {
        queryKey: ['queue-snapshot'],
        queryFn: async () => {
          const response = await fetch('/api/queue')
          if (!response.ok) throw new Error('Failed to fetch queue')
          return response.json()
        },
        enabled: activeTab === 'queue',
        gcTime: 0
      },
      {
        queryKey: ['stream-snapshot'],
        queryFn: async () => {
          const response = await fetch('/api/stream')
          if (!response.ok) throw new Error('Failed to fetch stream')
          return response.json()
        },
        enabled: activeTab === 'stream',
        gcTime: 0
      },
    ],
  })

  // Loading state: Only consider the ACTIVE tab's loading state
  const isLoading = 
      (activeTab === 'queue' && queueQuery.isLoading) ||
      (activeTab === 'stream' && streamQuery.isLoading);

  // Error state: Only consider the ACTIVE tab's error state
  const hasError = 
      (activeTab === 'queue' && queueQuery.error) ||
      (activeTab === 'stream' && streamQuery.error);

    if (isLoading) {
    return (
      <div className="flex h-screen items-center justify-center bg-background font-mono text-sm">
        <div className="flex items-center gap-2 text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            <span>CONNECTING_TO_BROKER...</span>
        </div>
      </div>
    )
  }

  if (hasError) {
    return (
      <div className="flex h-screen flex-col items-center justify-center gap-4 bg-background text-foreground">
        <ServerCrash className="h-12 w-12 text-destructive" />
        <h2 className="text-xl font-mono">CONNECTION_LOST</h2>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-screen bg-background text-foreground font-sans selection:bg-primary/20 selection:text-primary">
      {/* MAIN CONTAINER */}
      <div className="flex flex-col flex-1 max-w-[1600px] mx-auto w-full px-6 pt-6">
        
        {/* NAVIGATION GRID - HORIZONTAL COMPACT */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
            <NavCard 
                label="STORE" 
                desc="Cache IN MEMORY"
                active={activeTab === 'store'}
                onClick={() => setActiveTab('store')}
                icon={<Database className="h-5 w-5" />}
                onRefresh={() => {}}
                isRefreshing={false}
            />
            <NavCard 
                label="QUEUE" 
                desc="Job Processing"
                active={activeTab === 'queue'}
                onClick={() => setActiveTab('queue')}
                icon={<MessageSquare className="h-5 w-5" />}
                onRefresh={() => queueQuery.refetch()}
                isRefreshing={queueQuery.isFetching}
            />
            <NavCard 
                label="STREAM" 
                desc="Event Log"
                active={activeTab === 'stream'}
                onClick={() => setActiveTab('stream')}
                icon={<Activity className="h-5 w-5" />}
                onRefresh={() => streamQuery.refetch()}
                isRefreshing={streamQuery.isFetching}
            />
            <NavCard 
                label="PUBSUB" 
                desc="Realtime Messaging"
                active={activeTab === 'pubsub'}
                onClick={() => setActiveTab('pubsub')}
                icon={<Radio className="h-5 w-5" />}
                onRefresh={() => {}}
                isRefreshing={false}
            />
        </div>

        {/* CONTENT AREA */}
        <main className="h-[calc(100vh-11rem)] border-t border-border pt-6 overflow-hidden">
            {activeTab === 'store' && (
                <div className="h-full">
                    <StoreView />
                </div>
            )}
            {activeTab === 'queue' && queueQuery.data && (
                <div className="h-full space-y-4 overflow-auto">
                    <QueueView data={queueQuery.data} />
                </div>
            )}
            {activeTab === 'stream' && streamQuery.data && (
                <div className="h-full space-y-4 overflow-auto">
                    <StreamView data={streamQuery.data} />
                </div>
            )}
            {activeTab === 'pubsub' && (
                <div className="h-full space-y-4 overflow-auto">
                    <PubSubView />
                </div>
            )}
        </main>
      </div>
    </div>
  )
}
