import { useState } from 'react'
import { useQueryClient, useIsFetching } from '@tanstack/react-query'
import { StoreView } from './components/store/store'
import { QueueView } from './components/queue/queue'
import { StreamView } from './components/stream/stream'
import { PubSubView } from './components/pubsub/pubsub'
import { NavCard } from '@/components/layout/nav-card'
import { Database, MessageSquare, Radio, Activity } from 'lucide-react'

const SNAPSHOT_KEYS = {
  store: ['store-snapshot'],
  queue: ['queue-snapshot'],
  stream: ['stream-snapshot'],
  pubsub: ['pubsub-snapshot'],
} as const

export function DashboardPage() {
  const [activeTab, setActiveTab] = useState<'store' | 'queue' | 'stream' | 'pubsub'>('store')
  const queryClient = useQueryClient()

  const isStoreFetching = useIsFetching({ queryKey: SNAPSHOT_KEYS.store }) > 0
  const isQueueFetching = useIsFetching({ queryKey: SNAPSHOT_KEYS.queue }) > 0
  const isStreamFetching = useIsFetching({ queryKey: SNAPSHOT_KEYS.stream }) > 0
  const isPubsubFetching = useIsFetching({ queryKey: SNAPSHOT_KEYS.pubsub }) > 0

  const onRefresh = (tab: keyof typeof SNAPSHOT_KEYS) => () => {
    queryClient.invalidateQueries({ queryKey: SNAPSHOT_KEYS[tab] })
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
                onRefresh={onRefresh('store')}
                isRefreshing={isStoreFetching}
            />
            <NavCard 
                label="QUEUE" 
                desc="Job Processing"
                active={activeTab === 'queue'}
                onClick={() => setActiveTab('queue')}
                icon={<MessageSquare className="h-5 w-5" />}
                onRefresh={onRefresh('queue')}
                isRefreshing={isQueueFetching}
            />
            <NavCard 
                label="STREAM" 
                desc="Event Log"
                active={activeTab === 'stream'}
                onClick={() => setActiveTab('stream')}
                icon={<Activity className="h-5 w-5" />}
                onRefresh={onRefresh('stream')}
                isRefreshing={isStreamFetching}
            />
            <NavCard 
                label="PUBSUB" 
                desc="Realtime Messaging"
                active={activeTab === 'pubsub'}
                onClick={() => setActiveTab('pubsub')}
                icon={<Radio className="h-5 w-5" />}
                onRefresh={onRefresh('pubsub')}
                isRefreshing={isPubsubFetching}
            />
        </div>

        {/* CONTENT AREA */}
        <main className="h-[calc(100vh-11rem)] border-t border-border pt-6 overflow-hidden">
            {activeTab === 'store' && (
                <div className="h-full">
                    <StoreView />
                </div>
            )}
            {activeTab === 'queue' && (
                <div className="h-full space-y-4 overflow-auto">
                    <QueueView />
                </div>
            )}
            {activeTab === 'stream' && (
                <div className="h-full space-y-4 overflow-auto">
                    <StreamView />
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
