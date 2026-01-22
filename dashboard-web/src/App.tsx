import { useState } from 'react'
import { QueryClient, QueryClientProvider, useQueries } from '@tanstack/react-query'
import { StoreView } from '@/components/views/store-view'
import { StreamView } from '@/components/views/stream-view'
import { PubSubView } from '@/components/views/pubsub-view'
import { QueueList } from '@/components/dashboard/queue-list'
import { 
    Loader2, 
    ServerCrash, 
    Database, 
    MessageSquare, 
    Radio, 
    Activity,
    RefreshCw
} from 'lucide-react'

const queryClient = new QueryClient()

export default function App() {
    return (
        <QueryClientProvider client={queryClient}>
            <Dashboard />
        </QueryClientProvider>
    )
}

function Dashboard() {
  const [activeTab, setActiveTab] = useState<'store' | 'queue' | 'stream' | 'pubsub'>('store')

  const [storeQuery, queueQuery, streamQuery, pubsubQuery] = useQueries({
    queries: [
      {
        queryKey: ['store-snapshot'],
        queryFn: async () => {
          const response = await fetch('/api/store')
          if (!response.ok) throw new Error('Failed to fetch store')
          return response.json()
        },
      },
      {
        queryKey: ['queue-snapshot'],
        queryFn: async () => {
          const response = await fetch('/api/queue')
          if (!response.ok) throw new Error('Failed to fetch queue')
          return response.json()
        },
      },
      {
        queryKey: ['stream-snapshot'],
        queryFn: async () => {
          const response = await fetch('/api/stream')
          if (!response.ok) throw new Error('Failed to fetch stream')
          return response.json()
        },
      },
      {
        queryKey: ['pubsub-snapshot'],
        queryFn: async () => {
          const response = await fetch('/api/pubsub')
          if (!response.ok) throw new Error('Failed to fetch pubsub')
          return response.json()
        },
      },
    ],
  })

  const isLoading = storeQuery.isLoading || queueQuery.isLoading || streamQuery.isLoading || pubsubQuery.isLoading
  const hasError = storeQuery.error || queueQuery.error || streamQuery.error || pubsubQuery.error

  if (isLoading) {
    return (
      <div className="flex h-screen items-center justify-center bg-slate-950 font-mono text-sm">
        <div className="flex items-center gap-2 text-slate-400">
            <Loader2 className="h-4 w-4 animate-spin" />
            <span>CONNECTING_TO_BROKER...</span>
        </div>
      </div>
    )
  }

  if (hasError) {
    return (
      <div className="flex h-screen flex-col items-center justify-center gap-4 bg-slate-950 text-slate-200">
        <ServerCrash className="h-12 w-12 text-rose-500" />
        <h2 className="text-xl font-mono">CONNECTION_LOST</h2>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-screen bg-slate-950 text-slate-300 font-sans selection:bg-slate-700 selection:text-white">
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
                onRefresh={() => storeQuery.refetch()}
                isRefreshing={storeQuery.isFetching}
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
                onRefresh={() => pubsubQuery.refetch()}
                isRefreshing={pubsubQuery.isFetching}
            />
        </div>

        {/* CONTENT AREA */}
        <main className="flex-1 border-t border-slate-800/50 pt-6 pb-6 overflow-hidden">
            {activeTab === 'store' && storeQuery.data && <StoreView data={storeQuery.data} />}
            {activeTab === 'queue' && queueQuery.data && (
                <div className="h-full space-y-4 overflow-auto">
                    <QueueList data={queueQuery.data} />
                </div>
            )}
            {activeTab === 'stream' && streamQuery.data && (
                <div className="h-full space-y-4 overflow-auto">
                    <StreamView data={streamQuery.data} />
                </div>
            )}
            {activeTab === 'pubsub' && pubsubQuery.data && (
                <div className="h-full space-y-4 overflow-auto">
                    <PubSubView data={pubsubQuery.data} />
                </div>
            )}
        </main>
      </div>
    </div>
  )
}

function NavCard({ label, desc, active, onClick, icon, onRefresh, isRefreshing }: any) {
    return (
        <div
            onClick={onClick}
            className={`
                group relative flex items-center h-20 w-full rounded-sm border transition-all duration-200 cursor-pointer overflow-hidden
                ${active
                ? 'bg-slate-800 border-slate-600 shadow-md'
                : 'bg-slate-900/40 border-slate-800 hover:bg-slate-800/40 hover:border-slate-700'
            }
            `}
        >
            {/* CONTENT AREA */}
            <div className="flex flex-1 items-center gap-4 px-6 h-full">
                <div className={`
                    p-2.5 rounded-full transition-colors shrink-0
                    ${active ? 'bg-indigo-500/10 text-indigo-400' : 'bg-slate-900 text-slate-600 group-hover:text-slate-500'}
                `}>
                    {icon}
                </div>

                <div className="flex flex-col items-start text-left min-w-0">
                    <span className={`text-base font-mono font-bold tracking-tight leading-tight ${active ? 'text-white' : 'text-slate-400'}`}>
                        {label}
                    </span>
                    <span className="text-[10px] font-mono text-slate-600 uppercase tracking-wider truncate">
                        {desc}
                    </span>
                </div>
            </div>

            {/* REFRESH STRIP (Vertical button) */}
            <button
                onClick={(e) => {
                    e.stopPropagation()
                    onRefresh()
                }}
                disabled={isRefreshing}
                className={`
                    h-full w-12 flex items-center justify-center border-l transition-colors outline-none
                    ${active
                    ? 'border-slate-700 hover:bg-slate-700 text-slate-400 hover:text-white'
                    : 'border-slate-800 hover:bg-slate-800 text-slate-600 hover:text-slate-400'
                }
                `}
            >
                <RefreshCw className={`h-4 w-4 ${isRefreshing ? 'animate-spin' : ''}`} />
            </button>

            {/* Active Indicator */}
            {active && (
                <div className="absolute bottom-0 left-0 right-0 h-[2px] bg-indigo-500" />
            )}
        </div>
    )
}

