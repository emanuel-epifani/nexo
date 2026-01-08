import { useState } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { useSystemState } from '@/hooks/use-system-state'
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
    RefreshCw,
    Clock,
    Terminal
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { SystemSnapshot } from '@/lib/types'

const queryClient = new QueryClient()

export default function App() {
    return (
        <QueryClientProvider client={queryClient}>
            <Dashboard />
        </QueryClientProvider>
    )
}

function Dashboard() {
  const { data: serverData, isLoading, error, refetch } = useSystemState()
  const [activeTab, setActiveTab] = useState<'store' | 'queue' | 'stream' | 'pubsub'>('store')

  // MOCK DATA for preview
  const mockData: SystemSnapshot = {
      uptime_seconds: 1234,
      server_time: new Date().toISOString(),
      brokers: {
          store: { total_keys: 1542, expiring_keys: 120, keys: [] },
          queue: { queues: [{ name: "email_notif", pending_count: 42, inflight_count: 5, scheduled_count: 0, consumers_waiting: 2, messages: [] }] },
          stream: { total_topics: 5, total_active_groups: 2, topics: [] },
          pubsub: { active_clients: 89, wildcard_subscriptions: [], topic_tree: { name: "root", full_path: "", subscribers: 0, retained_value: null, children: [] } }
      }
  }

  const data = serverData || mockData

  if (isLoading) {
    return (
      <div className="flex h-screen items-center justify-center bg-slate-950 font-mono text-sm">
        <div className="flex items-center gap-2 text-slate-400">
            <Loader2 className="h-4 w-4 animate-spin" />
            <span>CONNECTING_TO_KERNEL...</span>
        </div>
      </div>
    )
  }

  if (error && !data) {
    return (
      <div className="flex h-screen flex-col items-center justify-center gap-4 bg-slate-950 text-slate-200">
        <ServerCrash className="h-12 w-12 text-rose-500" />
        <h2 className="text-xl font-mono">CONNECTION_LOST</h2>
        <Button variant="outline" onClick={() => refetch()} className="border-slate-800 hover:bg-slate-900 font-mono">
            RETRY_CONNECTION
        </Button>
      </div>
    )
  }

  // --- CALCOLO METRICHE PER LE CARD ---
  const queueCount = data.brokers.queue.queues.length
  const pendingTotal = data.brokers.queue.queues.reduce((acc, q) => acc + q.pending_count, 0)
  
  const streamTopics = data.brokers.stream.total_topics
  const streamGroups = data.brokers.stream.total_active_groups // O calcolato dai topic se preferisci
  
  const pubsubClients = data.brokers.pubsub.active_clients
  const pubsubSubs = data.brokers.pubsub.wildcard_subscriptions.length // Questo è parziale, ma è un indicatore

  return (
    <div className="min-h-screen bg-slate-950 text-slate-300 font-sans selection:bg-slate-700 selection:text-white">
        
      {/* HEADER */}
      <header className="sticky top-0 z-50 w-full border-b border-slate-800 bg-slate-950/95 backdrop-blur">
        <div className="max-w-[1600px] mx-auto px-6 h-14 flex items-center justify-between">
            <div className="flex items-center gap-4">
                <div className="flex items-center gap-2 text-slate-100">
                    <Terminal className="h-5 w-5" />
                    <span className="font-mono font-bold tracking-tight">NEXO </span>
                </div>
                <div className="h-4 w-[1px] bg-slate-800" />
                <span className="text-xs font-mono text-slate-500">v0.1.0-beta</span>
            </div>

            <div className="flex items-center gap-6 text-xs font-mono">
                <div className="flex items-center gap-2 text-slate-400">
                    <Clock className="h-3.5 w-3.5" />
                    <span>UPTIME: {formatUptime(data.uptime_seconds)}</span>
                </div>
                <div className="flex items-center gap-2">
                    <div className={`h-2 w-2 rounded-full ${serverData ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.4)]' : 'bg-rose-500'}`} />
                    <span className={serverData ? 'text-emerald-500' : 'text-rose-500'}>
                        {serverData ? 'SYSTEM_OPERATIONAL' : 'SYSTEM_OFFLINE'}
                    </span>
                </div>
                 <Button variant="ghost" size="sm" onClick={() => refetch()} className="h-8 w-8 p-0 hover:bg-slate-800 text-slate-500 hover:text-slate-300">
                    <RefreshCw className="h-4 w-4" />
                </Button>
            </div>
        </div>
      </header>

      {/* MAIN CONTAINER */}
      <div className="max-w-[1600px] mx-auto p-6 space-y-6">
        
        {/* KEY METRICS GRID - MAIN + SUB LOGIC */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <MetricCard 
                label="STORE (cache in memory)"
                mainValue={data.brokers.store.total_keys}
                subValue="KEYS (HASHMAP)"
                active={activeTab === 'store'}
                onClick={() => setActiveTab('store')}
                icon={<Database className="h-6 w-4" />}
            />
            <MetricCard 
                label="QUEUE (Job)"
                mainValue={queueCount}
                subValue={`${pendingTotal} PENDING MSGS`}
                active={activeTab === 'queue'}
                onClick={() => setActiveTab('queue')}
                icon={<MessageSquare className="h-6 w-4" />}
            />
            <MetricCard 
                label="STREAM (Event Log)"
                mainValue={streamTopics}
                subValue={`${streamGroups} CONS. GROUPS`}
                active={activeTab === 'stream'}
                onClick={() => setActiveTab('stream')}
                icon={<Activity className="h-6 w-4" />}
            />
            <MetricCard 
                label="PUBSUB (Realtime)"
                mainValue={pubsubClients}
                subValue={`${pubsubSubs} WILDCARD SUBS`}
                active={activeTab === 'pubsub'}
                onClick={() => setActiveTab('pubsub')}
                icon={<Radio className="h-6 w-4" />}
            />
        </div>

        {/* CONTENT AREA */}
        <main className="min-h-[600px] border-t border-slate-800/50 pt-6">
            {activeTab === 'store' && <StoreView data={data.brokers.store} />}
            {activeTab === 'queue' && (
                <div className="space-y-4">
                    <QueueList data={data.brokers.queue} />
                </div>
            )}
            {activeTab === 'stream' && (
                <div className="space-y-4">
                    <StreamView data={data.brokers.stream} />
                </div>
            )}
            {activeTab === 'pubsub' && (
                <div className="rounded border border-slate-800 bg-slate-900/30 p-6 min-h-[400px]">
                    <PubSubView data={data.brokers.pubsub} />
                </div>
            )}
        </main>
      </div>
    </div>
  )
}

function MetricCard({ label, mainValue, subValue, active, onClick, icon }: any) {
    return (
        <button 
            onClick={onClick}
            className={`
                flex flex-col justify-between p-4 rounded-sm border transition-all duration-200 text-left h-24
                ${active 
                    ? 'bg-slate-800 border-slate-600 shadow-sm' 
                    : 'bg-slate-900/50 border-slate-800 hover:bg-slate-800/50 hover:border-slate-700'
                }
            `}
        >
            <div className="flex items-center justify-between w-full text-slate-500 text-[10px] font-mono uppercase tracking-wider">
                <span>{label}</span>
                {icon}
            </div>
            
            <div className="flex flex-col items-start gap-0.5">
                <span className={`text-2xl font-mono font-bold tracking-tight leading-none ${active ? 'text-white' : 'text-slate-300'}`}>
                    {mainValue.toLocaleString()}
                </span>
                <span className="text-[10px] font-mono text-slate-500 uppercase">
                    {subValue}
                </span>
            </div>
        </button>
    )
}

function formatUptime(seconds: number): string {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    return `${h}h ${m}m ${s}s`;
}
