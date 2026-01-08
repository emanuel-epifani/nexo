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

// 1. Setup QueryClient outside component to avoid recreation on render
const queryClient = new QueryClient()

// 2. Main Entry Point wrapper
export default function App() {
    return (
        <QueryClientProvider client={queryClient}>
            <Dashboard />
        </QueryClientProvider>
    )
}

// 3. The actual Dashboard Logic (child of Provider)
function Dashboard() {
  const { data: serverData, isLoading, error, refetch } = useSystemState()
  const [activeTab, setActiveTab] = useState<'store' | 'queue' | 'stream' | 'pubsub'>('store')

  // MOCK DATA for preview if server is down
  const mockData: SystemSnapshot = {
      uptime_seconds: 1234,
      server_time: new Date().toISOString(),
      brokers: {
          store: { 
              total_keys: 1542, 
              expiring_keys: 120, 
              keys: [
                  { key: "users:1001:profile", value_preview: '{"id":1001, "name":"Alice", "role":"admin"}', created_at: new Date().toISOString(), expires_at: null },
                  { key: "users:1001:sessions:web", value_preview: "sess_xyz123", created_at: new Date().toISOString(), expires_at: new Date(Date.now() + 3600000).toISOString() },
                  { key: "config:app:theme", value_preview: "dark", created_at: new Date().toISOString(), expires_at: null },
                  { key: "queue:jobs:list", value_preview: "[1,2,3]", created_at: new Date().toISOString(), expires_at: null }
              ] 
          },
          queue: { 
              queues: [
                  { 
                      name: "email_notifications", 
                      pending_count: 42, 
                      inflight_count: 5, 
                      scheduled_count: 0, 
                      consumers_waiting: 2,
                      messages: [
                          { id: "msg-1", payload_preview: '{"email": "test@test.com"}', state: 'Pending', priority: 1, attempts: 0, next_delivery_at: null },
                          { id: "msg-2", payload_preview: '{"email": "bob@test.com"}', state: 'InFlight', priority: 2, attempts: 1, next_delivery_at: new Date().toISOString() }
                      ]
                  },
                  { 
                      name: "image_processing", 
                      pending_count: 0, 
                      inflight_count: 0, 
                      scheduled_count: 0, 
                      consumers_waiting: 1,
                      messages: []
                  }
              ] 
          },
          stream: { 
              total_topics: 5, 
              total_active_groups: 2, 
              topics: [
                  { 
                      name: "orders.created", 
                      total_messages: 15420, 
                      consumer_groups: [
                          { 
                              name: "billing-service", 
                              members: [
                                  { client_id: "billing-1", current_offset: 15400, lag: 20 },
                                  { client_id: "billing-2", current_offset: 15420, lag: 0 }
                              ] 
                          }
                      ] 
                  }
              ] 
          },
          pubsub: { 
              active_clients: 89, 
              wildcard_subscriptions: [{ pattern: "home/#", client_id: "client-monitor" }],
              topic_tree: { 
                  name: "root",
                  full_path: "", 
                  subscribers: 0, 
                  retained_value: null, 
                  children: [
                      { 
                          name: "sensors", 
                          full_path: "sensors",
                          subscribers: 0, 
                          retained_value: null, 
                          children: [
                              { name: "temp", full_path: "sensors/temp", subscribers: 12, retained_value: "23.5", children: [] },
                              { name: "humidity", full_path: "sensors/humidity", subscribers: 8, retained_value: null, children: [] }
                          ] 
                      }
                  ] 
              } 
          }
      }
  }

  const data = serverData || mockData
  // const isMock = !serverData && !isLoading

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

  return (
    <div className="min-h-screen bg-slate-950 text-slate-300 font-sans selection:bg-slate-700 selection:text-white">
        
      {/* HEADER - Technical & Utilitarian */}
      <header className="sticky top-0 z-50 w-full border-b border-slate-800 bg-slate-950/95 backdrop-blur supports-[backdrop-filter]:bg-slate-950/80">
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
                {/* Uptime */}
                <div className="flex items-center gap-2 text-slate-400">
                    <Clock className="h-3.5 w-3.5" />
                    <span>UPTIME: {formatUptime(data.uptime_seconds)}</span>
                </div>

                {/* Status Indicator */}
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

      {/* MAIN CONTAINER - Wide & Dense */}
      <div className="max-w-[1600px] mx-auto p-6 space-y-6">
        
        {/* KEY METRICS GRID - Dense Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <MetricCard 
                label="STORE_KEYS" 
                value={data.brokers.store.total_keys}
                active={activeTab === 'store'}
                onClick={() => setActiveTab('store')}
                icon={<Database className="h-4 w-4" />}
            />
            <MetricCard 
                label="PENDING_JOBS" 
                value={data.brokers.queue.queues.reduce((acc, q) => acc + q.pending_count, 0)}
                active={activeTab === 'queue'}
                onClick={() => setActiveTab('queue')}
                icon={<MessageSquare className="h-4 w-4" />}
            />
            <MetricCard 
                label="ACTIVE_TOPICS" 
                value={data.brokers.stream.total_topics}
                active={activeTab === 'stream'}
                onClick={() => setActiveTab('stream')}
                icon={<Activity className="h-4 w-4" />}
            />
            <MetricCard 
                label="CONNECTED_CLIENTS" 
                value={data.brokers.pubsub.active_clients}
                active={activeTab === 'pubsub'}
                onClick={() => setActiveTab('pubsub')}
                icon={<Radio className="h-4 w-4" />}
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

function MetricCard({ label, value, active, onClick, icon }: any) {
    return (
        <button 
            onClick={onClick}
            className={`
                flex flex-col gap-2 p-4 rounded-sm border transition-all duration-200 text-left
                ${active 
                    ? 'bg-slate-800 border-slate-600 shadow-sm' 
                    : 'bg-slate-900/50 border-slate-800 hover:bg-slate-800/50 hover:border-slate-700'
                }
            `}
        >
            <div className="flex items-center justify-between w-full text-slate-500 text-xs font-mono uppercase tracking-wider">
                <span>{label}</span>
                {icon}
            </div>
            <span className={`text-2xl font-mono font-bold tracking-tight ${active ? 'text-white' : 'text-slate-300'}`}>
                {value.toLocaleString()}
            </span>
        </button>
    )
}

function formatUptime(seconds: number): string {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    return `${h}h ${m}m ${s}s`;
}
