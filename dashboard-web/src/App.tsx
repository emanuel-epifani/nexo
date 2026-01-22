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
    Activity
} from 'lucide-react'
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
  const { data: serverData, isLoading, error } = useSystemState()
  const [activeTab, setActiveTab] = useState<'store' | 'queue' | 'stream' | 'pubsub'>('store')

  // MOCK DATA for preview
  const mockData: SystemSnapshot = {
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
            <span>CONNECTING_TO_BROKER...</span>
        </div>
      </div>
    )
  }

  if (error && !data) {
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
            />
            <NavCard 
                label="QUEUE" 
                desc="Job Processing"
                active={activeTab === 'queue'}
                onClick={() => setActiveTab('queue')}
                icon={<MessageSquare className="h-5 w-5" />}
            />
            <NavCard 
                label="STREAM" 
                desc="Event Log"
                active={activeTab === 'stream'}
                onClick={() => setActiveTab('stream')}
                icon={<Activity className="h-5 w-5" />}
            />
            <NavCard 
                label="PUBSUB" 
                desc="Realtime Messaging"
                active={activeTab === 'pubsub'}
                onClick={() => setActiveTab('pubsub')}
                icon={<Radio className="h-5 w-5" />}
            />
        </div>

        {/* CONTENT AREA */}
        <main className="flex-1 border-t border-slate-800/50 pt-6 pb-6 pr-6 overflow-hidden">
            {activeTab === 'store' && <StoreView data={data.brokers.store} />}
            {activeTab === 'queue' && (
                <div className="h-full space-y-4 overflow-auto">
                    <QueueList data={data.brokers.queue} />
                </div>
            )}
            {activeTab === 'stream' && (
                <div className="h-full space-y-4 overflow-auto">
                    <StreamView data={data.brokers.stream} />
                </div>
            )}
            {activeTab === 'pubsub' && (
                <div className="h-full space-y-4 overflow-auto">
                    <PubSubView data={data.brokers.pubsub} />
                </div>
            )}
        </main>
      </div>
    </div>
  )
}

function NavCard({ label, desc, active, onClick, icon }: any) {
    return (
        <button 
            onClick={onClick}
            className={`
                group relative flex items-center gap-4 px-6 rounded-sm border transition-all duration-200 h-20 w-full
                ${active 
                    ? 'bg-slate-800 border-slate-600 shadow-md' 
                    : 'bg-slate-900/40 border-slate-800 hover:bg-slate-800/40 hover:border-slate-700'
                }
            `}
        >
            {/* ICONA A SINISTRA (OVALE) */}
            <div className={`
                p-2.5 rounded-full transition-colors shrink-0
                ${active ? 'bg-indigo-500/10 text-indigo-400' : 'bg-slate-900 text-slate-600 group-hover:text-slate-500'}
            `}>
                {icon}
            </div>
            
            {/* TESTO A DESTRA (allineato a sinistra) */}
            <div className="flex flex-col items-start text-left min-w-0">
                <span className={`text-base font-mono font-bold tracking-tight leading-tight ${active ? 'text-white' : 'text-slate-400'}`}>
                    {label}
                </span>
                <span className="text-[10px] font-mono text-slate-600 uppercase tracking-wider truncate w-full">
                    {desc}
                </span>
            </div>

            {/* Active Indicator (Barra inferiore centrata) */}
            {active && (
                <div className="absolute bottom-0 left-6 right-6 h-[2px] bg-indigo-500 rounded-b-sm" />
            )}
        </button>
    )
}

