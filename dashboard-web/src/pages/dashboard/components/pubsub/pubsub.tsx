import { useState, useMemo } from "react"
import { PubSubBrokerSnapshot, TopicNodeSnapshot, WildcardSubscription } from "./types"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { 
    Search, 
    Hash, 
    Radio,
    Circle,
    FileJson,
    Binary,
    Zap,
    Users
} from "lucide-react"
import { Badge } from "@/components/ui/badge"

interface Props {
  data: PubSubBrokerSnapshot
}

// Flattened Topic representation
interface FlatTopic {
    path: string;
    subscribers: number;
    retained_value: string | null;
    is_wildcard: boolean;
    client_id?: string;
}

export function PubSubView({ data }: Props) {
  const [activeTab, setActiveTab] = useState<'topics' | 'wildcards'>('topics')
  const [selectedPath, setSelectedPath] = useState<string | null>(null)
  
  const { topics, wildcards } = useMemo(() => {
      const flatTopics: FlatTopic[] = [];
      const traverse = (node: TopicNodeSnapshot, currentPath: string) => {
          const path = currentPath ? `${currentPath}/${node.name}` : node.name;
          if (node.name !== 'root') {
              // Show ALL topics, regardless of subscribers or retained value
              flatTopics.push({
                  path: node.full_path || path,
                  subscribers: node.subscribers,
                  retained_value: node.retained_value,
                  is_wildcard: false
              });
          }
          node.children.forEach((child: TopicNodeSnapshot) => traverse(child, node.name === 'root' ? '' : path));
      };
      traverse(data.topic_tree, "");

      const flatWildcards: FlatTopic[] = data.wildcard_subscriptions.map((sub: WildcardSubscription) => ({
          path: sub.pattern,
          subscribers: 1,
          retained_value: null,
          is_wildcard: true,
          client_id: sub.client_id
      }));

      return { topics: flatTopics, wildcards: flatWildcards };
  }, [data]);

  const activeList = activeTab === 'topics' ? topics : wildcards;
  
  // Find selected item details
  const selectedItem = useMemo(() => 
      [...topics, ...wildcards].find(t => t.path === selectedPath), 
  [selectedPath, topics, wildcards]);

  return (
      <div className="flex h-full gap-0 border border-slate-800 rounded bg-slate-900/20 overflow-hidden font-mono text-sm">
          
          {/* SIDEBAR */}
          <div className="w-[400px] flex flex-col border-r border-slate-800 bg-slate-950/50">
              <div className="flex border-b border-slate-800">
                  <button 
                      onClick={() => setActiveTab('topics')}
                      className={`flex-1 py-3 text-[10px] font-bold uppercase tracking-wider transition-colors ${activeTab === 'topics' ? 'bg-slate-800 text-white' : 'text-slate-500 hover:text-slate-300'}`}
                  >
                      Active Topics ({topics.length})
                  </button>
                  <div className="w-[1px] bg-slate-800" />
                  <button 
                      onClick={() => setActiveTab('wildcards')}
                      className={`flex-1 py-3 text-[10px] font-bold uppercase tracking-wider transition-colors ${activeTab === 'wildcards' ? 'bg-slate-800 text-white' : 'text-slate-500 hover:text-slate-300'}`}
                  >
                      Wildcards ({wildcards.length})
                  </button>
              </div>

              <TopicBrowser 
                  list={activeList} 
                  type={activeTab} 
                  selectedPath={selectedPath}
                  onSelect={setSelectedPath}
              />
          </div>

          {/* MAIN AREA */}
          <div className="flex-1 bg-slate-950/30 flex flex-col min-w-0">
             {selectedItem ? (
                 <div className="flex flex-col h-full">
                     <div className="p-4 border-b border-slate-800 bg-slate-900/20">
                         <div className="flex items-center gap-2 mb-2">
                             {selectedItem.is_wildcard ? (
                                 <Hash className="h-4 w-4 text-amber-500" />
                             ) : (
                                 <Circle className="h-3 w-3 text-emerald-500" />
                             )}
                             <h2 className="text-sm font-bold text-slate-100">{selectedItem.path}</h2>
                         </div>
                         <div className="flex gap-4 text-[10px] text-slate-500 uppercase tracking-wide">
                             <div className="flex items-center gap-1.5">
                                 <Users className="h-3 w-3" />
                                 {selectedItem.subscribers} SUBSCRIBERS
                             </div>
                             {selectedItem.client_id && (
                                 <div className="flex items-center gap-1.5 text-slate-400">
                                     <Zap className="h-3 w-3 text-amber-500" />
                                     CLIENT: {selectedItem.client_id}
                                 </div>
                             )}
                         </div>
                     </div>

                     {/* Retained Value Viewer */}
                     {selectedItem.retained_value ? (
                         <div className="flex-1 relative group">
                             <div className="absolute inset-0 overflow-auto p-6 scrollbar-thin">
                                <pre className="text-xs text-slate-300 font-mono leading-relaxed whitespace-pre-wrap">
                                    {tryFormatJson(selectedItem.retained_value)}
                                </pre>
                             </div>
                         </div>
                     ) : (
                         <div className="flex-1 flex flex-col items-center justify-center text-slate-700">
                             <p className="text-xs font-mono uppercase tracking-widest opacity-50">NO_RETAINED_VALUE</p>
                         </div>
                     )}

                     {/* Footer Metadata */}
                     {selectedItem.retained_value && (
                        <div className="px-4 py-2 border-t border-slate-800 bg-slate-900/20 text-[10px] text-slate-500 flex justify-between font-mono uppercase">
                            <div className="flex items-center gap-2">
                                <Binary className="h-3 w-3" />
                                <span>SIZE: {new Blob([selectedItem.retained_value]).size} BYTES</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <FileJson className="h-3 w-3" />
                                <span>UTF-8 CONTENT</span>
                            </div>
                        </div>
                     )}
                 </div>
             ) : (
                <div className="flex-1 flex flex-col items-center justify-center text-slate-700">
                    <Radio className="h-12 w-12 opacity-20 mb-4" />
                    <p className="text-xs font-mono uppercase tracking-widest opacity-50">SELECT_TOPIC</p>
                </div>
             )}
          </div>

      </div>
  )
}

function TopicBrowser({ list, type, selectedPath, onSelect }: any) {
    const [filter, setFilter] = useState("")

    const filtered = useMemo(() => {
        return list.filter((t: any) => t.path.toLowerCase().includes(filter.toLowerCase()))
    }, [list, filter])

    return (
        <div className="flex flex-col h-full">
            <div className="p-3 border-b border-slate-800 bg-slate-900/10">
                <div className="relative">
                    <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-slate-500" />
                    <Input 
                        placeholder={type === 'topics' ? "Search path..." : "Search pattern..."}
                        value={filter}
                        onChange={(e) => setFilter(e.target.value)}
                        className="h-9 pl-8 bg-slate-950 border-slate-800 text-xs font-mono placeholder:text-slate-600 focus-visible:ring-1 focus-visible:ring-slate-700 focus-visible:border-slate-700"
                    />
                </div>
            </div>

            <ScrollArea className="flex-1">
                <div className="p-0">
                    {filtered.map((item: any) => (
                        <div
                            key={item.path + (item.client_id || '')}
                            onClick={() => onSelect(item.path)}
                            className={`
                                group flex items-center justify-between px-4 py-3 border-b border-slate-800/50 cursor-pointer transition-all
                                ${selectedPath === item.path ? 'bg-slate-800' : 'hover:bg-slate-900/50'}
                            `}
                        >
                            <div className="flex flex-col gap-1 overflow-hidden">
                                <div className="flex items-center gap-2">
                                    {item.is_wildcard ? (
                                        <Hash className="h-3 w-3 text-amber-500 flex-shrink-0" />
                                    ) : (
                                        <Circle className="h-2 w-2 text-emerald-500 flex-shrink-0" />
                                    )}
                                    <span className={`font-mono text-xs truncate ${selectedPath === item.path ? 'text-white' : 'text-slate-300'}`}>
                                        {item.path}
                                    </span>
                                </div>
                                {item.client_id && (
                                    <div className="pl-5 text-[10px] text-slate-500 truncate">
                                        Client: {item.client_id}
                                    </div>
                                )}
                            </div>

                            <div className="flex items-center gap-2 flex-shrink-0 ml-4">
                                {item.retained_value && (
                                    <Badge variant="outline" className="h-4 px-1 text-[9px] border-purple-900 bg-purple-950/30 text-purple-400 rounded-sm">
                                        RET
                                    </Badge>
                                )}
                                {!item.is_wildcard && item.subscribers > 0 && (
                                    <Badge variant="secondary" className="h-4 px-1.5 text-[9px] bg-slate-900 text-slate-400 border-slate-700 rounded-sm">
                                        {item.subscribers}
                                    </Badge>
                                )}
                            </div>
                        </div>
                    ))}
                    
                    {filtered.length === 0 && (
                        <div className="py-12 text-center">
                            <p className="text-xs text-slate-600">NO_RESULTS</p>
                        </div>
                    )}
                </div>
            </ScrollArea>
        </div>
    )
}

function tryFormatJson(str: string) {
    try {
        if (str.startsWith("{") || str.startsWith("[")) {
            return JSON.stringify(JSON.parse(str), null, 2)
        }
    } catch (e) {}
    return str
}
