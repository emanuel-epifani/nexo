import { PubSubBrokerSnapshot, TopicNodeSnapshot } from "@/lib/types"
import { ChevronRight, ChevronDown, Circle, Zap, Hash } from "lucide-react"
import { useState } from "react"
import { cn } from "@/lib/utils"

interface Props {
  data: PubSubBrokerSnapshot
}

export function PubSubView({ data }: Props) {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 h-full">
        {/* Topic Tree Column */}
        <div className="lg:col-span-2 flex flex-col h-full border-r border-slate-800 pr-6">
            <div className="flex items-center justify-between mb-4">
                <h3 className="text-sm font-bold font-mono text-slate-300 uppercase tracking-wider">Topic Tree</h3>
                <span className="text-[10px] font-mono text-slate-500">{data.active_clients} CLIENTS</span>
            </div>
            
            <div className="flex-1 overflow-auto rounded border border-slate-800 bg-slate-950/30 p-2 font-mono text-sm">
                <TreeNode node={data.topic_tree} level={0} />
            </div>
        </div>

        {/* Wildcards Column */}
        <div className="flex flex-col h-full">
             <div className="flex items-center gap-2 mb-4 text-slate-300">
                <Zap className="h-3 w-3 text-amber-500" />
                <h3 className="text-sm font-bold font-mono uppercase tracking-wider">Wildcards</h3>
            </div>

            <div className="flex-1 overflow-auto space-y-2">
                {data.wildcard_subscriptions.length === 0 ? (
                    <div className="border border-dashed border-slate-800 rounded p-4 text-center text-xs text-slate-600 font-mono italic">
                        NO_WILDCARD_SUBS
                    </div>
                ) : (
                    data.wildcard_subscriptions.map((sub, i) => (
                        <div key={i} className="p-3 rounded border border-slate-800 bg-slate-900/30 text-xs">
                            <div className="flex items-center gap-2 mb-2">
                                <Hash className="h-3 w-3 text-slate-600" />
                                <code className="font-mono text-amber-500 font-bold">{sub.pattern}</code>
                            </div>
                            <div className="flex items-center gap-2 pl-5">
                                <span className="text-[10px] text-slate-600 font-mono">CLIENT:</span>
                                <span className="font-mono text-slate-400 truncate max-w-[150px]" title={sub.client_id}>{sub.client_id}</span>
                            </div>
                        </div>
                    ))
                )}
            </div>
        </div>
    </div>
  )
}

function TreeNode({ node, level }: { node: TopicNodeSnapshot, level: number }) {
    const [isOpen, setIsOpen] = useState(true)
    const hasChildren = node.children.length > 0
    
    return (
        <div className="select-none">
            <div 
                className={cn(
                    "flex items-center gap-2 py-0.5 px-2 hover:bg-slate-800/50 rounded cursor-pointer transition-colors group",
                    level > 0 && "ml-4",
                    level === 0 && "mb-1"
                )}
                onClick={() => setIsOpen(!isOpen)}
            >
                 <div className="w-4 h-4 flex items-center justify-center text-slate-600 group-hover:text-slate-400">
                    {hasChildren ? (
                        isOpen ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />
                    ) : <Circle className="h-1.5 w-1.5 opacity-30" />}
                 </div>
                 
                 <span className={cn(
                     "font-mono text-xs", 
                     node.name === "root" ? "text-slate-600 italic" : "text-slate-300"
                 )}>
                    {node.name === "root" ? "/" : node.name}
                 </span>

                 {/* Indicators */}
                 <div className="ml-auto flex items-center gap-2">
                    {node.retained_value && (
                        <span className="text-[9px] px-1 rounded bg-purple-950 text-purple-400 border border-purple-900/50 max-w-[80px] truncate" title={node.retained_value}>
                            RET
                        </span>
                    )}
                    
                    {node.subscribers > 0 && (
                        <span className="text-[9px] px-1 rounded bg-slate-800 text-slate-400 border border-slate-700">
                            {node.subscribers}
                        </span>
                    )}
                 </div>
            </div>
            
            {isOpen && hasChildren && (
                <div className="ml-2 pl-2 border-l border-slate-800">
                    {node.children.map((child, i) => (
                        <TreeNode key={child.name + i} node={child} level={level + 1} />
                    ))}
                </div>
            )}
        </div>
    )
}
