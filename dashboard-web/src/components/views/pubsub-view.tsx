import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { PubSubBrokerSnapshot, TopicNodeSnapshot } from "@/lib/types"
import { Badge } from "@/components/ui/badge"
import { ChevronRight, ChevronDown, Circle } from "lucide-react"
import { useState } from "react"
import { cn } from "@/lib/utils"

interface Props {
  data: PubSubBrokerSnapshot
}

export function PubSubView({ data }: Props) {
  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-bold tracking-tight">Pub/Sub Topics</h2>
      <Card>
        <CardHeader>
           <CardTitle>Topic Tree ({data.active_clients} clients connected)</CardTitle>
        </CardHeader>
        <CardContent>
             <div className="border rounded-md p-4 bg-muted/20">
                <TreeNode node={data.topic_tree} level={0} />
             </div>
        </CardContent>
      </Card>
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
                    "flex items-center gap-2 py-1 hover:bg-muted/50 rounded cursor-pointer transition-colors",
                    level > 0 && "ml-4"
                )}
                onClick={() => setIsOpen(!isOpen)}
            >
                 <div className="w-4 h-4 flex items-center justify-center">
                    {hasChildren ? (
                        isOpen ? <ChevronDown className="h-4 w-4 text-muted-foreground" /> : <ChevronRight className="h-4 w-4 text-muted-foreground" />
                    ) : <Circle className="h-2 w-2 text-muted-foreground/50" />}
                 </div>
                 
                 <span className={cn("font-mono font-medium", node.name === "root" && "text-muted-foreground italic")}>
                    {node.name === "root" ? "/" : node.name}
                 </span>

                 {node.retained_msg && (
                    <Badge variant="secondary" className="text-[10px] h-4">Retained</Badge>
                 )}
                 
                 {node.subscribers > 0 && (
                     <Badge variant="outline" className="text-[10px] h-4 bg-blue-50 text-blue-700 border-blue-200">
                        {node.subscribers} sub
                     </Badge>
                 )}
            </div>
            
            {isOpen && hasChildren && (
                <div className="ml-2 border-l pl-2">
                    {node.children.map((child, i) => (
                        <TreeNode key={child.name + i} node={child} level={level + 1} />
                    ))}
                </div>
            )}
        </div>
    )
}

