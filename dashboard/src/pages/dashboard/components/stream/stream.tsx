import { useState, useEffect } from "react"
import { useQuery } from "@tanstack/react-query"
import { StreamBrokerSnapshot, TopicSummary, ConsumerGroupSummary, StreamMessages } from "./types"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
    Search,
    Database,
    Box,
    Layers,
    RefreshCw,
    ChevronLeft,
    ChevronRight,
} from "lucide-react"

interface Props {
    data: StreamBrokerSnapshot
}

const PAGE_SIZE = 50

export function StreamView({ data }: Props) {
    const [filter, setFilter] = useState("")
    const [selectedTopicName, setSelectedTopicName] = useState<string | null>(null)
    const [selectedPartitionId, setSelectedPartitionId] = useState<number>(0)
    const [selectedMessageOffset, setSelectedMessageOffset] = useState<number | null>(null)
    const [fromOffset, setFromOffset] = useState<number | null>(null)

    // Default: select first topic
    useEffect(() => {
        if (!selectedTopicName && data.topics.length > 0) {
            setSelectedTopicName(data.topics[0].name)
        }
    }, [data.topics, selectedTopicName])

    // Reset when topic or partition changes
    useEffect(() => {
        setSelectedPartitionId(0)
        setSelectedMessageOffset(null)
        setFromOffset(null)
    }, [selectedTopicName])

    useEffect(() => {
        setSelectedMessageOffset(null)
        setFromOffset(null)
    }, [selectedPartitionId])

    const filteredTopics = data.topics.filter((t: TopicSummary) => t.name.toLowerCase().includes(filter.toLowerCase()))
    const selectedTopic = data.topics.find((t: TopicSummary) => t.name === selectedTopicName)
    const currentPartition = selectedTopic?.partitions.find(p => p.id === selectedPartitionId)

    // Fetch messages on-demand when topic+partition selected
    const { data: streamData, isLoading } = useQuery({
        queryKey: ["stream-messages", selectedTopicName, selectedPartitionId, fromOffset],
        queryFn: async (): Promise<StreamMessages> => {
            const params = new URLSearchParams({ limit: String(PAGE_SIZE) })
            if (fromOffset !== null) params.set("from", String(fromOffset))
            const res = await fetch(`/api/stream/${selectedTopicName}/${selectedPartitionId}/messages?${params}`)
            if (!res.ok) throw new Error("Failed to fetch messages")
            return res.json()
        },
        enabled: !!selectedTopicName,
    })

    const messages = streamData?.messages ?? []
    const lastOffset = streamData?.last_offset ?? currentPartition?.last_offset ?? 0
    const currentFrom = streamData?.from_offset ?? 0
    const hasOlder = currentFrom > 0
    const hasNewer = streamData ? currentFrom + PAGE_SIZE < lastOffset : false

    const selectedMessage = messages.find(m => m.offset === selectedMessageOffset)

    const hasTopics = data.topics.length > 0

    return (
        <div className="flex h-full gap-0 border-2 border-border rounded-sm bg-panel overflow-hidden font-mono text-sm">

            {/* COL 1: Topics */}
            <div className="w-[220px] flex flex-col border-r-2 border-border bg-sidebar shrink-0">
                <div className="p-3 border-b-2 border-border">
                    <div className="relative">
                        <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                        <Input
                            placeholder="FILTER TOPICS..."
                            value={filter}
                            onChange={(e) => setFilter(e.target.value)}
                            className="h-8 pl-8 bg-background border-border text-xs font-mono placeholder:text-muted-foreground focus-visible:ring-1 focus-visible:ring-ring"
                        />
                    </div>
                </div>
                <ScrollArea className="flex-1">
                    <div className="p-0">
                        {filteredTopics.map((t: TopicSummary) => (
                            <div
                                key={t.name}
                                onClick={() => setSelectedTopicName(t.name)}
                                className={`
                                    group flex items-center justify-between px-3 py-2.5 border-b border-border/30 cursor-pointer transition-all
                                    ${selectedTopicName === t.name ? 'bg-secondary border-l-2 border-l-primary' : 'hover:bg-muted/50 border-l-2 border-l-transparent'}
                                `}
                            >
                                <div className="flex items-center gap-2 overflow-hidden">
                                    <Box className={`h-3.5 w-3.5 shrink-0 ${selectedTopicName === t.name ? 'text-primary' : 'text-muted-foreground'}`} />
                                    <span className={`font-mono text-xs truncate ${selectedTopicName === t.name ? 'text-foreground font-medium' : 'text-muted-foreground'}`}>
                                        {t.name}
                                    </span>
                                </div>
                                <span className="text-xs text-muted-foreground font-mono">
                                    {t.partitions.length}P
                                </span>
                            </div>
                        ))}
                    </div>
                </ScrollArea>
            </div>

            {/* COL 2: Partitions */}
            <div className="w-[200px] flex flex-col border-r-2 border-border bg-sidebar shrink-0">
                <div className="px-3 py-2 border-b-2 border-border bg-section-header">
                    <span className="text-xs text-muted-foreground uppercase font-bold tracking-wider">Partitions</span>
                </div>
                {hasTopics && selectedTopic ? (
                    <ScrollArea className="flex-1">
                        <div className="p-0">
                            {selectedTopic.partitions.map((p) => (
                                <div
                                    key={p.id}
                                    onClick={() => setSelectedPartitionId(p.id)}
                                    className={`
                                        flex items-center justify-between px-3 py-2.5 border-b border-border/30 cursor-pointer transition-all
                                        ${selectedPartitionId === p.id ? 'bg-secondary border-l-2 border-l-primary' : 'hover:bg-muted/50 border-l-2 border-l-transparent'}
                                    `}
                                >
                                    <div className="flex items-center gap-2">
                                        <Layers className={`h-3.5 w-3.5 ${selectedPartitionId === p.id ? 'text-primary' : 'text-muted-foreground'}`} />
                                        <span className={`font-mono text-xs ${selectedPartitionId === p.id ? 'text-foreground font-medium' : 'text-muted-foreground'}`}>
                                            {p.id}
                                        </span>
                                    </div>
                                    <span className="text-xs text-muted-foreground font-mono">
                                        {p.last_offset} msgs
                                    </span>
                                </div>
                            ))}
                        </div>
                    </ScrollArea>
                ) : (
                    <div className="flex-1" />
                )}
            </div>

            {/* COL 3: Messages + Payload */}
            <div className="flex-1 flex min-w-0 bg-content">
                {hasTopics && selectedTopic && currentPartition ? (
                    <>
                        {/* Messages Log */}
                        <div className="flex-1 flex flex-col min-w-0 border-r-2 border-border">
                            {/* Header: stats + groups */}
                            <div className="px-5 py-3 border-b-2 border-border bg-section-header">
                                <div className="text-sm text-muted-foreground font-mono font-medium pb-4">
                                    Total messages: {currentPartition.last_offset}
                                </div>
                                <div className="text-sm text-muted-foreground font-mono font-medium">Consumer groups:</div>
                                {currentPartition.groups.length > 0 && (
                                    <div className="space-y-1">
                                        {currentPartition.groups.map((group: ConsumerGroupSummary) => (
                                            <div key={group.id} className="grid grid-cols-3 gap-4 text-sm text-muted-foreground font-mono">
                                                <div className="truncate ml-6">- {group.id}</div>
                                                <div className="text-right text-muted-foreground">
                                                    Progress: {group.committed_offset}/{currentPartition.last_offset}
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </div>

                            {/* Events Log Header */}
                            <div className="px-5 py-2 border-b border-border bg-section-header shrink-0">
                                <span className="text-xs font-bold uppercase tracking-wider text-muted-foreground">Events Log</span>
                            </div>

                            {/* Table Header */}
                            <div className="flex items-center px-5 py-2 border-b border-border bg-section-header text-xs font-mono text-muted-foreground uppercase shrink-0 tracking-wider">
                                <div className="w-16 shrink-0">Offset</div>
                                <div className="flex-1">Timestamp</div>
                            </div>

                            {/* Messages List */}
                            <div className="flex-1 overflow-y-auto">
                                {isLoading ? (
                                    <div className="flex flex-col items-center justify-center h-full text-muted-foreground">
                                        <RefreshCw className="h-6 w-6 animate-spin mb-4 opacity-50" />
                                        <p className="text-xs font-mono uppercase tracking-widest opacity-50">LOADING...</p>
                                    </div>
                                ) : (
                                    messages.map((msg) => {
                                        const isSelected = selectedMessageOffset === msg.offset
                                        const timeStr = new Date(msg.timestamp).toLocaleTimeString('it-IT', {
                                            hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit'
                                        })
                                        return (
                                            <div
                                                key={msg.offset}
                                                onClick={() => setSelectedMessageOffset(msg.offset)}
                                                className={`
                                                    flex items-center px-5 h-8 border-b border-border/50 cursor-pointer transition-colors
                                                    ${isSelected ? 'bg-primary/10 border-primary/30' : 'hover:bg-muted/40'}
                                                `}
                                            >
                                                <div className={`w-16 shrink-0 font-mono text-xs ${isSelected ? 'text-primary font-bold' : 'text-muted-foreground'}`}>
                                                    {msg.offset}
                                                </div>
                                                <div className="flex-1 font-mono text-xs text-muted-foreground">
                                                    {timeStr}
                                                </div>
                                            </div>
                                        )
                                    })
                                )}
                            </div>

                            {/* Pagination */}
                            <div className="p-2 border-t-2 border-border bg-section-header flex items-center justify-between flex-shrink-0">
                                <div className="text-xs text-muted-foreground font-mono">
                                    {lastOffset > 0 ? (
                                        <span>
                                            {currentFrom}-{Math.min(currentFrom + PAGE_SIZE - 1, lastOffset - 1)} of {lastOffset}
                                        </span>
                                    ) : (
                                        <span>0 messages</span>
                                    )}
                                </div>
                                <div className="flex gap-1">
                                    <button
                                        disabled={!hasOlder || isLoading}
                                        onClick={() => setFromOffset(Math.max(0, currentFrom - PAGE_SIZE))}
                                        className="h-7 px-2 flex items-center gap-1 text-xs font-mono uppercase bg-background border border-border rounded hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                                    >
                                        <ChevronLeft className="h-3 w-3" />
                                        Older
                                    </button>
                                    <button
                                        disabled={!hasNewer || isLoading}
                                        onClick={() => setFromOffset(currentFrom + PAGE_SIZE)}
                                        className="h-7 px-2 flex items-center gap-1 text-xs font-mono uppercase bg-background border border-border rounded hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                                    >
                                        Newer
                                        <ChevronRight className="h-3 w-3" />
                                    </button>
                                </div>
                            </div>
                        </div>

                        {/* Payload Inspector */}
                        <div className="w-[420px] bg-content flex flex-col shadow-xl border-l border-border shrink-0">
                            <div className="px-5 py-3 border-b border-border bg-section-header flex items-center justify-between shrink-0">
                                <span className="text-xs text-muted-foreground uppercase tracking-wider font-bold">Payload</span>
                                {selectedMessage && (
                                    <span className="text-xs font-mono text-muted-foreground">Offset {selectedMessage.offset}</span>
                                )}
                            </div>
                            <ScrollArea className="flex-1">
                                <div className="p-5">
                                    {selectedMessage ? (
                                        <pre className="font-mono text-sm leading-relaxed text-foreground whitespace-pre-wrap break-all">
                                            {JSON.stringify(selectedMessage.payload, null, 2)}
                                        </pre>
                                    ) : (
                                        <div className="flex flex-col items-center justify-center h-40 text-muted-foreground mt-10">
                                            <p className="text-xs text-center opacity-60">Select a message</p>
                                        </div>
                                    )}
                                </div>
                            </ScrollArea>
                        </div>
                    </>
                ) : (
                    <div className="flex-1 flex flex-col items-center justify-center text-muted-foreground">
                        <Database className="h-10 w-10 mb-3 opacity-20" />
                        <p className="text-xs font-mono uppercase tracking-widest opacity-60">SELECT A PARTITION</p>
                    </div>
                )}
            </div>
        </div>
    )
}
