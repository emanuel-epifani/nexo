import { useState, useMemo, useEffect, useRef } from "react"
import { useVirtualizer } from '@tanstack/react-virtual'
import { StreamBrokerSnapshot, TopicSummary, ConsumerGroupSummary } from "./types"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
    Search,
    Database,
    Box,
    Layers,
    ArrowRight
} from "lucide-react"

interface Props {
    data: StreamBrokerSnapshot
}

export function StreamView({ data }: Props) {
    const [filter, setFilter] = useState("")
    const [selectedTopicName, setSelectedTopicName] = useState<string | null>(null)
    const [selectedPartitionId, setSelectedPartitionId] = useState<number>(0)
    const [selectedMessageOffset, setSelectedMessageOffset] = useState<number | null>(null)

    // Default selection
    useEffect(() => {
        if (!selectedTopicName && data.topics.length > 0) {
            setSelectedTopicName(data.topics[0].name)
        }
    }, [data.topics, selectedTopicName])

    // Reset partition when topic changes
    useEffect(() => {
        setSelectedPartitionId(0)
        setSelectedMessageOffset(null)
    }, [selectedTopicName])

    const filteredTopics = useMemo(() => {
        return data.topics.filter((t: TopicSummary) => t.name.toLowerCase().includes(filter.toLowerCase()))
    }, [data.topics, filter])

    const selectedTopic = useMemo(() =>
            data.topics.find((t: TopicSummary) => t.name === selectedTopicName),
        [data.topics, selectedTopicName])

    const currentPartition = useMemo(() =>
            selectedTopic?.partitions.find(p => p.id === selectedPartitionId),
        [selectedTopic, selectedPartitionId])

    const selectedMessage = useMemo(() =>
            currentPartition?.messages.find(m => m.offset === selectedMessageOffset),
        [currentPartition, selectedMessageOffset])

    // Virtualizer for messages table
    const parentRef = useRef<HTMLDivElement>(null)

    const rowVirtualizer = useVirtualizer({
        count: currentPartition?.messages.length ?? 0,
        getScrollElement: () => parentRef.current,
        estimateSize: () => 32,
        overscan: 5,
    })

    const hasTopics = data.topics.length > 0

    return (
        <div className="flex h-full gap-0 border-2 border-border rounded-sm bg-panel overflow-hidden font-mono text-sm">

            {/* COL 1: SIDEBAR Topics List */}
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
                                    <Box className={`h-3.5 w-3.5 ${selectedTopicName === t.name ? 'text-primary' : 'text-muted-foreground'}`} />
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

            {/* COL 2: PARTITIONS LIST */}
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
                                        {p.messages.length} messages
                                    </span>
                                </div>
                            ))}
                        </div>
                    </ScrollArea>
                ) : (
                    <div className="flex-1" />
                )}
            </div>

            {/* COL 3: DETAILS (Consumer Groups + Messages with Tab Toggle) + PAYLOAD INSPECTOR */}
            <div className="flex-1 flex min-w-0 bg-content">
                {hasTopics && selectedTopic && currentPartition ? (
                    <>
                        {/* LEFT SECTION: Messages + Consumer Groups */}
                        <div className="flex-1 flex flex-col min-w-0 border-r-2 border-border">
                            {/* HEADER */}
                            <div className="px-5 py-3  border-b-2 border-border bg-section-header">
                                <div className="text-sm text-muted-foreground font-mono font-medium pb-4">Total messages: {currentPartition.last_offset}</div>
                                <div className="text-sm text-muted-foreground font-mono font-medium">Consumer groups:</div>
                                {currentPartition.groups.length > 0 && (
                                    <div className="space-y-1">
                                        {currentPartition.groups.map((group: ConsumerGroupSummary) => {
                                            return (
                                                <div key={group.id} className="grid grid-cols-3 gap-4 text-sm text-muted-foreground font-mono">
                                                    <div className="truncate ml-6 ">- {group.id}</div>
                                                    <div className="text-right text-muted-foreground">Progress: {group.committed_offset}/{currentPartition.last_offset}</div>
                                                </div>
                                            )
                                        })}
                                    </div>
                                )}
                            </div>

                            {/* EVENTS LOG HEADER */}
                            <div className="px-5 py-2 border-b border-border bg-section-header shrink-0">
                                <span className="text-xs font-bold uppercase tracking-wider text-muted-foreground">Events Log</span>
                            </div>

                            {/* CONTENT AREA */}
                            <div className="flex-1 flex flex-col min-h-0 overflow-hidden">
                                {/* MESSAGES LOG */}
                                <div className="flex-1 flex flex-col min-h-0">
                                    {/* HEADERS */}
                                    <div className="flex items-center px-5 py-2 border-b border-border bg-section-header text-xs font-mono text-muted-foreground uppercase shrink-0 tracking-wider">
                                        <div className="w-16 shrink-0">Offset</div>
                                        <div className="flex-1">Timestamp</div>
                                    </div>

                                    {/* LIST */}
                                    <div className="flex-1 w-full overflow-y-auto" ref={parentRef}>
                                        <div
                                            style={{
                                                height: `${rowVirtualizer.getTotalSize()}px`,
                                                width: '100%',
                                                position: 'relative',
                                            }}
                                        >
                                            {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                                                const msg = currentPartition.messages[virtualRow.index]
                                                const isSelected = selectedMessageOffset === msg.offset

                                                const timeStr = new Date(msg.timestamp).toLocaleTimeString('it-IT', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })

                                                return (
                                                    <div
                                                        key={virtualRow.index}
                                                        onClick={() => setSelectedMessageOffset(msg.offset)}
                                                        style={{
                                                            position: 'absolute',
                                                            top: 0,
                                                            left: 0,
                                                            width: '100%',
                                                            height: `${virtualRow.size}px`,
                                                            transform: `translateY(${virtualRow.start}px)`,
                                                        }}
                                                        className={`
                                                         flex items-center px-5 border-b border-border/50 cursor-pointer hover:bg-muted/40 transition-colors
                                                         ${isSelected ? 'bg-primary/10 border-primary/30' : ''}
                                                     `}
                                                    >
                                                        <div className={`w-16 shrink-0 font-mono text-xs ${isSelected ? 'text-primary font-bold' : 'text-muted-foreground'}`}>
                                                            {msg.offset}
                                                        </div>
                                                        <div className="flex-1 font-mono text-xs text-muted-foreground">
                                                            {timeStr}
                                                        </div>
                                                        {isSelected && <ArrowRight className="h-3 w-3 text-primary ml-2" />}
                                                    </div>
                                                )
                                            })}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>

                        {/* RIGHT SECTION: PAYLOAD INSPECTOR */}
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
                                        <div className="font-mono text-sm leading-relaxed text-foreground whitespace-pre-wrap break-all">
                                            {JSON.stringify(selectedMessage.payload, null, 2)}
                                        </div>
                                    ) : (
                                        <div className="flex flex-col items-center justify-center h-40 text-muted-foreground mt-10">
                                            <p className="text-xs text-center opacity-60">
                                                Select a message
                                            </p>
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