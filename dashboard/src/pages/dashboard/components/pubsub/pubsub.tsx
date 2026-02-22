import { useState, useEffect } from "react"
import { useQuery } from "@tanstack/react-query"
import { PubSubBrokerSnapshot, WildcardSubscription } from "./types"
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
    Users,
    ChevronLeft,
    ChevronRight,
    RefreshCw,
} from "lucide-react"
import { Badge } from "@/components/ui/badge"

const PAGE_SIZE = 50

export function PubSubView() {
    const [activeTab, setActiveTab] = useState<'topics' | 'wildcards'>('topics')
    const [selectedPath, setSelectedPath] = useState<string | null>(null)
    const [inputValue, setInputValue] = useState("")
    const [debouncedSearch, setDebouncedSearch] = useState("")
    const [offset, setOffset] = useState(0)
    const [wildcardFilter, setWildcardFilter] = useState<'multi' | 'single'>('single')

    // Debounce search input
    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearch(inputValue)
            setOffset(0)
        }, 300)
        return () => clearTimeout(timer)
    }, [inputValue])

    // Reset offset when tab changes
    useEffect(() => {
        setOffset(0)
        setSelectedPath(null)
    }, [activeTab])

    const { data, isLoading } = useQuery({
        queryKey: ['pubsub-snapshot', offset, debouncedSearch],
        queryFn: async (): Promise<PubSubBrokerSnapshot> => {
            const params = new URLSearchParams({ limit: String(PAGE_SIZE), offset: String(offset) })
            if (debouncedSearch) params.set("search", debouncedSearch)
            const res = await fetch(`/api/pubsub?${params}`)
            if (!res.ok) throw new Error('Failed to fetch pubsub')
            return res.json()
        },
    })

    const topics = data?.topics ?? []
    const totalTopics = data?.total_topics ?? 0
    const wildcards = data?.wildcards
    const hasMore = offset + PAGE_SIZE < totalTopics
    
    const allWildcards = wildcards ? [...wildcards.multi_level, ...wildcards.single_level] : []
    const filteredWildcards = wildcardFilter === 'multi'
        ? allWildcards.filter(w => w.pattern.includes('#'))
        : allWildcards.filter(w => w.pattern.includes('+'))

    const topic = topics.find(t => t.full_path === selectedPath)
    const wildcard = allWildcards.find(w => w.pattern === selectedPath)
    const selectedItem = !selectedPath ? null
        : topic ? { ...topic, is_wildcard: false }
        : wildcard ? { full_path: wildcard.pattern, subscribers: 1, retained_value: null, is_wildcard: true, client_id: wildcard.client_id }
        : null

    return (
        <div className="flex h-full gap-0 border-2 border-border rounded-sm bg-panel overflow-hidden font-mono text-sm">

            {/* SIDEBAR */}
            <div className="w-[400px] flex flex-col border-r-2 border-border bg-sidebar">
                {/* Tab switcher */}
                <div className="flex border-b-2 border-border">
                    <button
                        onClick={() => setActiveTab('topics')}
                        className={`flex-1 py-3 text-xs font-bold uppercase tracking-wider transition-colors ${activeTab === 'topics' ? 'bg-secondary text-foreground' : 'text-muted-foreground hover:text-foreground'}`}
                    >
                        Topics ({totalTopics})
                    </button>
                    <div className="w-[1px] bg-border" />
                    <button
                        onClick={() => setActiveTab('wildcards')}
                        className={`flex-1 py-3 text-xs font-bold uppercase tracking-wider transition-colors ${activeTab === 'wildcards' ? 'bg-secondary text-foreground' : 'text-muted-foreground hover:text-foreground'}`}
                    >
                        Wildcards ({(wildcards?.multi_level.length ?? 0) + (wildcards?.single_level.length ?? 0)})
                    </button>
                </div>

                {/* Search — only for topics tab */}
                <div className="p-3 border-b border-border bg-section-header">
                    {activeTab === 'topics' ? (
                        <div className="relative">
                            <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                            <Input
                                placeholder="Search path..."
                                value={inputValue}
                                onChange={(e) => setInputValue(e.target.value)}
                                className="h-9 pl-8 bg-background border-border text-xs font-mono placeholder:text-muted-foreground focus-visible:ring-1 focus-visible:ring-ring"
                            />
                        </div>
                    ) : (
                        <div className="flex gap-1">
                            <button
                                onClick={() => setWildcardFilter('multi')}
                                className={`flex-1 px-2 py-1 text-xs font-mono rounded transition-colors ${wildcardFilter === 'multi' ? 'bg-status-pending/10 text-status-pending border border-status-pending/30' : 'bg-background text-muted-foreground border border-border hover:text-status-pending'}`}
                            >
                                # MULTI ({wildcards?.multi_level.length ?? 0})
                            </button>
                            <button
                                onClick={() => setWildcardFilter('single')}
                                className={`flex-1 px-2 py-1 text-xs font-mono rounded transition-colors ${wildcardFilter === 'single' ? 'bg-status-processing/10 text-status-processing border border-status-processing/30' : 'bg-background text-muted-foreground border border-border hover:text-status-processing'}`}
                            >
                                + SINGLE ({wildcards?.single_level.length ?? 0})
                            </button>
                        </div>
                    )}
                </div>

                {/* List */}
                {activeTab === 'topics' ? (
                    <>
                        {/* Table header */}
                        <div className="sticky top-0 bg-section-header border-b border-border px-3 py-2 grid grid-cols-[1fr_4rem_4rem] gap-4 text-xs font-bold uppercase text-muted-foreground z-10">
                            <div>Topic Path</div>
                            <div className="text-center">Subs</div>
                            <div className="text-center">Retained</div>
                        </div>

                        <div className="flex-1 overflow-y-auto">
                            {isLoading ? (
                                <div className="flex flex-col items-center justify-center h-32 text-muted-foreground">
                                    <RefreshCw className="h-5 w-5 animate-spin mb-2 opacity-50" />
                                    <p className="text-xs uppercase opacity-50">LOADING...</p>
                                </div>
                            ) : topics.length === 0 ? (
                                <div className="py-12 text-center">
                                    <p className="text-xs text-muted-foreground">NO_RESULTS</p>
                                </div>
                            ) : (
                                topics.map((item) => (
                                    <div
                                        key={item.full_path}
                                        onClick={() => setSelectedPath(item.full_path)}
                                        className={`grid grid-cols-[1fr_4rem_4rem] gap-4 items-center px-3 py-2 border-b border-border/50 cursor-pointer transition-all min-h-9 ${selectedPath === item.full_path ? 'bg-secondary' : 'hover:bg-muted/50'}`}
                                    >
                                        <span className={`font-mono text-xs truncate ${selectedPath === item.full_path ? 'text-foreground' : 'text-muted-foreground'}`}>
                                            {item.full_path}
                                        </span>
                                        <div className="flex justify-center">
                                            {item.subscribers > 0 ? (
                                                <Badge variant="secondary" className="h-4 px-1.5 text-xs bg-muted text-muted-foreground border-border rounded-sm">
                                                    {item.subscribers}
                                                </Badge>
                                            ) : (
                                                <span className="text-muted-foreground text-xs">—</span>
                                            )}
                                        </div>
                                        <div className="flex justify-center">
                                            {item.retained_value ? (
                                                <Badge variant="outline" className="h-4 px-1 text-xs border-status-processing bg-status-processing/10 text-status-processing rounded-sm">
                                                    RET
                                                </Badge>
                                            ) : (
                                                <span className="text-muted-foreground text-xs">—</span>
                                            )}
                                        </div>
                                    </div>
                                ))
                            )}
                        </div>

                        {/* Pagination */}
                        <div className="p-2 border-t border-border bg-section-header flex items-center justify-between flex-shrink-0">
                            <div className="text-xs text-muted-foreground font-mono">
                                {totalTopics > 0 ? (
                                    <span>{offset + 1}-{Math.min(offset + PAGE_SIZE, totalTopics)} of {totalTopics}</span>
                                ) : (
                                    <span>0 topics</span>
                                )}
                            </div>
                            <div className="flex gap-1">
                                <button
                                    disabled={offset === 0 || isLoading}
                                    onClick={() => setOffset(o => Math.max(0, o - PAGE_SIZE))}
                                    className="h-7 px-2 flex items-center gap-1 text-xs font-mono uppercase bg-background border border-border rounded hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                                >
                                    <ChevronLeft className="h-3 w-3" />
                                    Prev
                                </button>
                                <button
                                    disabled={!hasMore || isLoading}
                                    onClick={() => setOffset(o => o + PAGE_SIZE)}
                                    className="h-7 px-2 flex items-center gap-1 text-xs font-mono uppercase bg-background border border-border rounded hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                                >
                                    Next
                                    <ChevronRight className="h-3 w-3" />
                                </button>
                            </div>
                        </div>
                    </>
                ) : (
                    <ScrollArea className="flex-1">
                        {filteredWildcards.length === 0 ? (
                            <div className="py-12 text-center">
                                <p className="text-xs text-muted-foreground">NO_WILDCARDS</p>
                            </div>
                        ) : (
                            filteredWildcards.map((item: WildcardSubscription) => (
                                <div
                                    key={`${item.pattern}-${item.client_id}`}
                                    onClick={() => setSelectedPath(item.pattern)}
                                    className={`flex items-center gap-3 px-3 py-2 border-b border-border/50 cursor-pointer transition-all min-h-9 ${selectedPath === item.pattern ? 'bg-secondary' : 'hover:bg-muted/50'}`}
                                >
                                    <div className="flex flex-col gap-1 overflow-hidden min-w-0">
                                        <span className={`font-mono text-xs truncate ${selectedPath === item.pattern ? 'text-foreground' : 'text-muted-foreground'}`}>
                                            {item.pattern}
                                        </span>
                                        <div className="text-xs text-muted-foreground truncate">
                                            Client: {item.client_id}
                                        </div>
                                    </div>
                                </div>
                            ))
                        )}
                    </ScrollArea>
                )}
            </div>

            {/* DETAIL PANEL */}
            <div className="flex-1 bg-content flex flex-col min-w-0">
                {selectedItem ? (
                    <div className="flex flex-col h-full">
                        <div className="p-4 border-b border-border bg-section-header">
                            <div className="flex items-center gap-2 mb-2">
                                {selectedItem.is_wildcard ? (
                                    <Hash className="h-4 w-4 text-status-pending" />
                                ) : (
                                    <Circle className="h-3 w-3 text-status-success" />
                                )}
                                <h2 className="text-sm font-bold text-foreground">{selectedItem.full_path}</h2>
                            </div>
                            <div className="flex gap-4 text-xs text-muted-foreground uppercase tracking-wide">
                                <div className="flex items-center gap-1.5">
                                    <Users className="h-3 w-3" />
                                    {selectedItem.subscribers} SUBSCRIBERS
                                </div>
                                {'client_id' in selectedItem && selectedItem.client_id && (
                                    <div className="flex items-center gap-1.5 text-muted-foreground">
                                        <Zap className="h-3 w-3 text-status-pending" />
                                        CLIENT: {selectedItem.client_id}
                                    </div>
                                )}
                            </div>
                        </div>

                        {selectedItem.retained_value ? (
                            <div className="flex-1 relative">
                                <div className="absolute inset-0 overflow-auto p-6 scrollbar-thin">
                                    <pre className="text-xs text-foreground font-mono leading-relaxed whitespace-pre-wrap">
                                        {JSON.stringify(selectedItem.retained_value, null, 2)}
                                    </pre>
                                </div>
                            </div>
                        ) : (
                            <div className="flex-1 flex flex-col items-center justify-center text-muted-foreground">
                                <p className="text-xs font-mono uppercase tracking-widest opacity-50">NO_RETAINED_VALUE</p>
                            </div>
                        )}

                        {selectedItem.retained_value && (
                            <div className="px-4 py-2 border-t border-border bg-section-header text-xs text-muted-foreground flex justify-between font-mono uppercase">
                                <div className="flex items-center gap-2">
                                    <Binary className="h-3 w-3" />
                                    <span>SIZE: {new Blob([JSON.stringify(selectedItem.retained_value)]).size} BYTES</span>
                                </div>
                                <div className="flex items-center gap-2">
                                    <FileJson className="h-3 w-3" />
                                    <span>UTF-8 CONTENT</span>
                                </div>
                            </div>
                        )}
                    </div>
                ) : (
                    <div className="flex-1 flex flex-col items-center justify-center text-muted-foreground">
                        <Radio className="h-12 w-12 opacity-20 mb-4" />
                        <p className="text-xs font-mono uppercase tracking-widest opacity-50">SELECT_TOPIC</p>
                    </div>
                )}
            </div>
        </div>
    )
}
