import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Input } from "@/components/ui/input"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { StoreBrokerSnapshot } from "@/lib/types"
import { useState } from "react"
import { Badge } from "@/components/ui/badge"

interface Props {
  data: StoreBrokerSnapshot
}

export function StoreView({ data }: Props) {
  const [filter, setFilter] = useState("")

  const filteredKeys = data.keys.filter(k => 
    k.key.toLowerCase().includes(filter.toLowerCase())
  )

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold tracking-tight">Key-Value Store</h2>
        <div className="flex items-center space-x-2">
            <Input 
                placeholder="Filter keys..." 
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                className="w-[300px]"
            />
        </div>
      </div>

      <Card>
        <CardHeader>
           <CardTitle>Keys ({filteredKeys.length})</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[300px]">Key</TableHead>
                  <TableHead>Value Preview</TableHead>
                  <TableHead className="w-[200px]">Expires At</TableHead>
                  <TableHead className="w-[100px]">Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredKeys.length === 0 ? (
                    <TableRow>
                        <TableCell colSpan={4} className="h-24 text-center">
                            No keys found.
                        </TableCell>
                    </TableRow>
                ) : (
                    filteredKeys.map((k) => (
                    <TableRow key={k.key}>
                        <TableCell className="font-mono font-medium">{k.key}</TableCell>
                        <TableCell className="font-mono text-xs text-muted-foreground truncate max-w-[300px]">
                            {k.value_preview}
                        </TableCell>
                        <TableCell className="text-xs">
                            {k.expires_at ? new Date(k.expires_at).toLocaleString() : "Never"}
                        </TableCell>
                        <TableCell>
                            {k.expires_at ? (
                                <Badge variant="secondary">TTL</Badge>
                            ) : (
                                <Badge variant="outline">Persistent</Badge>
                            )}
                        </TableCell>
                    </TableRow>
                    ))
                )}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

