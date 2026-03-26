import { AlertCircle } from "lucide-react"
import { Button } from "./button"
import { Alert, AlertDescription, AlertTitle } from "./alert"

interface QueryErrorProps {
    error: Error | null | unknown
    onRetry?: () => void
    title?: string
}

export function QueryError({ error, onRetry, title = "ERROR_LOADING_DATA" }: QueryErrorProps) {
    const errorMessage = error instanceof Error ? error.message : "Unknown error occurred"

    return (
        <div className="flex h-full flex-col items-center justify-center p-8 m-4">
            <Alert variant="destructive" className="max-w-md w-full bg-destructive/5 border-2 border-destructive/20 text-center flex flex-col items-center gap-2 py-6">
                <AlertCircle className="h-12 w-12 text-destructive mb-2" />
                <AlertTitle className="font-bold text-base uppercase tracking-wider">{title}</AlertTitle>
                <AlertDescription className="text-sm opacity-80 mb-4">
                    {errorMessage}
                </AlertDescription>
                {onRetry && (
                    <Button variant="outline" size="sm" onClick={onRetry} className="mt-2 border-destructive/30 hover:bg-destructive/10">
                        TRY_AGAIN
                    </Button>
                )}
            </Alert>
        </div>
    )
}
