import { useQuery } from '@tanstack/react-query'
import { SystemSnapshot } from '@/lib/types'

const FETCH_INTERVAL = 2000; // 2 seconds

async function fetchSystemState(): Promise<SystemSnapshot> {
    const response = await fetch('/api/state');
    if (!response.ok) {
        throw new Error('Network response was not ok');
    }
    return response.json();
}

export function useSystemState() {
    return useQuery({
        queryKey: ['system-state'],
        queryFn: fetchSystemState,
        refetchInterval: FETCH_INTERVAL,
        retry: 3
    });
}

