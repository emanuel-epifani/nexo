import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import Index from "./pages/Index";
import NotFound from "./pages/NotFound";
import DocsLayout from "./layouts/DocsLayout";
import Introduction from "./pages/docs/Introduction";
import QuickStart from "./pages/docs/QuickStart";
import Architecture from "./pages/docs/Architecture";
import SDKs from "./pages/docs/SDKs";
import StoreDocs from "./pages/docs/StoreDocs";
import PubSubDocs from "./pages/docs/PubSubDocs";
import QueueDocs from "./pages/docs/QueueDocs";
import StreamDocs from "./pages/docs/StreamDocs";
import BinaryDocs from "./pages/docs/BinaryDocs";

const queryClient = new QueryClient();

const App = () => (
  <QueryClientProvider client={queryClient}>
    <TooltipProvider>
      <Toaster />
      <Sonner />
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Index />} />
          <Route path="/docs" element={<DocsLayout />}>
            <Route index element={<Introduction />} />
            <Route path="quickstart" element={<QuickStart />} />
            <Route path="architecture" element={<Architecture />} />
            <Route path="sdks" element={<SDKs />} />
            <Route path="store" element={<StoreDocs />} />
            <Route path="pubsub" element={<PubSubDocs />} />
            <Route path="queue" element={<QueueDocs />} />
            <Route path="stream" element={<StreamDocs />} />
            <Route path="binary" element={<BinaryDocs />} />
          </Route>
          <Route path="*" element={<NotFound />} />
        </Routes>
      </BrowserRouter>
    </TooltipProvider>
  </QueryClientProvider>
);

export default App;
