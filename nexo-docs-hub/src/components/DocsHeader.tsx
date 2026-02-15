import { Link, useLocation } from "react-router-dom";
import { Menu, X, Package, Container } from "lucide-react";
import { useState } from "react";
import { cn } from "@/lib/utils";
import MobileSidebar from "./MobileSidebar";
import { HoverCard, HoverCardTrigger, HoverCardContent } from "@/components/ui/hover-card";

const GitHubIcon = ({ className }: { className?: string }) => (
  <svg viewBox="0 0 24 24" fill="currentColor" className={className}>
    <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0 0 24 12c0-6.63-5.37-12-12-12z" />
  </svg>
);

const LinkedInIcon = ({ className }: { className?: string }) => (
  <svg viewBox="0 0 24 24" fill="currentColor" className={className}>
    <path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433a2.062 2.062 0 0 1-2.063-2.065 2.064 2.064 0 1 1 2.063 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z" />
  </svg>
);

const sdkList = [
  { language: "TypeScript", package: "@emanuelepifani/nexo-client", available: true, npmUrl: "https://www.npmjs.com/package/@emanuelepifani/nexo-client" },
  { language: "Python", package: "nexo-client", available: false },
];

const DocsHeader = () => {
  const [mobileOpen, setMobileOpen] = useState(false);
  const location = useLocation();
  const isHome = location.pathname === "/";
  const isDocs = location.pathname.startsWith("/docs");

  return (
    <>
      <header className="h-14 border-b border-border bg-background/80 backdrop-blur-sm sticky top-0 z-50">
        <div className="h-full flex items-center justify-between px-4 lg:px-6 max-w-screen-2xl mx-auto">
          <div className="flex items-center gap-4">
            <button
              className="lg:hidden p-1.5 rounded-md hover:bg-secondary"
              onClick={() => setMobileOpen(!mobileOpen)}
            >
              {mobileOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
            </button>
            <Link to="/" className="flex items-center gap-2">
              <div className="h-7 w-7 rounded-md nexo-gradient flex items-center justify-center">
                <span className="text-sm font-bold text-primary-foreground">N</span>
              </div>
              <span className="font-bold text-lg tracking-tight">Nexo</span>
            </Link>
            <nav className="hidden md:flex items-center gap-1 ml-4">
              <Link
                to="/docs"
                className={cn(
                  "text-sm px-3 py-1.5 rounded-md transition-colors",
                  isDocs ? "text-foreground font-medium" : "text-muted-foreground hover:text-foreground"
                )}
              >
                Docs
              </Link>
              <HoverCard openDelay={100} closeDelay={200}>
                <HoverCardTrigger asChild>
                  <Link
                    to="/docs/sdks"
                    className={cn(
                      "text-sm px-3 py-1.5 rounded-md transition-colors",
                      location.pathname === "/docs/sdks" ? "text-foreground font-medium" : "text-muted-foreground hover:text-foreground"
                    )}
                  >
                    Client SDKs
                  </Link>
                </HoverCardTrigger>
                <HoverCardContent className="w-56 p-3" sideOffset={8}>
                  <div className="space-y-2">
                    {sdkList.map((sdk) => (
                      <div key={sdk.language} className={cn("flex items-center gap-2 text-sm", !sdk.available && "opacity-40")}>
                        <Package className="h-3.5 w-3.5 shrink-0" />
                        {sdk.available ? (
                          <a href={sdk.npmUrl} target="_blank" rel="noopener noreferrer" className="hover:text-primary transition-colors font-medium">
                            {sdk.language}
                          </a>
                        ) : (
                          <span className="text-muted-foreground">{sdk.language} <span className="text-xs">(coming soon)</span></span>
                        )}
                      </div>
                    ))}
                  </div>
                </HoverCardContent>
              </HoverCard>
              <a
                href="https://hub.docker.com/r/emanuelepifani/nexo"
                target="_blank"
                rel="noopener noreferrer"
                className={cn(
                  "text-sm px-3 py-1.5 rounded-md transition-colors flex items-center gap-1.5",
                  "text-muted-foreground hover:text-foreground"
                )}
              >
                <Container className="h-3.5 w-3.5" />
                Docker Hub
              </a>
            </nav>
          </div>
          <div className="flex items-center gap-1">
            <a
              href="https://www.linkedin.com/in/emanuel-epifani/"
              target="_blank"
              rel="noopener noreferrer"
              className="p-2 rounded-md hover:bg-secondary transition-colors"
            >
              <LinkedInIcon className="h-5 w-5" />
            </a>
            <a
              href="https://github.com/emanuel-epifani/nexo"
              target="_blank"
              rel="noopener noreferrer"
              className="p-2 rounded-md hover:bg-secondary transition-colors"
            >
              <GitHubIcon className="h-5 w-5" />
            </a>
          </div>
        </div>
      </header>
      {mobileOpen && <MobileSidebar onClose={() => setMobileOpen(false)} />}
    </>
  );
};

export default DocsHeader;
