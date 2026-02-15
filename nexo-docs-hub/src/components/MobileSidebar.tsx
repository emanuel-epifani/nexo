import { NavLink } from "react-router-dom";
import { 
  Database, Radio, ListOrdered, Activity, 
  Rocket, LayoutGrid, Binary, Zap, Code2
} from "lucide-react";
import { cn } from "@/lib/utils";

interface SidebarItem {
  title: string;
  path: string;
  icon: React.ElementType;
}

const sections = [
  {
    title: "Getting Started",
    items: [
      { title: "Introduction", path: "/docs", icon: Rocket },
      { title: "Quick Start", path: "/docs/quickstart", icon: Zap },
      { title: "Architecture", path: "/docs/architecture", icon: LayoutGrid },
      { title: "Client SDKs", path: "/docs/sdks", icon: Code2 },
    ] as SidebarItem[],
  },
  {
    title: "Brokers",
    items: [
      { title: "Store", path: "/docs/store", icon: Database },
      { title: "Pub/Sub", path: "/docs/pubsub", icon: Radio },
      { title: "Queue", path: "/docs/queue", icon: ListOrdered },
      { title: "Stream", path: "/docs/stream", icon: Activity },
    ] as SidebarItem[],
  },
  {
    title: "Advanced",
    items: [
      { title: "Binary Payloads", path: "/docs/binary", icon: Binary },
    ] as SidebarItem[],
  },
];

interface MobileSidebarProps {
  onClose: () => void;
}

const MobileSidebar = ({ onClose }: MobileSidebarProps) => {
  return (
    <div className="fixed inset-0 top-14 z-40 bg-background lg:hidden">
      <nav className="p-4 space-y-6 overflow-y-auto h-full">
        {sections.map((section) => (
          <div key={section.title}>
            <h4 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-2 px-2">
              {section.title}
            </h4>
            <ul className="space-y-0.5">
              {section.items.map((item) => (
                <li key={item.path}>
                  <NavLink
                    to={item.path}
                    end={item.path === "/docs"}
                    onClick={onClose}
                    className={({ isActive }) =>
                      cn(
                        "flex items-center gap-2.5 px-2 py-2 rounded-md text-sm transition-colors",
                        isActive
                          ? "bg-accent text-accent-foreground font-medium"
                          : "text-muted-foreground hover:text-foreground hover:bg-secondary"
                      )
                    }
                  >
                    <item.icon className="h-4 w-4" />
                    {item.title}
                  </NavLink>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </nav>
    </div>
  );
};

export default MobileSidebar;
