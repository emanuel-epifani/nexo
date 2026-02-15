import { useEffect, useState } from "react";
import { useLocation } from "react-router-dom";
import { cn } from "@/lib/utils";

interface TocItem {
  id: string;
  text: string;
  level: number;
}

const TableOfContents = () => {
  const [headings, setHeadings] = useState<TocItem[]>([]);
  const [activeId, setActiveId] = useState<string>("");
  const location = useLocation();

  useEffect(() => {
    // Small delay to let the page render
    const timer = setTimeout(() => {
      const main = document.querySelector("main");
      if (!main) return;

      const elements = main.querySelectorAll("h2[id], h3[id]");
      const items: TocItem[] = Array.from(elements).map((el) => ({
        id: el.id,
        text: el.textContent?.replace(/coming soon/gi, "").trim() || "",
        level: el.tagName === "H2" ? 2 : 3,
      }));
      setHeadings(items);
    }, 100);

    return () => clearTimeout(timer);
  }, [location.pathname]);

  useEffect(() => {
    const main = document.querySelector("main");
    if (!main) return;

    const observer = new IntersectionObserver(
      (entries) => {
        // Find the first visible heading
        const visible = entries.filter((e) => e.isIntersecting);
        if (visible.length > 0) {
          setActiveId(visible[0].target.id);
        }
      },
      { rootMargin: "-80px 0px -70% 0px", threshold: 0 }
    );

    const elements = main.querySelectorAll("h2[id], h3[id]");
    elements.forEach((el) => observer.observe(el));

    return () => observer.disconnect();
  }, [headings]);

  if (headings.length === 0) return null;

  return (
    <aside className="w-48 shrink-0 hidden xl:block">
      <div className="sticky top-20 overflow-y-auto max-h-[calc(100vh-6rem)]">
        <h4 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3 px-1">
          On this page
        </h4>
        <ul className="space-y-1 border-l border-border">
          {headings.map((heading) => (
            <li key={heading.id}>
              <a
                href={`#${heading.id}`}
                onClick={(e) => {
                  e.preventDefault();
                  document.getElementById(heading.id)?.scrollIntoView({ behavior: "smooth" });
                  setActiveId(heading.id);
                }}
                className={cn(
                  "block text-xs py-1 transition-colors border-l -ml-px",
                  heading.level === 3 ? "pl-5" : "pl-3",
                  activeId === heading.id
                    ? "border-primary text-primary font-medium"
                    : "border-transparent text-muted-foreground hover:text-foreground"
                )}
              >
                {heading.text}
              </a>
            </li>
          ))}
        </ul>
      </div>
    </aside>
  );
};

export default TableOfContents;
