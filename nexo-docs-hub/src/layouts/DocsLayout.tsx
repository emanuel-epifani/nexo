import { Outlet } from "react-router-dom";
import DocsHeader from "@/components/DocsHeader";
import DocsSidebar from "@/components/DocsSidebar";
import TableOfContents from "@/components/TableOfContents";

const DocsLayout = () => {
  return (
    <div className="min-h-screen bg-background">
      <DocsHeader />
      <div className="flex max-w-screen-2xl mx-auto">
        <DocsSidebar />
        <main className="flex-1 min-w-0 px-6 lg:px-12 py-10 max-w-3xl">
          <Outlet />
        </main>
        <TableOfContents />
      </div>
    </div>
  );
};

export default DocsLayout;
