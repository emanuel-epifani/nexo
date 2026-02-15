import { useState } from "react";
import { Check, Copy } from "lucide-react";
import { Highlight, themes } from "prism-react-renderer";

interface CodeBlockProps {
  code: string;
  language?: string;
  title?: string;
}

const CodeBlock = ({ code, language = "typescript", title }: CodeBlockProps) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative group mb-6 rounded-lg border border-code-border overflow-hidden">
      {title && (
        <div className="px-4 py-2 border-b border-code-border text-xs font-medium text-muted-foreground bg-[hsl(220,20%,8%)]">
          {title}
        </div>
      )}
      <button
        onClick={handleCopy}
        className="absolute top-3 right-3 p-1.5 rounded-md opacity-0 group-hover:opacity-100 transition-opacity bg-[hsl(220,14%,18%)] hover:bg-[hsl(220,14%,24%)] z-10"
        aria-label="Copy code"
      >
        {copied ? (
          <Check className="h-3.5 w-3.5 text-primary" />
        ) : (
          <Copy className="h-3.5 w-3.5 text-muted-foreground" />
        )}
      </button>
      <Highlight
        theme={themes.nightOwl}
        code={code.trim()}
        language={language as any}
      >
        {({ style, tokens, getLineProps, getTokenProps }) => (
          <pre
            className="overflow-x-auto p-4 leading-relaxed text-sm"
            style={{ ...style, margin: 0, background: 'hsl(220, 20%, 6%)' }}
          >
            {tokens.map((line, i) => (
              <div key={i} {...getLineProps({ line })}>
                {line.map((token, key) => (
                  <span key={key} {...getTokenProps({ token })} />
                ))}
              </div>
            ))}
          </pre>
        )}
      </Highlight>
    </div>
  );
};

export default CodeBlock;
