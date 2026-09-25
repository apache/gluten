import os
import urllib.parse
import html
from http.server import HTTPServer, SimpleHTTPRequestHandler

class PrettyHTTPRequestHandler(SimpleHTTPRequestHandler):
    def send_head(self):
        path = self.translate_path(self.path)
        _, ext = os.path.splitext(path)
        content_types = {
            '.html': 'text/html; charset=utf-8',
            '.htm':  'text/html; charset=utf-8',
            '.md':   'text/markdown; charset=utf-8',
        }
        if ext.lower() in content_types:
            try:
                f = open(path, 'rb')
            except OSError:
                self.send_error(404, "File not found")
                return None
            fs = os.fstat(f.fileno())
            self.send_response(200)
            self.send_header("Content-Type", content_types[ext.lower()])
            self.send_header("Content-Length", str(fs[6]))
            self.end_headers()
            return f
        return super().send_head()

    def list_directory(self, path):
        try:
            list_dir = os.listdir(path)
        except OSError:
            self.send_error(404, "No permission to list directory")
            return None

        list_dir.sort(key=lambda a: (not os.path.isdir(os.path.join(path, a)), a.lower()))
        
        r = []
        displaypath = html.escape(urllib.parse.unquote(self.path, errors='surrogatepass'), quote=False)
        
        r.append('<!DOCTYPE html>')
        r.append('<html><head><meta charset="utf-8">')
        r.append(f'<title>Index of {displaypath}</title>')
        r.append('<meta name="viewport" content="width=device-width, initial-scale=1">')
        r.append('''
        <style>
            :root {
                --bg: #f8fafc; --card-bg: #ffffff; --text: #0f172a; 
                --muted: #64748b; --border: #e2e8f0; --accent: #2563eb; 
            }
            @media (prefers-color-scheme: dark) {
                :root {
                    --bg: #0f172a; --card-bg: #1e293b; --text: #f8fafc; 
                    --muted: #94a3b8; --border: #334155; --accent: #3b82f6; 
                }
            }
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 24px 12px; }
            .container { max-width: 850px; margin: 0 auto; background: var(--card-bg); border-radius: 12px; border: 1px solid var(--border); box-shadow: 0 10px 15px -3px rgba(0,0,0,0.05); overflow: hidden; }
            header { padding: 20px 24px; border-bottom: 1px solid var(--border); background: rgba(0,0,0,0.01); }
            h1 { margin: 0; font-size: 1.15rem; word-break: break-all; font-weight: 600; }
            .file-list { list-style: none; margin: 0; padding: 0; }
            .file-item { display: flex; align-items: center; justify-content: space-between; padding: 12px 24px; border-bottom: 1px solid var(--border); text-decoration: none; color: inherit; transition: background 0.15s; }
            .file-item:last-child { border-bottom: none; }
            .file-item:hover { background: rgba(37, 99, 235, 0.06); }
            .file-info { display: flex; align-items: center; gap: 12px; overflow: hidden; }
            .icon { font-size: 1.1rem; min-width: 24px; text-align: center; }
            .name { text-overflow: ellipsis; overflow: hidden; white-space: nowrap; font-weight: 500; color: var(--accent); }
            .meta { font-size: 0.85rem; color: var(--muted); font-family: monospace; }
            .search-wrap { padding: 12px 24px; border-bottom: 1px solid var(--border); }
            #search { width: 100%; box-sizing: border-box; padding: 8px 12px; border: 1px solid var(--border); border-radius: 8px; font-size: 0.95rem; background: var(--bg); color: var(--text); outline: none; }
            #search:focus { border-color: var(--accent); }
            .file-item.hidden { display: none; }
        </style>
        ''')
        r.append('</head><body>')
        r.append('<div class="container">')
        r.append(f'<header><h1>📁 Index of {displaypath}</h1></header>')
        r.append('<div class="search-wrap"><input id="search" type="search" placeholder="Search files and folders…" autocomplete="off" autofocus></div>')
        r.append('<ul class="file-list" id="file-list">')

        if self.path != '/':
            r.append('<a class="file-item" href="../"><div class="file-info"><span class="icon">⬆️</span><span class="name">.. (Parent Directory)</span></div></a>')

        for name in list_dir:
            fullname = os.path.join(path, name)
            display_name = link_name = name
            icon = "📄"
            
            if os.path.isdir(fullname):
                icon = "📁"
                display_name = name + "/"
                link_name = name + "/"
            
            size_str = ""
            if not os.path.isdir(fullname):
                try:
                    size = os.path.getsize(fullname)
                    for unit in ['B', 'KB', 'MB', 'GB']:
                        if size < 1024:
                            size_str = f"{size:.1f} {unit}"
                            break
                        size /= 1024
                except OSError:
                    size_str = "-"

            r.append(f'<a class="file-item" href="{urllib.parse.quote(link_name)}">')
            r.append(f'  <div class="file-info"><span class="icon">{icon}</span><span class="name">{html.escape(display_name)}</span></div>')
            r.append(f'  <div class="meta">{size_str}</div>')
            r.append('</a>')

        r.append('</ul></div>')
        r.append('''<script>
var input = document.getElementById("search");
var items = document.querySelectorAll("#file-list .file-item");
input.addEventListener("input", function() {
    var q = this.value.toLowerCase();
    items.forEach(function(el) {
        var name = el.querySelector(".name").textContent.toLowerCase();
        el.classList.toggle("hidden", q !== "" && name.indexOf(q) === -1);
    });
});
</script>
</body></html>''')
        
        encoded = '\n'.join(r).encode('utf-8', 'surrogateescape')
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)
        return None

if __name__ == '__main__':
    PORT = 5020
    server = HTTPServer(('0.0.0.0', PORT), PrettyHTTPRequestHandler)
    print(f"Serving HTTP on port {PORT} (http://localhost:{PORT}/) ...")
    server.serve_forever()
