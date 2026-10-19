"""Local portal; all addresses and credentials follow Compose variables."""
import html, json, os
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
PRODUCTS = [
 ('JupyterLab','JUPYTER_PORT',25010,'','JUPYTER_TOKEN','Saisir le token ci-dessous. Notebooks dans un volume persistant.'),
 ('PostgreSQL · pgAdmin','PGADMIN_PORT',25017,'PGADMIN_EMAIL','PGADMIN_PASSWORD','Serveur SQL postgres-source:5432. Base {POSTGRES_DB} ; utilisateur SQL {POSTGRES_USER} ; mot de passe SQL {POSTGRES_PASSWORD}.'),
 ('RustFS · S3','S3_CONSOLE_PORT',25004,'S3_ACCESS_KEY','S3_SECRET_KEY','Buckets Parquet, Delta, Iceberg et Druid.'),
 ('Apache Druid', 'DRUID_PORT', 25019, '', '', 'Profil analytics ou full. Console native d’ingestion et SQL.'),
 ('Trino','TRINO_PORT',25012,'','','Utilisateur libre : formation, sans mot de passe. Supervision SQL ; requêtes dans Superset ou Jupyter.'),
 ('Apache Superset', 'SUPERSET_PORT', 25020, 'SUPERSET_ADMIN_USER', 'SUPERSET_ADMIN_PASSWORD','Profil analytics ou full. PostgreSQL, Trino et Druid préenregistrés. SQL Lab et tableaux de bord.'),
 ('Spark Master','SPARK_MASTER_UI_PORT',25006,'','','Workers et applications.'),
 ('Spark Worker 1','SPARK_WORKER1_UI_PORT',25007,'','','Exécuteurs du premier worker.'),
 ('Spark Worker 2','SPARK_WORKER2_UI_PORT',25008,'','','Exécuteurs du second worker.'),
 ('Spark History','SPARK_HISTORY_UI_PORT',25009,'','','Historique des applications conservé dans S3.'),
 ('Spark · job Jupyter actif','SPARK_JOBS_UI_PORT',25011,'','','Disponible uniquement pendant une SparkSession Jupyter active.'),
 ('Kafka UI','KAFKA_UI_PORT',25018,'','','Topics, messages et consommateurs.'),
 ('Airflow','AIRFLOW_PORT',25014,'','','Profil orchestration ou full. Mode pédagogique all_admins : sans authentification.'),
 ('Iceberg REST · API','ICEBERG_REST_PORT',25005,'','','API /v1/config. Tables consultables dans Superset, Trino ou Jupyter.'),
]
def render():
 cards=[]
 for name,key,port,user,secret,description in PRODUCTS:
  url=f'http://localhost:{os.getenv(key,str(port))}'+('/v1/config' if key=='ICEBERG_REST_PORT' else '')
  esc=html.escape
  cards.append(f'<article><h2>{esc(name)}</h2><p>{esc(description.format_map(os.environ))}</p><dl><dt>Utilisateur</dt><dd>{esc(os.getenv(user,"Sans compte"))}</dd><dt>{"Token" if secret=="JUPYTER_TOKEN" else "Mot de passe"}</dt><dd><code>{esc(os.getenv(secret,"Aucun"))}</code></dd></dl><a href="{url}" target="_blank" rel="noopener">Ouvrir ↗</a><small>{url}</small></article>')
 reports=[]
 for directory,filename in [('/reports','ports-check.json'),('/reports','integration.json'),('/reports','airflow-check.json')]:
  path=Path(directory)/filename
  if path.exists():
   try: reports.append('<h3>'+filename+'</h3><pre>'+html.escape(json.dumps(json.loads(path.read_text()),ensure_ascii=False,indent=2))+'</pre>')
   except (ValueError,OSError): reports.append('<p>Rapport illisible.</p>')
 return ('''<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Formation · Big Data & BI</title><style>body{font:16px system-ui;background:#0c1425;color:#e9efff;margin:0}header,main{max-width:1280px;margin:auto;padding:32px}h1{font-size:38px}input{background:#18243b;border:1px solid #456080;color:white;padding:14px;border-radius:10px;width:min(90%,520px)}section{display:grid;grid-template-columns:repeat(auto-fit,minmax(285px,1fr));gap:18px}article{background:#16223a;border:1px solid #2d405f;border-radius:16px;padding:24px}h2{font-size:20px}article p{color:#bccbe2;min-height:75px}dt,small{color:#8fa7c9}dd{margin:3px 0;overflow-wrap:anywhere}a{display:inline-block;margin:20px 0 8px;color:#071426;background:#78d8c2;padding:10px 18px;border-radius:8px;text-decoration:none;font-weight:600}small{display:block}pre{white-space:pre-wrap;background:#16223a;padding:20px;border-radius:12px}</style><header><span>LABORATOIRE DE FORMATION</span><h1>Big Data & Business Intelligence</h1><p>Interfaces, accès et contrôles d’intégration.</p><input id="search" placeholder="Rechercher une interface…" aria-label="Rechercher"></header><main><section>'''+''.join(cards)+'</section><h2>Contrôles</h2>'+(''.join(reports) or '<p>Aucun rapport : exécuter les contrôles du README.</p>')+'<p>Accès pédagogiques locaux. Les cartes des profils non démarrés restent affichées. Les liens ouvrent les produits déjà démarrés. Les variables correspondent à la création des comptes ; modifier une variable ne change pas un compte déjà enregistré.</p></main><script>document.querySelector("#search").oninput=e=>document.querySelectorAll("article").forEach(a=>a.hidden=!a.textContent.toLowerCase().includes(e.target.value.toLowerCase()));</script></html>').encode()
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  if self.path not in ['/', '/health']: self.send_error(404); return
  body=b'ok' if self.path=='/health' else render()
  self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
if __name__=='__main__': ThreadingHTTPServer(('0.0.0.0',8090),Handler).serve_forever()
