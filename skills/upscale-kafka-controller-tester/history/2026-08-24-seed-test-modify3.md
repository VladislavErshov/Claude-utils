# Seed test-modify3 из прода (2026-08-24)

Кластер: `9fc47c1b-011d-4aaa-b411-de5345a0204e` (test-modify3, kafka, project 160).
Продовый снапшот: `/tmp/test-modify3.json` (7 хостов: 3 broker, 3 controller, 1 cruise; one_cloud_meta: cruise-control-service, db-service, kafka-controller-service; versions 5, hw presets 100/14).

## Генерация seed-SQL (python)

```python
import json
d = json.load(open('/tmp/test-modify3.json'))
def esc(v):
    if v is None: return 'NULL'
    if isinstance(v,(dict,list)): v = json.dumps(v, ensure_ascii=False)
    if isinstance(v,bool): return 'true' if v else 'false'
    if isinstance(v,(int,float)): return str(v)
    return "'" + str(v).replace("'","''") + "'"
def rows(tbl, data, drop=()):
    if not data: return []
    cols = [c for c in data[0].keys() if c not in drop]
    return ["INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING;" % (tbl, ",".join(cols), ",".join(esc(r[c]) for c in cols)) for r in data]
cid = '9fc47c1b-011d-4aaa-b411-de5345a0204e'
sql = ["BEGIN;",
 "DELETE FROM operations WHERE cluster_id='%s';" % cid,
 "DELETE FROM one_cloud_meta WHERE cluster_id='%s';" % cid,
 "DELETE FROM host_state WHERE cluster_id='%s';" % cid,
 "DELETE FROM db_cluster_version WHERE cluster_id='%s';" % cid,
 "DELETE FROM db_cluster WHERE id='%s';" % cid,
]
for tbl,drop in [('namespaces',()),('projects',()),('hardware_presets',()),('db_cluster',()),('db_cluster_version',()),('host_state',()),('one_cloud_meta',('fake_id',)),('operations',()),('settings',())]:
    sql += rows(tbl, d.get(tbl) or [], drop)
sql.append("COMMIT;")
print("\n".join(sql))
```

Грабли:
- в проде у `one_cloud_meta` есть колонка `fake_id`, локально её нет — дропаем при INSERT;
- psql только через `docker cp` + `psql -f` (heredoc в stdin тихо не применяет DELETE/INSERT);
- после seed — проставить пустые конфиги в draft-версиях (см. SKILL.md).

Верификация: cl=1, hosts=7, meta=3, vers=5.
