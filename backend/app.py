
import hashlib, secrets, sqlite3, threading
from datetime import date, datetime
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 同主题回滚串行锁：同主题两名值班同时抢回滚，只放行一支，后到记“回滚争用”
ROLLBACK_LOCKS: dict[str, threading.RLock] = {}
ROLLBACK_LOCKS_GUARD = threading.Lock()

def rollback_lock(subject: str) -> threading.RLock:
    with ROLLBACK_LOCKS_GUARD:
        lk = ROLLBACK_LOCKS.get(subject)
        if lk is None:
            lk = threading.RLock()
            ROLLBACK_LOCKS[subject] = lk
        return lk

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        -- 正式指针：每主题唯一一行，目录/入站/详情/对账/附页全部同源到这里
        CREATE TABLE IF NOT EXISTS contract_pointers(
          subject TEXT PRIMARY KEY,
          current_version TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        -- 乙路径回滚快照表：目标版切指针前先落入快照，不可改
        CREATE TABLE IF NOT EXISTS rollback_snapshots(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          rollback_seq INTEGER NOT NULL,
          subject TEXT NOT NULL,
          version TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          required_fields TEXT NOT NULL,
          snapshot_kind TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        -- 旧正式停用后进历史，停用历史版不可再裁（不可再启用/修剪）
        CREATE TABLE IF NOT EXISTS contract_history(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          rollback_seq INTEGER NOT NULL,
          subject TEXT NOT NULL,
          version TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          required_fields TEXT NOT NULL,
          retired_at TEXT NOT NULL
        );
        -- 回滚处置簿：选定路径当天不许换，偏了记“回滚偏了”，争用记“回滚争用”
        CREATE TABLE IF NOT EXISTS rollback_ledger(
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          target_version TEXT,
          path TEXT,
          source TEXT NOT NULL,
          operator TEXT NOT NULL,
          status TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_day TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3","订单入站","严格","正式","启用","order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow","订单入站","兼容","影子","试跑","order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2","库存入站","严格","正式","停用","sku,qty"))
        # 指针与现存启用正式契约对齐，保证只有一个正式来源
        for r in conn.execute("SELECT subject, version FROM contracts WHERE track='正式' AND status='启用'").fetchall():
            hit = conn.execute("SELECT 1 FROM contract_pointers WHERE subject=?", (r["subject"],)).fetchone()
            if not hit:
                conn.execute("INSERT INTO contract_pointers(subject,current_version,updated_at) VALUES (?,?,?)",
                             (r["subject"], r["version"], datetime.now().isoformat(timespec="seconds")))

@app.on_event("startup")
def _s():
    init_db()

def now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")

def ledger_insert(conn, *, subject, target_version, path, source, operator, status, detail):
    cur = conn.execute(
        """INSERT INTO rollback_ledger(subject,target_version,path,source,operator,status,detail,created_day,created_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (subject, target_version, path, source, operator, status, detail,
         date.today().isoformat(), now_text()))
    return cur.lastrowid

def ledger_record(**kw):
    """独立事务落处置簿：即使外层因 HTTPException 回滚，记录也必须留下。"""
    with connect() as c:
        seq = ledger_insert(c, **kw)
        c.commit()
        return seq

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""
            SELECT ct.*, CASE WHEN p.current_version IS NOT NULL THEN 1 ELSE 0 END AS is_pointer
            FROM contracts ct
            LEFT JOIN contract_pointers p
              ON p.subject = ct.subject AND p.current_version = ct.version
            ORDER BY ct.id""").fetchall()
        return {"items":[dict(r) for r in rows]}

class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present

@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
    with connect() as c:
        # 只认唯一正式指针：目录与入站同源，停用版/历史版永远不会再被命中
        ctr = c.execute("""
            SELECT ct.* FROM contracts ct
            JOIN contract_pointers p
              ON p.subject = ct.subject AND p.current_version = ct.version
            WHERE ct.subject=? AND ct.track='正式' AND ct.status='启用'
            LIMIT 1""", (body.subject,)).fetchone()
        if not ctr:
            raise HTTPException(404, "无启用正式契约")
        need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
        missing = sorted(need - fields)
        now = now_text()
        if missing:
            if ctr["mode"] == "兼容":
                cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                (body.subject, body.payload_fields, ctr["version"], "缓冲", "缺字段:"+ ",".join(missing), now))
                c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (cur.lastrowid, "缺字段进双写缓冲"))
                return {"result":"缓冲", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
            cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                            (body.subject, body.payload_fields, ctr["version"], "拒收", "缺字段:"+ ",".join(missing), now))
            return {"result":"拒收", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
        cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                        (body.subject, body.payload_fields, ctr["version"], "通过", "字段齐全", now))
        return {"result":"通过", "version": ctr["version"], "batch_id": cur.lastrowid}

@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall()]}

@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT b.*, i.subject, i.payload_fields FROM buffers b JOIN ingress_batches i ON i.id=b.batch_id ORDER BY b.id DESC""").fetchall()
        return {"items":[dict(r) for r in rows]}

@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        total = c.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
        passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"]
        buffered = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
        rejected = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
        buf_rows = c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
        # 对账也只认正式指针：停用历史版不可再裁
        ptr_rows = c.execute("SELECT subject, current_version FROM contract_pointers ORDER BY subject").fetchall()
    return {
        "ingress_total": total,
        "passed": passed,
        "buffered": buffered,
        "rejected": rejected,
        "buffer_rows": buf_rows,
        "aligned": buffered == buf_rows,
        "pointers": [dict(r) for r in ptr_rows],
    }

class RollbackIn(BaseModel):
    subject: str
    target_version: str
    path: str  # 甲 | 乙
    source: str = "manual"  # manual | scheduler

@app.post("/api/rollback")
def rollback(body: RollbackIn, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    if body.path not in ("甲", "乙"):
        raise HTTPException(400, "回滚路径必须是甲或乙，且只能走互斥一支")
    lk = rollback_lock(body.subject)
    if not lk.acquire(blocking=False):
        # 同主题两名值班同时抢回滚：后到只记回滚争用
        seq = ledger_record(subject=body.subject, target_version=body.target_version,
                            path=body.path, source=body.source, operator=operator,
                            status="回滚争用", detail="同主题已有回滚在执行，后到排队不入")
        raise HTTPException(409, f"回滚争用，处置簿序号{seq}")
    try:
        with connect() as c:
            today = date.today().isoformat()
            # 当天已选定路径不许换：偏离处置簿直接拒绝
            booked = c.execute(
                "SELECT path, status FROM rollback_ledger WHERE subject=? AND created_day=? AND status='已回滚' ORDER BY seq DESC LIMIT 1",
                (body.subject, today)).fetchone()
            if booked and booked["path"] != body.path:
                seq = ledger_record(subject=body.subject, target_version=body.target_version,
                                    path=body.path, source=body.source, operator=operator,
                                    status="回滚偏了",
                                    detail=f"当天处置簿已选定{booked['path']}路径，不得混用{body.path}路径")
                raise HTTPException(409, f"回滚偏了，处置簿序号{seq}：当天只能走{booked['path']}路径")
            target = c.execute("SELECT * FROM contracts WHERE subject=? AND version=?",
                               (body.subject, body.target_version)).fetchone()
            if not target:
                seq = ledger_record(subject=body.subject, target_version=body.target_version,
                                    path=body.path, source=body.source, operator=operator,
                                    status="回滚偏了", detail="目标版本不存在或不属于该主题")
                raise HTTPException(404, f"回滚偏了，处置簿序号{seq}：目标版本不存在")
            if target["track"] != "正式":
                seq = ledger_record(subject=body.subject, target_version=body.target_version,
                                    path=body.path, source=body.source, operator=operator,
                                    status="回滚偏了", detail="影子契约不能作为回滚目标")
                raise HTTPException(400, f"回滚偏了，处置簿序号{seq}：仅正式契约可回滚")
            # 停用历史版不可再裁：已进历史的版本永远不能再被指针选中
            retired = c.execute("SELECT 1 FROM contract_history WHERE subject=? AND version=?",
                                (body.subject, body.target_version)).fetchone()
            if retired:
                seq = ledger_record(subject=body.subject, target_version=body.target_version,
                                    path=body.path, source=body.source, operator=operator,
                                    status="回滚偏了", detail="目标版已停用入历史，历史版不可再裁")
                raise HTTPException(400, f"回滚偏了，处置簿序号{seq}：停用历史版不可再裁")
            ptr = c.execute("SELECT * FROM contract_pointers WHERE subject=?", (body.subject,)).fetchone()
            if not ptr:
                seq = ledger_record(subject=body.subject, target_version=body.target_version,
                                    path=body.path, source=body.source, operator=operator,
                                    status="回滚偏了", detail="该主题无正式指针，无可回滚版本")
                raise HTTPException(404, f"回滚偏了，处置簿序号{seq}：无正式指针")
            if ptr["current_version"] == body.target_version:
                seq = ledger_record(subject=body.subject, target_version=body.target_version,
                                    path=body.path, source=body.source, operator=operator,
                                    status="回滚偏了", detail="目标版本即当前正式，指针不动不算回滚")
                raise HTTPException(409, f"回滚偏了，处置簿序号{seq}：只改文案不改指针按失败处理")

            # 先占处置簿序号（独立提交），后续切换在另一事务内原子完成，失败整体回滚
            seq = ledger_record(subject=body.subject, target_version=body.target_version,
                                path=body.path, source=body.source, operator=operator,
                                status="执行中", detail=f"{body.path}路径回滚开始")
            try:
                if body.path == "乙":
                    # 乙路径：目标先入回滚快照表再切指针
                    c.execute("""INSERT INTO rollback_snapshots(rollback_seq,subject,version,mode,track,required_fields,snapshot_kind,created_at)
                                 VALUES (?,?,?,?,?,?,?,?)""",
                              (seq, target["subject"], target["version"], target["mode"],
                               target["track"], target["required_fields"], "目标版回滚快照", now_text()))
                # 甲路径：回滚与停用当前正式同一事务，指针原子回到目标版
                old = c.execute("SELECT * FROM contracts WHERE subject=? AND version=?",
                                (body.subject, ptr["current_version"])).fetchone()
                c.execute("UPDATE contracts SET status='停用' WHERE id=?", (old["id"],))
                c.execute("""INSERT INTO contract_history(rollback_seq,subject,version,mode,track,required_fields,retired_at)
                             VALUES (?,?,?,?,?,?,?)""",
                          (seq, old["subject"], old["version"], old["mode"], old["track"],
                           old["required_fields"], now_text()))
                c.execute("UPDATE contracts SET status='启用' WHERE id=?", (target["id"],))
                c.execute("UPDATE contract_pointers SET current_version=?, updated_at=? WHERE subject=?",
                          (target["version"], now_text(), body.subject))
                if body.path == "甲":
                    # 甲路径同样留快照附页，保证附页与处置簿同源可核
                    c.execute("""INSERT INTO rollback_snapshots(rollback_seq,subject,version,mode,track,required_fields,snapshot_kind,created_at)
                                 VALUES (?,?,?,?,?,?,?,?)""",
                              (seq, target["subject"], target["version"], target["mode"],
                               target["track"], target["required_fields"], "甲路径原子切换快照", now_text()))
                c.execute("UPDATE rollback_ledger SET status='已回滚', detail=? WHERE seq=?",
                          (f"{body.path}路径：{old['version']}停用入历史，指针切到{target['version']}", seq))
                # 事务提交后再做不变量自检
                c.commit()
            except Exception:
                c.rollback()
                with connect() as cf:
                    ledger_insert(cf, subject=body.subject, target_version=body.target_version,
                                  path=body.path, source=body.source, operator=operator,
                                  status="回滚偏了", detail=f"事务中断，指针未移动（处置簿序号{seq}）")
                raise

        # 提交后独立连接核验：正式指针唯一、无双正式、停用版不再被命中
        with connect() as chk:
            dup = chk.execute("""SELECT subject FROM contract_pointers GROUP BY subject HAVING COUNT(*)>1""").fetchall()
            dual = chk.execute("""SELECT subject FROM contracts WHERE track='正式' AND status='启用'
                                  GROUP BY subject HAVING COUNT(*)>1""").fetchall()
            if dup or dual:
                raise HTTPException(500, "回滚偏了：正式指针不唯一，出现双正式")
            ptr_now = chk.execute("SELECT current_version FROM contract_pointers WHERE subject=?",
                                  (body.subject,)).fetchone()
            hit = chk.execute("""SELECT 1 FROM contracts ct JOIN contract_pointers p
                                 ON p.subject=ct.subject AND p.current_version=ct.version
                                 WHERE ct.subject=? AND ct.version=? AND ct.status='启用'""",
                              (body.subject, target["version"])).fetchone()
            if not ptr_now or ptr_now["current_version"] != target["version"] or not hit:
                raise HTTPException(500, "回滚偏了：指针未落到目标版或目标版未启用")
        return {"ok": True, "seq": seq, "subject": body.subject,
                "path": body.path, "current_version": target["version"],
                "retired_version": old["version"]}
    finally:
        lk.release()

@app.get("/api/rollback/ledger")
def rollback_ledger(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM rollback_ledger ORDER BY seq").fetchall()
        return {"items":[dict(r) for r in rows]}

@app.get("/api/rollback/snapshots")
def rollback_snapshots(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM rollback_snapshots ORDER BY id").fetchall()
        return {"items":[dict(r) for r in rows]}

@app.get("/api/rollback/history")
def rollback_history(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM contract_history ORDER BY id").fetchall()
        return {"items":[dict(r) for r in rows]}

@app.get("/api/rollback/pointers")
def rollback_pointers(authorization: str | None = Header(default=None)):
    """调度任务可核：每主题正式指针唯一，目录/入站/详情/对账同源。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM contract_pointers ORDER BY subject").fetchall()
        pointers = [dict(r) for r in rows]
        dup_subject = [r["subject"] for r in c.execute(
            "SELECT subject FROM contract_pointers GROUP BY subject HAVING COUNT(*)>1").fetchall()]
        dual_official = [r["subject"] for r in c.execute(
            "SELECT subject FROM contracts WHERE track='正式' AND status='启用' GROUP BY subject HAVING COUNT(*)>1").fetchall()]
        # 停用历史版不可再裁：历史表里的版本必须保持停用
        revived = [dict(r) for r in c.execute("""
            SELECT h.version, h.subject FROM contract_history h
            JOIN contracts ct ON ct.version=h.version AND ct.subject=h.subject
            WHERE ct.status='启用'""").fetchall()]
        return {"pointers": pointers,
                "unique": not dup_subject and not dual_official and not revived,
                "dup_pointer_subjects": dup_subject,
                "dual_official_subjects": dual_official,
                "revived_history_versions": revived}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
