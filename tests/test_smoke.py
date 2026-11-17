
import threading

import pytest
from fastapi.testclient import TestClient

from app import app, init_db
import app as appmod


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def h(client):
    tok = client.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return {"Authorization": f"Bearer {tok}"}


def test_validate_paths(client, h):
    # strict reject missing
    r = client.post("/api/ingress/validate", headers=h,
                    json={"subject": "订单入站", "payload_fields": "order_id,amount"})
    assert r.status_code == 200 and r.json()["result"] == "拒收"
    ok = client.post("/api/ingress/validate", headers=h,
                     json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert ok.json()["result"] == "通过"
    assert client.get("/api/reconcile", headers=h).json()["passed"] >= 1


def _rollback(client, h, path, target, subject="订单入站", source="manual"):
    return client.post("/api/rollback", headers=h,
                       json={"subject": subject, "target_version": target, "path": path, "source": source})


def _seed_second_formal_version():
    """造一个启用中的新版正式契约 v5，使 v3 可作为回滚目标。"""
    with appmod.connect() as c:
        c.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                  ("v5", "订单入站", "严格", "正式", "停用", "order_id,amount,currency,channel"))
        # 模拟 v5 曾是正式：先启 v5、停 v3、切指针
        c.execute("UPDATE contracts SET status='停用' WHERE version='v3'")
        c.execute("UPDATE contracts SET status='启用' WHERE version='v5'")
        c.execute("UPDATE contract_pointers SET current_version='v5' WHERE subject='订单入站'")
        c.commit()


def test_rollback_path_a_atomic_pointer_switch(client, h):
    _seed_second_formal_version()
    # 回滚前入站命中 v5 的必填（带 channel 才通过）
    miss = client.post("/api/ingress/validate", headers=h,
                       json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert miss.json()["version"] == "v5" and miss.json()["result"] == "拒收"

    r = _rollback(client, h, "甲", target="v3")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["path"] == "甲" and body["current_version"] == "v3" and body["retired_version"] == "v5"

    # 回滚后入站只认 v3 必填，不再命中停用的 v5
    ok = client.post("/api/ingress/validate", headers=h,
                     json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert ok.json()["result"] == "通过" and ok.json()["version"] == "v3"

    # 正式指针唯一、无双正式；种子里库存主题 v2 为停用态故无指针行
    chk = client.get("/api/rollback/pointers", headers=h).json()
    assert chk["unique"] is True
    ptrs = {p["subject"]: p["current_version"] for p in chk["pointers"]}
    assert ptrs == {"订单入站": "v3"}
    contracts = {x["version"]: x for x in client.get("/api/contracts", headers=h).json()["items"]}
    assert contracts["v3"]["status"] == "启用" and contracts["v3"]["is_pointer"] == 1
    assert contracts["v5"]["status"] == "停用" and contracts["v5"]["is_pointer"] == 0

    # 处置簿与快照/历史附页同源
    ledger = client.get("/api/rollback/ledger", headers=h).json()["items"]
    done = [x for x in ledger if x["status"] == "已回滚"]
    assert len(done) == 1 and done[0]["path"] == "甲"
    hist = client.get("/api/rollback/history", headers=h).json()["items"]
    assert [x["version"] for x in hist] == ["v5"]
    snaps = client.get("/api/rollback/snapshots", headers=h).json()["items"]
    assert [x["version"] for x in snaps] == ["v3"]


def test_rollback_path_b_snapshot_then_switch(client, h):
    _seed_second_formal_version()
    r = _rollback(client, h, "乙", target="v3", source="scheduler")
    assert r.status_code == 200
    assert r.json()["path"] == "乙"
    snaps = client.get("/api/rollback/snapshots", headers=h).json()["items"]
    assert len(snaps) == 1 and snaps[0]["version"] == "v3" and snaps[0]["snapshot_kind"] == "目标版回滚快照"
    ledger = [x for x in client.get("/api/rollback/ledger", headers=h).json()["items"] if x["status"] == "已回滚"]
    assert ledger[0]["source"] == "scheduler"
    # 调度场景下目录与入站详情只认唯一正式指针
    chk = client.get("/api/rollback/pointers", headers=h).json()
    assert chk["unique"] is True
    detail = client.post("/api/ingress/validate", headers=h,
                         json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"}).json()
    assert detail["version"] == "v3"


def test_path_mix_same_day_rejected_and_logged(client, h):
    _seed_second_formal_version()
    assert _rollback(client, h, "甲", target="v3").status_code == 200
    # 造一个全新正式版 v6 承接指针（v5 已入历史不可复活），再试换路径
    with appmod.connect() as c:
        c.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                  ("v6", "订单入站", "严格", "正式", "停用", "order_id,amount,currency,channel,tenant"))
        c.execute("UPDATE contracts SET status='启用' WHERE version='v6'")
        c.execute("UPDATE contracts SET status='停用' WHERE version='v3'")
        c.execute("UPDATE contract_pointers SET current_version='v6' WHERE subject='订单入站'")
        c.commit()
    r = _rollback(client, h, "乙", target="v3")
    assert r.status_code == 409 and "回滚偏了" in r.json()["detail"]
    bad = [x for x in client.get("/api/rollback/ledger", headers=h).json()["items"] if x["status"] == "回滚偏了"]
    assert bad and "不得混用" in bad[0]["detail"]
    # 被拒后指针不动，仍是 v6 为正式
    ptr = client.get("/api/rollback/pointers", headers=h).json()
    assert any(p["subject"] == "订单入站" and p["current_version"] == "v6" for p in ptr["pointers"])


def test_text_only_change_without_pointer_moves_fails(client, h):
    _seed_second_formal_version()
    # 目标即当前正式：只改文案不动指针 → 失败并记偏了
    r = _rollback(client, h, "甲", target="v5")
    assert r.status_code == 409 and "回滚偏了" in r.json()["detail"]
    ledger = client.get("/api/rollback/ledger", headers=h).json()["items"]
    assert all(x["status"] != "已回滚" for x in ledger)


def test_shadow_contract_cannot_be_rollback_target(client, h):
    r = _rollback(client, h, "甲", target="v4-shadow")
    assert r.status_code == 400 and "回滚偏了" in r.json()["detail"]


def test_retired_history_version_cannot_be_retrimmed(client, h):
    _seed_second_formal_version()
    _rollback(client, h, "甲", target="v3")
    # v5 已停用入历史，不可再裁：回滚目标选 v5 必须被拒并记“回滚偏了”
    r = _rollback(client, h, "甲", target="v5")
    assert r.status_code == 400 and "不可再裁" in r.json()["detail"]
    bad = [x for x in client.get("/api/rollback/ledger", headers=h).json()["items"] if x["status"] == "回滚偏了"]
    assert any("不可再裁" in x["detail"] for x in bad)
    chk = client.get("/api/rollback/pointers", headers=h).json()
    assert chk["unique"] is True and not chk["revived_history_versions"]
    contracts = {x["version"]: x for x in client.get("/api/contracts", headers=h).json()["items"]}
    assert contracts["v5"]["status"] == "停用"


def test_concurrent_rollback_contention_queued(client, h):
    _seed_second_formal_version()
    lk = appmod.rollback_lock("订单入站")
    lk.acquire()  # 先到值班持锁中，后到值班必须排队并只记“回滚争用”
    box = {}

    def worker():
        c = TestClient(app)
        tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
        rr = c.post("/api/rollback", headers={"Authorization": f"Bearer {tok}"},
                    json={"subject": "订单入站", "target_version": "v3", "path": "甲"})
        box["status"] = rr.status_code
        box["body"] = rr.json()

    t = threading.Thread(target=worker)
    t.start(); t.join()
    lk.release()
    assert box["status"] == 409 and "回滚争用" in box["body"]["detail"]
    ledger = client.get("/api/rollback/ledger", headers=h).json()["items"]
    assert any(x["status"] == "回滚争用" for x in ledger)
    # 后到一支未落任何指针变更：仍只有原指针 v5
    done = [x for x in ledger if x["status"] == "已回滚"]
    assert done == []
    chk = client.get("/api/rollback/pointers", headers=h).json()
    assert chk["unique"] is True
    assert any(p["subject"] == "订单入站" and p["current_version"] == "v5" for p in chk["pointers"])


def test_no_dual_official_and_stopped_version_never_hit(client, h):
    _seed_second_formal_version()
    assert _rollback(client, h, "乙", target="v3").status_code == 200
    with appmod.connect() as c:
        enabled = [dict(r) for r in c.execute(
            "SELECT version FROM contracts WHERE subject='订单入站' AND track='正式' AND status='启用'")]
        assert len(enabled) == 1 and enabled[0]["version"] == "v3"
        ptr = dict(c.execute("SELECT * FROM contract_pointers WHERE subject='订单入站'").fetchone())
        assert ptr["current_version"] == "v3"
    # 入站绝不命中停用版 v5 的 channel 必填
    r = client.post("/api/ingress/validate", headers=h,
                    json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert r.json()["version"] == "v3" and r.json()["result"] == "通过"


def test_catalog_ingress_detail_reconcile_share_same_pointer(client, h):
    _seed_second_formal_version()
    _rollback(client, h, "甲", target="v3")
    catalog = {x["version"]: x for x in client.get("/api/contracts", headers=h).json()["items"]}
    ptrs = {p["subject"]: p["current_version"] for p in
            client.get("/api/rollback/pointers", headers=h).json()["pointers"]}
    recon = client.get("/api/reconcile", headers=h).json()
    recon_ptrs = {p["subject"]: p["current_version"] for p in recon["pointers"]}
    assert ptrs == recon_ptrs
    assert catalog[ptrs["订单入站"]]["is_pointer"] == 1
    detail = client.post("/api/ingress/validate", headers=h,
                         json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"}).json()
    assert detail["version"] == ptrs["订单入站"]
