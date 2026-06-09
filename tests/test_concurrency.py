"""Optimistic locking: concurrent writers race for one task; exactly one wins."""
import asyncio, httpx

BASE = "http://127.0.0.1:8000"


async def main():
    async with httpx.AsyncClient(base_url=BASE, timeout=10) as c:
        cid = (await c.post("/api/v1/customers", json={"name": "Acme"})).json()["id"]
        h = {"X-Customer-Id": cid}
        pid = (await c.post("/api/v1/projects", json={"name": "P"}, headers=h)).json()["id"]
        A = (await c.post(f"/api/v1/projects/{pid}/tasks", headers=h, json={
            "name": "A", "planned_start": "2026-07-01T09:00:00Z",
            "planned_completion": "2026-07-01T17:00:00Z"})).json()

        # 5 managers move the SAME task at once, all holding version 1
        async def move(i):
            r = await c.patch(f"/api/v1/tasks/{A['id']}/schedule",
                              headers={**h, "If-Match": "1"},
                              json={"planned_start": f"2026-07-0{i+2}T09:00:00Z",
                                    "planned_completion": f"2026-07-0{i+2}T17:00:00Z"})
            return r.status_code

        codes = await asyncio.gather(*[move(i) for i in range(5)])
        print("5 concurrent moves -> status codes:", sorted(codes))
        assert codes.count(200) == 1, f"expected exactly ONE winner, got {codes}"
        assert codes.count(409) == 4, f"expected 4 rejected, got {codes}"

        g = (await c.get(f"/api/v1/projects/{pid}/gantt", headers=h)).json()
        a = next(t for t in g["tasks"] if t["id"] == A["id"])
        print("final version:", a["version"], "(exactly one increment)")
        assert a["version"] == 2, f"version corrupted: {a['version']}"

        # stale client (holds v1, current is v2) -> 409
        r = await c.patch(f"/api/v1/tasks/{A['id']}/schedule", headers={**h, "If-Match": "1"},
                          json={"planned_start": "2026-08-01T09:00:00Z",
                                "planned_completion": "2026-08-01T17:00:00Z"})
        print("stale If-Match ->", r.status_code)
        assert r.status_code == 409

        # missing If-Match -> 428
        r = await c.patch(f"/api/v1/tasks/{A['id']}/schedule", headers=h,
                          json={"planned_start": "2026-08-01T09:00:00Z",
                                "planned_completion": "2026-08-01T17:00:00Z"})
        print("missing If-Match ->", r.status_code)
        assert r.status_code == 428
    print("\nCONCURRENCY (optimistic lock) PASSED ✅  one writer wins, the rest 409")


asyncio.run(main())