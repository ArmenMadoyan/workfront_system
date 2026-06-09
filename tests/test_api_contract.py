import asyncio, json, httpx, websockets

BASE = "http://127.0.0.1:8000"
WS = "ws://127.0.0.1:8000/ws"


async def main():
    async with httpx.AsyncClient(base_url=BASE, timeout=10) as c:
        cid = (await c.post("/api/v1/customers", json={"name": "Acme"})).json()["id"]
        h = {"X-Customer-Id": cid}
        pid = (await c.post("/api/v1/projects", json={"name": "Launch"}, headers=h)).json()["id"]

        A = (await c.post(f"/api/v1/projects/{pid}/tasks", headers=h, json={
            "name": "A", "planned_start": "2026-07-01T09:00:00Z",
            "planned_completion": "2026-07-01T17:00:00Z"})).json()
        B = (await c.post(f"/api/v1/projects/{pid}/tasks", headers=h, json={
            "name": "B", "planned_start": "2026-07-01T17:00:00Z",
            "planned_completion": "2026-07-02T01:00:00Z"})).json()
        await c.post("/api/v1/predecessors", headers=h, json={
            "predecessor_id": A["id"], "successor_id": B["id"], "type": "FS"})
        print("setup: A", A["id"][:8], "B", B["id"][:8], "ver A", A["version"])

        # subscribe BEFORE the move
        async with websockets.connect(WS) as ws:
            await ws.send(json.dumps({"action": "subscribe", "project_id": pid}))
            ack = json.loads(await ws.recv())
            print("ws ack:", ack["type"], "seq", ack["seq"])

            # move A 4 days later
            r = await c.patch(f"/api/v1/tasks/{A['id']}/schedule",
                              headers={**h, "If-Match": str(A["version"])},
                              json={"planned_start": "2026-07-05T09:00:00Z",
                                    "planned_completion": "2026-07-05T17:00:00Z"})
            print("PATCH /schedule ->", r.status_code, "| A new start", r.json()["planned_start"], "ver", r.json()["version"])

            evt = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
            print("WS event:", evt["type"], "seq", evt["seq"])
            ch = evt["data"]["changes"]
            print("  origin:", evt["data"]["origin_task_id"][:8])
            for x in ch:
                tag = "B" if x["task_id"] == B["id"] else x["task_id"][:8]
                print(f"  cascade -> {tag}: start {x['planned_start']} end {x['planned_completion']} ver {x['version']}")
            assert any(x["task_id"] == B["id"] and x["planned_start"] == "2026-07-05T17:00:00Z" for x in ch), "B did not cascade!"

        # stale If-Match -> 409
        r2 = await c.patch(f"/api/v1/tasks/{A['id']}/schedule",
                           headers={**h, "If-Match": "1"},
                           json={"planned_start": "2026-08-01T09:00:00Z",
                                 "planned_completion": "2026-08-01T17:00:00Z"})
        print("stale If-Match ->", r2.status_code, "(expect 409)")
        assert r2.status_code == 409
        print("\nALL CONTRACT CHECKS PASSED ✅")


asyncio.run(main())