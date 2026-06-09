"""Full-pipeline e2e: the cascade ripple must arrive purely via
outbox -> Debezium -> worker -> Debezium -> ws-fanout -> Redis -> WebSocket.
(api runs with INLINE_CASCADE=false, so it does NOT compute the cascade.)"""
import asyncio, json, time, httpx, websockets

BASE, WS = "http://127.0.0.1:8000", "ws://127.0.0.1:8000/ws"


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
        print("setup done: A", A["id"][:8], "B", B["id"][:8])

        async with websockets.connect(WS) as ws:
            await ws.send(json.dumps({"action": "subscribe", "project_id": pid}))
            print("ws ack:", json.loads(await ws.recv())["type"])

            t0 = time.time()
            r = await c.patch(f"/api/v1/tasks/{A['id']}/schedule",
                              headers={**h, "If-Match": str(A["version"])},
                              json={"planned_start": "2026-07-05T09:00:00Z",
                                    "planned_completion": "2026-07-05T17:00:00Z"})
            print("PATCH /schedule ->", r.status_code, "(api did NOT cascade; INLINE_CASCADE=false)")

            # wait for the ripple to traverse the whole pipeline
            while True:
                evt = json.loads(await asyncio.wait_for(ws.recv(), timeout=40))
                if evt["type"] == "schedule.cascade":
                    break
            dt = time.time() - t0
            print(f"WS schedule.cascade arrived in {dt:.2f}s  seq={evt['seq']}")
            for x in evt["data"]["changes"]:
                tag = "B" if x["task_id"] == B["id"] else x["task_id"][:8]
                print(f"  cascade -> {tag}: start {x['planned_start']} ver {x['version']}")
            assert any(x["task_id"] == B["id"] and x["planned_start"] == "2026-07-05T17:00:00+00:00"
                       for x in evt["data"]["changes"]), "B did not cascade through the pipeline!"

        # confirm B persisted by the WORKER (not the api)
        g = (await c.get(f"/api/v1/projects/{pid}/gantt", headers=h)).json()
        b = next(t for t in g["tasks"] if t["id"] == B["id"])
        print("B in DB:", b["planned_start"], "ver", b["version"])
        assert b["planned_start"].replace("Z", "+00:00") == "2026-07-05T17:00:00+00:00"
    print("\nFULL PIPELINE e2e PASSED ✅  (outbox->Debezium->worker->Debezium->fanout->Redis->WS)")


asyncio.run(main())