"""독립 WS 클라이언트: KRW 전 마켓 trade 구독, 받은 모든 (cd, sid, ttms, recv_ms)를 기록. DB 없음, 즉시 파일 append."""
import asyncio, json, time, uuid, sys, websockets
markets=[m.strip() for m in open('out/krw_markets.txt').read().split(',') if m.strip()]
dur=int(sys.argv[1]); out=open(sys.argv[2],'w'); n=0; snap=0; t_end=time.time()+dur
async def main():
    global n, snap
    async with websockets.connect('wss://api.upbit.com/websocket/v1', ping_interval=30, ping_timeout=10, max_size=2**23, max_queue=4096) as ws:
        await ws.send(json.dumps([{"ticket":str(uuid.uuid4())[:8]},{"type":"trade","codes":markets,"isOnlyRealtime":True},{"format":"SIMPLE"}]))
        while time.time()<t_end:
            try: raw=await asyncio.wait_for(ws.recv(), timeout=max(0.1,t_end-time.time()))
            except asyncio.TimeoutError: break
            d=json.loads(raw if isinstance(raw,str) else raw.decode())
            if d.get('ty')!='trade': continue
            if d.get('st')=='SNAPSHOT': snap+=1; continue
            out.write(f"{d['cd']}\t{d['sid']}\t{d['ttms']}\t{int(time.time()*1000)}\n"); n+=1
    out.close(); print(json.dumps({"trades":n,"snapshots_skipped":snap,"start":t_end-dur,"end":time.time()}))
asyncio.run(main())
