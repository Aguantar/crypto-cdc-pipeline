#!/usr/bin/env python3
"""토픽 스키마 계약 검사 (docs/34 #8, schemas/).

왜: Debezium·수집기가 schemas.enable=false 로 스키마 없는 JSON 을 보낸다. 생산자가 필드 이름·타입을 바꿔도
    하류는 런타임에야 안다(그것도 운이 좋아야). 이 검사가 "약속한 모양"과 실제 메시지를 대조한다.
사용: python3 scripts/ops/validate-topic-schemas.py [--samples 5] [--topic <이름>]
반환: 위반이 하나라도 있으면 exit 1 (CI·배포 전 게이트로 쓸 수 있다).
의존성 없음 - JSON Schema 의 부분집합(type/required/properties/items/oneOf/enum)만 직접 검사한다.
"""
import argparse, datetime, glob, json, os, subprocess, sys

TYPES = {'object': dict, 'array': list, 'string': str, 'integer': int, 'number': (int, float), 'boolean': bool, 'null': type(None)}


def check(node, schema, path='$'):
    """스키마 위반 목록을 돌려준다. 빈 리스트면 통과."""
    errs = []
    if 'oneOf' in schema:
        alts = [check(node, s, path) for s in schema['oneOf']]
        if all(a for a in alts):
            errs.append(f"{path}: oneOf 중 어느 것도 만족 못 함 ({[a[0] for a in alts]})")
        return errs
    t = schema.get('type')
    if t:
        allowed = t if isinstance(t, list) else [t]
        py = tuple(x for name in allowed for x in (TYPES[name] if isinstance(TYPES[name], tuple) else (TYPES[name],)))
        # bool 은 int 의 하위형이라 integer 로 통과하지 않게
        if isinstance(node, bool) and 'boolean' not in allowed:
            errs.append(f"{path}: boolean 인데 {allowed} 를 기대"); return errs
        if not isinstance(node, py):
            errs.append(f"{path}: {type(node).__name__} 인데 {allowed} 를 기대"); return errs
    if 'enum' in schema and node not in schema['enum']:
        errs.append(f"{path}: '{node}' 는 허용값 {schema['enum']} 밖")
    if isinstance(node, dict):
        for r in schema.get('required', []):
            if r not in node:
                errs.append(f"{path}.{r}: 필수 필드 없음")
        for k, sub in schema.get('properties', {}).items():
            if k in node and node[k] is not None or (k in node and 'null' in (sub.get('type') or [])):
                errs += check(node[k], sub, f"{path}.{k}")
            elif k in node and node[k] is None:
                tt = sub.get('type'); allowed = tt if isinstance(tt, list) else [tt]
                if 'null' not in allowed:
                    errs.append(f"{path}.{k}: null 인데 {allowed} 를 기대")
    if isinstance(node, list) and 'items' in schema and node:
        errs += check(node[0], schema['items'], f"{path}[0]")
    return errs


def sample(topic, n, timeout_ms=20000, from_beginning=False):
    cmd = ['docker', 'exec', 'cdc-kafka-1', 'kafka-console-consumer', '--bootstrap-server', 'kafka-1:29092',
           '--topic', topic, '--max-messages', str(n), '--timeout-ms', str(timeout_ms)]
    if from_beginning:
        cmd.append('--from-beginning')
    out = subprocess.run(cmd, capture_output=True, text=True).stdout
    rows = []
    for line in out.splitlines():
        line = line.strip()
        if not line or line == 'null':   # tombstone
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--samples', type=int, default=3)
    ap.add_argument('--topic')
    ap.add_argument('--self-test', action='store_true', help='검증기가 위반을 실제로 잡는지 확인(계약 파일·토픽 불필요)')
    ap.add_argument('--record', action='store_true', help='결과를 cdc_pipeline.schema_validation_runs 에 적는다(cron 용)')
    a = ap.parse_args()
    if a.self_test:
        sch = json.load(open(os.path.join(os.path.dirname(__file__), '..', '..', 'schemas', 'binance.trades.v1.json')))
        good = {"e": "trade", "E": 1, "s": "BTCUSDT", "t": 1, "p": "1.0", "q": "1.0", "T": 1, "m": True, "M": True, "recv_ms": 1}
        cases = [("정상", good, 0),
                 ("필수 필드 누락(recv_ms)", {k: v for k, v in good.items() if k != 'recv_ms'}, 1),
                 ("타입 변경(p 가 문자열→숫자)", {**good, 'p': 1.0}, 1),
                 ("허용값 밖(e=aggTrade)", {**good, 'e': 'aggTrade'}, 1),
                 ("null 인데 non-null 기대", {**good, 's': None}, 1)]
        bad = 0
        for name, msg, expect in cases:
            errs = check(msg, sch)
            ok = (len(errs) > 0) == (expect > 0)
            print(f"  {'PASS' if ok else 'FAIL'}  {name:34s} 위반 {len(errs)}건 {errs[:1]}")
            bad += 0 if ok else 1
        print(f"\n자체 시험: {'통과 - 검증기가 위반을 잡는다' if not bad else '실패'}")
        sys.exit(1 if bad else 0)
    root = os.path.join(os.path.dirname(__file__), '..', '..', 'schemas')
    failed = 0
    records = []
    ran_at = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0, tzinfo=None).isoformat(sep=' ')
    for f in sorted(glob.glob(os.path.join(root, '*.json'))):
        topic = os.path.basename(f)[:-5]
        if a.topic and a.topic != topic:
            continue
        schema = json.load(open(f))
        msgs = sample(topic, a.samples)
        if not msgs:
            # 원장·DLQ 처럼 메시지가 드문 토픽은 끝에서 읽으면 표본이 안 잡힌다 → 처음부터 다시
            msgs = sample(topic, a.samples, from_beginning=True)
        if not msgs:
            print(f"  {topic:38s} 표본 없음 (토픽이 조용하거나 tombstone 뿐) - 건너뜀")
            records.append({'ran_at': ran_at, 'topic': topic, 'status': 'no_sample', 'checked': 0, 'violations': 0, 'detail': ''})
            continue
        errs = []
        for i, m in enumerate(msgs):
            errs += [f"[{i}] {e}" for e in check(m, schema)]
        if errs:
            failed += 1
            print(f"  {topic:38s} 위반 {len(errs)}건")
            for e in errs[:6]:
                print(f"      {e}")
            records.append({'ran_at': ran_at, 'topic': topic, 'status': 'violation', 'checked': len(msgs),
                            'violations': len(errs), 'detail': ' | '.join(errs[:6])[:1000]})
        else:
            print(f"  {topic:38s} OK ({len(msgs)}건 검사) - {schema.get('title','')}")
            records.append({'ran_at': ran_at, 'topic': topic, 'status': 'ok', 'checked': len(msgs), 'violations': 0, 'detail': ''})
    if a.record and records:
        payload = '\n'.join(json.dumps(r, ensure_ascii=False) for r in records)
        p = subprocess.run(['docker', 'exec', '-i', 'cdc-clickhouse', 'clickhouse-client', '-q',
                            'INSERT INTO cdc_pipeline.schema_validation_runs FORMAT JSONEachRow'],
                           input=payload, text=True, capture_output=True)
        # 적재 실패를 조용히 넘기면 "위반 없음"과 "검사를 못 했음"이 구분되지 않는다
        if p.returncode != 0:
            print(f"  기록 실패: {p.stderr.strip()[:200]}")
            sys.exit(2)
        print(f"  기록 {len(records)}건 → cdc_pipeline.schema_validation_runs")
    print(f"\n결과: {'위반 있음' if failed else '전부 계약대로'}")
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()
