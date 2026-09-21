-- 유일성 계약: (market, sequential_id) 는 업비트 체결의 자연키다. sequential_id 단독은 마켓 간 충돌 (docs/13).
-- 범위를 전날(KST)로 한정하는 이유: 전체 1억 행 유일성은 1.75GB ClickHouse 에서 매일 돌릴 수 없고,
-- 과거분은 07·13 감사에서 한 번 검증했다. 매일 새로 들어온 창만 검사하는 것이 증분 테스트의 관행.
SELECT market, sequential_id, count() AS n
FROM {{ ref('stg_trades') }}
WHERE day_kst = yesterday()
GROUP BY market, sequential_id
HAVING n > 1
-- 2026-09-20: 단독 실행은 통과하는데 전체 dbt test 에서만 code 241 로 죽었다.
-- 원인은 쿼리 한도(600MB)가 아니라 서버 총 메모리 한도 1.57GiB 였다 - 하루 1,600만 행을
-- (market, sequential_id) 로 묶으면 그룹이 1,600만 개라 해시테이블이 RAM 에 다 올라가고,
-- Flink 적재·다른 테스트와 겹치는 순간 총량이 넘는다. 품질 테스트가 부하 때 실패하면 거짓 경보가 된다.
-- → 300MB 를 넘으면 디스크로 흘려 집계한다(느려지지만 메모리 상한이 고정된다).
SETTINGS max_bytes_before_external_group_by = 300000000, max_memory_usage = 600000000, max_threads = 2
