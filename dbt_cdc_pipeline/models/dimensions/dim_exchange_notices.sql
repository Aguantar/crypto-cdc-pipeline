{{ config(materialized='table', order_by='(listed_at, notice_id)') }}
-- 거래소 공지 분류 (docs/33 "점검 공지로 유실 창을 라벨링", 2026-09-20).
--
-- 왜 카테고리를 안 쓰나 (실측 1,200건, 2025-06 ~ 2026-09): 업비트 `category='점검'` 은 대부분
-- 신분증 진위확인·원화 입출금이라 거래를 멈추지 않는다. 카테고리로 거르면 거래 중단을 놓치고
-- 멀쩡한 시간을 점검으로 오인한다. 실제로 거래가 멈추는 것은 제목 두 가지뿐이다:
--   ① '서버 점검 안내'  → 전 마켓(KRW·BTC·USDT) 정지. 15개월에 3건
--   ② '… 거래 지원 일시 중단' → 그 코인만. 리브랜딩·토큰 스왑. 15개월에 5건
--
-- 창 추출: '서버 점검 안내' 본문에는 시각이 `2026-07-06(월) 02:00` 형태로 여러 번 나오는데
-- 최소가 점검 시작, 최대가 거래 재개다(3건 전부 확인). 본문 시각은 KST 이므로 9시간을 뺀다.
-- 코인별 중단은 재개 시점만 적히는 경우가 있어 창을 만들지 않는다 - 없는 값을 지어내지 않는다.
WITH base AS (
    SELECT notice_id, listed_at, category, title, body_text, url,
           multiIf(
               match(title, '^서버 점검 안내'), 'exchange_wide',
               match(title, '거래\\s?지원\\s?(일시\\s?)?중단'), 'market',
               match(title, '입출금|입금|출금'), 'deposit_withdrawal',
               'other') AS scope,
           -- 본문의 'YYYY-MM-DD(요일) HH:MM' 전부. 전체 점검은 시작·재개가 같이 적힌다
           -- OrZero 를 쓰는 이유: OrNull 은 Nullable(DateTime) 배열을 만드는데 arrayMin/Max 가 그 타입을 거부한다.
           -- 파싱 못 한 것은 0 이 되므로 뒤에서 걸러낸다
           arrayFilter(x -> x > toDateTime(0),
               arrayMap(x -> parseDateTimeBestEffortOrZero(concat(substring(x, 1, 10), ' ', substring(x, -5))),
                        extractAll(body_text, '20[0-9]{2}-[0-9]{2}-[0-9]{2}\\([월화수목금토일]\\) [0-9]{2}:[0-9]{2}'))) AS kst_times
    FROM {{ source('reference', 'exchange_notices') }} FINAL
)
SELECT notice_id, listed_at, category, title, scope, url,
       toUInt8(scope IN ('exchange_wide', 'market'))                    AS halts_trading,
       -- 제목 괄호 안의 티커: '스토리(IP)' → IP. 코인별 중단을 마켓에 연결하는 열쇠
       arrayFilter(t -> length(t) BETWEEN 2 AND 10, extractAll(title, '\\(([A-Z0-9]{2,10})\\)')) AS tickers,
       if(scope = 'exchange_wide' AND length(kst_times) >= 2,
          arrayMin(kst_times) - INTERVAL 9 HOUR, NULL)                  AS window_start_utc,
       if(scope = 'exchange_wide' AND length(kst_times) >= 2,
          arrayMax(kst_times) - INTERVAL 9 HOUR, NULL)                  AS window_end_utc,
       substring(body_text, 1, 300)                                     AS body_head
FROM base
