-- 거래소 공지 보관소 (docs/33 "점검 공지로 유실 창을 예정 점검으로 라벨링", 2026-09-20).
--
-- 왜 필요한가: 우리 수집이 비면 지금은 전부 "유실"로 보인다. 그런데 업비트 서버 점검 중에는
-- KRW·BTC·USDT 마켓 거래가 전부 멈춘다(2026-07-06 공지 본문: "점검 시간 내 업비트 서비스 전체 이용이 제한").
-- 코인별로는 리브랜딩·토큰 스왑으로 그 마켓만 거래지원이 중단된다. 둘 다 정상이고, 유실은 사고다.
-- 그 구분의 근거를 파이프라인 안에 둔다.
--
-- 실측(2026-09-20, 공지 1,200건 / 2025-06-24 ~ 2026-09-19):
--   category '점검' 은 대부분 신분증·원화 입출금이라 거래를 멈추지 않는다 - 카테고리만 보면 틀린다.
--   거래가 멈추는 것은 ① 제목이 '서버 점검 안내'(전체, 15개월에 2~3회) ② '… 거래 지원 일시 중단'(코인별, 6건).
--   그래서 분류는 카테고리가 아니라 제목·본문으로 한다(dbt dim_exchange_notices).
--
-- 본문을 통째로 보관하는 이유: 창 시각과 대상 마켓이 본문에만 있고, 공지는 사후에 갱신된다
-- ("거래 재개 시점 안내" 가 같은 글에 덧붙는다). 파싱을 나중에 고치려면 원문이 있어야 한다.
CREATE TABLE IF NOT EXISTS cdc_pipeline.exchange_notices
(
    notice_id    UInt32,
    listed_at    DateTime,              -- 게시 시각(KST → UTC 로 저장)
    updated_at   DateTime,              -- 우리가 마지막으로 받아온 시각
    category     LowCardinality(String),
    title        String,
    body_text    String,                -- HTML 제거한 본문
    url          String
)
ENGINE = ReplacingMergeTree(updated_at)   -- 공지는 사후 갱신된다 → 같은 id 의 최신본만 남긴다
ORDER BY notice_id;
