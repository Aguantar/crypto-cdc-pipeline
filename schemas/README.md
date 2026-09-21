# 토픽 스키마 계약 (docs/34 #8)

왜: Debezium·수집기가 `schemas.enable=false` 로 **스키마 없는 JSON** 을 보낸다. 09-19 에 컬럼 3개를 추가할 때 하류가 안 깨진 건
운이 아니라 파서가 이름으로 읽기 때문이었지만, **깨졌어도 알 방법이 없었다**. 이 파일들이 "생산자가 약속한 모양"이다.

- 형식: JSON Schema 부분집합(`type`·`required`·`properties`·`items`·`oneOf`). 외부 의존성 없이 `scripts/ops/validate-topic-schemas.py` 가 검사한다.
- 검사: `scripts/ops/validate-topic-schemas.py` - 각 토픽에서 최근 메시지를 표본으로 떠 계약과 대조. CI·배포 전에 돌린다.
- 바꿀 때: 생산자를 바꾸면 **여기를 먼저 고치고** 검사를 돌린다. 필드 추가는 호환(파서가 이름으로 읽음), 이름 변경·타입 변경은 비호환이다.
- 레지스트리(Schema Registry)를 안 쓰는 이유: 토픽 6개·생산자 3개 규모에서 운영 비용이 이득보다 크다(docs/29 §4). 파일 + 검사로 같은 효과를 낸다.
