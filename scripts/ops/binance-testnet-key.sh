#!/usr/bin/env bash
# Binance Spot Testnet 용 Ed25519 키쌍 생성 (docs/28 B-4, 2026-09-19). 개인키는 secrets/(gitignore) 에만, 공개키는 테스트넷 사이트에 등록.
# 왜 Ed25519: Binance 권장(HMAC 은 비밀키가 서버에도 있고, Ed25519 는 공개키만 등록해 개인키가 우리 밖으로 안 나간다).
# 실행: scripts/ops/binance-testnet-key.sh  → 출력된 공개키를 https://testnet.binance.vision/ (GitHub 로그인) → "Generate HMAC_SHA256 Key" 옆 "Register Ed25519 public key" 에 붙여넣기 → 발급된 API Key 를 .env 의 BINANCE_API_KEY= 에.
set -euo pipefail
cd "$(dirname "$0")/../.."
mkdir -p secrets; chmod 700 secrets
PRIV=secrets/binance_testnet_ed25519.pem; PUB=secrets/binance_testnet_ed25519.pub.pem
if [ -f "$PRIV" ]; then echo "이미 있음: $PRIV (재생성하려면 지우고 다시)"; else
  openssl genpkey -algorithm ed25519 -out "$PRIV"; chmod 600 "$PRIV"; echo "개인키 생성: $PRIV (커밋 금지, secrets/ 는 gitignore)"
fi
openssl pkey -in "$PRIV" -pubout -out "$PUB"
echo "---- 아래 공개키 전체를 테스트넷 사이트에 등록 ----"; cat "$PUB"; echo "-----------------------------------------------"
grep -q '^BINANCE_API_KEY=' .env || echo 'BINANCE_API_KEY=' >> .env
echo "다음: 사이트가 준 API Key 를 .env 의 BINANCE_API_KEY= 뒤에 붙여넣고 알려주기. (.env 는 gitignore, 키는 테스트넷 전용이라 실돈과 무관)"
