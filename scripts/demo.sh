#!/usr/bin/env bash
# 시연 스크립트 — 서버(./gradlew run)가 떠 있는 상태에서 실행
#   ./scripts/demo.sh            : 상태 확인 + 오류 케이스 + 배치 수동 실행 + 로그 (DB·FTP 쓰기 없음)
#   ./scripts/demo.sh --order    : 위 + 실제 샘플 주문 전송 (ORDER_TB 적재 + FTP 영수증 업로드 — FTP 파일은 삭제 불가)
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${BASE_URL:-http://localhost:${SERVER_PORT:-8080}}"
LOG="logs/interface-$(date +%F).log"
pretty() { python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin), ensure_ascii=False, indent=2))'; }
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

step "1. 상태 확인  GET /health"
curl -sf "$BASE/health" | pretty

step "2. 오류 케이스: 깨진 XML → 400 XML_PARSE (DB·FTP 접근 없음)"
curl -s -w '\nHTTP %{http_code}\n' -X POST -H 'Content-Type: application/xml' --data '<HEADER><USER_ID>' "$BASE/orders"

step "3. 오류 케이스: GET /orders → 405"
curl -s -o /dev/null -w 'HTTP %{http_code}\n' "$BASE/orders"

if [[ "${1:-}" == "--order" ]]; then
  [[ -f secrets/sample.xml ]] || { echo "secrets/sample.xml 없음 — 먼저 ./gradlew fetchInfo"; exit 1; }
  step "4. 실제 샘플 주문 (제공 원본과 같은 EUC-KR 로 전송) → ORDER_TB + FTP 영수증"
  TMP="$(mktemp)"; trap 'rm -f "$TMP"' EXIT
  iconv -f UTF-8 -t EUC-KR secrets/sample.xml > "$TMP"
  curl -s -X POST -H 'Content-Type: application/xml; charset=EUC-KR' --data-binary @"$TMP" "$BASE/orders" \
    | python3 -c '
import json,sys
r=json.load(sys.stdin)
ids=r["orderIds"]
print("resultCode :", r["resultCode"], "-", r["message"])
print("주문번호   :", f"{ids[0]} ~ {ids[-1]} ({len(ids)}건)" if ids else "-")
print("영수증 파일:", r.get("receiptFile","-"))
from collections import Counter
print("제외 사유  :", dict(Counter(x["reason"] for x in r["excluded"])))'
fi

step "5. 운송 배치 수동 실행  POST /batch/shipments/run (주기 실행과 겹치면 409)"
curl -s -w '\nHTTP %{http_code}\n' -X POST "$BASE/batch/shipments/run" | python3 -c '
import sys,json
t=sys.stdin.read(); body,code=t.rsplit("\nHTTP",1)
try:
    d=json.loads(body); print(d.get("resultCode"), "-", d.get("message"), "| ID", d.get("shipmentIdRange"), "| HTTP", code.strip())
except ValueError: print(t)'

step "6. 모니터링 로그 (최근 3건)  $LOG"
tail -n 3 "$LOG" | python3 -c '
import json,sys
for l in sys.stdin:
    d=json.loads(l)
    steps=" > ".join("%s(%s)" % (s["step"], s["status"]) for s in d.get("steps",[]))
    print(d["timestamp"][11:19], d["interfaceId"], d["resultCode"].ljust(15), str(d["durationMs"])+"ms", d.get("errorCode") or "")
    if steps: print("   ", steps)'
