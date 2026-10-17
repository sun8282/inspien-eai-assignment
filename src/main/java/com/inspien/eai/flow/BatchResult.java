package com.inspien.eai.flow;

import java.util.List;

/** 배치 1회 실행 결과 — 모니터링 로그·수동 실행 API 응답의 공통 원천. */
public record BatchResult(String runId, String interfaceId, String trigger, ResultCode resultCode, String message,
                          int fetched, int shipped, int repaired, int skipped, int failed, int quarantinedSkipped,
                          int chunks, String shipmentIdRange, String stopReason, List<Failure> failures,
                          String errorCode, long durationMs) {

    /** NO_DATA = 대상 없음(정상). 주기마다 남는 이 로그가 곧 "배치가 살아 있다"는 하트비트다. */
    public enum ResultCode { SUCCESS, PARTIAL_SUCCESS, NO_DATA, FAIL }

    /** 실패 건 — 개인정보 없이 주문 ID·코드·사유만. */
    public record Failure(String orderId, String errorCode, String detail, int failCount, boolean quarantined) { }
}
