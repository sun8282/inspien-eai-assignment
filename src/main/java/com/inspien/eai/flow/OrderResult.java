package com.inspien.eai.flow;

import com.inspien.eai.model.ExcludedRecord;

import java.util.List;

/** 주문 연계 처리 결과 — REST 응답과 모니터링 로그의 공통 원천. */
public record OrderResult(String msgId, String interfaceId, ResultCode resultCode, String message,
                          int receivedHeaderCount, int receivedItemCount,
                          List<String> orderIds, List<ExcludedRecord> excluded,
                          String errorCode, String receiptFile) {

    public enum ResultCode { SUCCESS, PARTIAL_SUCCESS, FAIL }

    public int processedCount() { return orderIds.size(); }
    public int excludedCount() { return excluded.size(); }
}
