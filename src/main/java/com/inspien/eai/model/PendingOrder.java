package com.inspien.eai.model;

/** 운송 전송 대상 주문(ORDER_TB, STATUS='N') — 배치가 읽는 최소 컬럼. */
public record PendingOrder(String orderId, String itemId, String address) { }
