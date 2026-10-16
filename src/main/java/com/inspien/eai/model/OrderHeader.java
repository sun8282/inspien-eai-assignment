package com.inspien.eai.model;

/** 수신 XML 의 HEADER (주문자). 값은 trim 된 상태, 누락 엘리먼트는 null. */
public record OrderHeader(String userId, String name, String address, String status) { }
