package com.inspien.eai.model;

/** 수신 XML 의 ITEM (주문 상품). HEADER 와 USER_ID 로 1:N 조인된다. */
public record OrderItem(String userId, String itemId, String itemName, String price) { }
