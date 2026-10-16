package com.inspien.eai.model;

/**
 * 정규 모델(Canonical). HEADER 1 : ITEM N 을 펼친 한 행 = ORDER_TB 한 행 = 영수증 파일 한 줄.
 * 모든 수신처(DB·FTP·운송사)는 이 모델에서 변환된다.
 */
public record OrderRecord(String orderId, String applicantKey, String userId, String itemId,
                          String name, String address, String itemName, String price, String status) {

    public OrderRecord withOrderId(String id, String key) {
        return new OrderRecord(id, key, userId, itemId, name, address, itemName, price, status);
    }
}
