package com.inspien.eai.mapping;

import com.inspien.eai.model.PendingOrder;
import com.inspien.eai.model.ShipmentRecord;

/** Message Mapping: 주문(ORDER_TB) → 운송(SHIPMENT_TB). 원천 DB 는 수기 입력 등으로 비정상 행이 있을 수 있어 다시 검증한다. */
public final class ShipmentMapper {

    public ShipmentRecord map(PendingOrder o, String shipmentId, String applicantKey) {
        return new ShipmentRecord(shipmentId, applicantKey, o.orderId(), o.itemId(), o.address());
    }

    /** @return 위반 내용, 정상이면 null */
    public String validate(PendingOrder o) {
        if (blank(o.itemId())) return "ITEM_ID 누락";
        if (blank(o.address())) return "ADDRESS 누락 — 배송 불가";
        return null;
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }
}
