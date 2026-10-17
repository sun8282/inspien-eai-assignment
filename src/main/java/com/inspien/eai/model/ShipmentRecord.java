package com.inspien.eai.model;

/** 운송사 스키마(SHIPMENT_TB) 한 행. */
public record ShipmentRecord(String shipmentId, String applicantKey, String orderId, String itemId, String address) { }
