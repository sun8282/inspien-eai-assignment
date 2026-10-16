package com.inspien.eai.model;

/**
 * 적재에서 제외된 건. 응답·로그로 사유를 돌려준다(부분 수용 정책).
 * 개인정보(이름·주소)는 담지 않고 식별자만 남긴다.
 */
public record ExcludedRecord(Level level, String userId, String itemId, Reason reason, String detail) {

    public enum Level { HEADER, ITEM }

    public enum Reason {
        /** ITEM 의 USER_ID 에 해당하는 HEADER 가 없음 → 이름·주소를 채울 수 없음 */
        NO_HEADER,
        /** HEADER 에 딸린 ITEM 이 없음 → 만들 주문 행이 없음 */
        NO_ITEM,
        /** 같은 USER_ID 의 HEADER 가 여러 개 → 어느 주문자 정보인지 모호 */
        DUPLICATE_HEADER,
        /** 필수값 누락·형식 오류·길이 초과·구분자 문자 포함 */
        INVALID_FIELD
    }
}
