package com.inspien.eai.core;

/**
 * 표준 에러코드. 응답 JSON·모니터링 로그·운영 가이드에 같은 코드를 쓴다.
 * retryable = 같은 요청을 다시 보내면 성공할 가능성이 있는가(일시적 장애).
 */
public enum ErrorCode {
    METHOD_NOT_ALLOWED(405, false, "허용되지 않은 메서드"),
    PAYLOAD_TOO_LARGE(413, false, "요청 본문이 너무 큼"),
    UNSUPPORTED_ENCODING(400, false, "요청 본문 인코딩을 해석할 수 없음"),
    XML_PARSE(400, false, "XML 파싱 실패"),
    NO_VALID_RECORD(422, false, "적재 가능한 주문이 없음"),
    ID_EXHAUSTED(500, false, "ID 채번 한도 소진"),
    DB_CONNECTION(503, true, "DB 연결 실패"),
    DB_TIMEOUT(503, true, "DB 응답 시간 초과"),
    DB_DUPLICATE_KEY(500, true, "DB 키 중복"),
    DB_ERROR(500, false, "DB 처리 오류"),
    INTERNAL(500, false, "내부 오류");

    private final int httpStatus;
    private final boolean retryable;
    private final String description;

    ErrorCode(int httpStatus, boolean retryable, String description) {
        this.httpStatus = httpStatus;
        this.retryable = retryable;
        this.description = description;
    }

    public int httpStatus() { return httpStatus; }
    public boolean retryable() { return retryable; }
    public String description() { return description; }
}
