package com.inspien.eai.mapping;

import com.inspien.eai.model.OrderRecord;
import com.inspien.eai.model.ReceiptFile;

import java.nio.charset.Charset;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Message Mapping: 주문 행 → 영수증 파일(IF_ORD_002).
 *
 * <pre>
 * 파일명: INSPIEN_참여자명_yyyyMMddHHmmss.txt (참여자명 한글)
 * 내용:   ORDER_ID^USER_ID^ITEM_ID^APPLICANT_KEY^NAME^ADDRESS^ITEM_NAME^PRICE\n  (필드·순서 고정, 개행 LF 고정)
 * </pre>
 * 파일명이 초 단위라 같은 초에 두 요청이 오면 이름이 겹친다. 형식을 바꿀 수 없으므로
 * 프로세스 내에서 직전 발급 시각 이후의 초를 배정해 유일성을 보장한다(버스트 시 수 초 앞당겨질 수 있음).
 */
public final class ReceiptFileMapper {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final char DELIMITER = '^';
    private static final char LINE_END = '\n'; // OS 기본 개행(CRLF) 금지

    private final String applicantName;
    private final Charset charset;
    private final Clock clock;
    private LocalDateTime lastIssued;

    public ReceiptFileMapper(String applicantName, Charset charset, Clock clock) {
        if (applicantName == null || applicantName.isBlank()) throw new IllegalArgumentException("참여자명(APPLICANT_NAME) 필요");
        this.applicantName = applicantName.strip();
        this.charset = charset;
        this.clock = clock;
    }

    public ReceiptFile map(List<OrderRecord> records) {
        StringBuilder sb = new StringBuilder(records.size() * 80);
        for (OrderRecord r : records) {
            sb.append(r.orderId()).append(DELIMITER)
                    .append(r.userId()).append(DELIMITER)
                    .append(r.itemId()).append(DELIMITER)
                    .append(r.applicantKey()).append(DELIMITER)
                    .append(r.name()).append(DELIMITER)
                    .append(r.address()).append(DELIMITER)
                    .append(r.itemName()).append(DELIMITER)
                    .append(r.price()).append(LINE_END);
        }
        return new ReceiptFile(nextName(), sb.toString().getBytes(charset), records.size());
    }

    /** 같은 내용, 다음 초의 이름 — 서버에 동일 파일명이 이미 있을 때. */
    public ReceiptFile rename(ReceiptFile file) {
        return new ReceiptFile(nextName(), file.content(), file.lineCount());
    }

    synchronized String nextName() {
        LocalDateTime now = LocalDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS);
        if (lastIssued != null && !now.isAfter(lastIssued)) now = lastIssued.plusSeconds(1);
        lastIssued = now;
        return "INSPIEN_" + applicantName + "_" + TS.format(now) + ".txt";
    }
}
