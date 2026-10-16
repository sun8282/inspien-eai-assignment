package com.inspien.eai.mapping;

import com.inspien.eai.model.ExcludedRecord;
import com.inspien.eai.model.ExcludedRecord.Level;
import com.inspien.eai.model.ExcludedRecord.Reason;
import com.inspien.eai.model.OrderHeader;
import com.inspien.eai.model.OrderItem;
import com.inspien.eai.model.OrderRecord;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Message Mapping: HEADER 1 : ITEM N → 플랫 주문 행(Canonical).
 *
 * <p>조인 키는 USER_ID. 문서 내 위치로 묶지 않는다 — 실제 수신 데이터는 HEADER/ITEM 이 순서 없이 섞여 온다.
 * 정책: <b>부분 수용</b>. 조인·검증을 통과한 행만 적재하고, 나머지는 사유와 함께 {@link ExcludedRecord} 로 돌려준다.
 */
public final class OrderFlattener {

    public record Result(List<OrderRecord> records, List<ExcludedRecord> excluded) { }

    /** 대상 컬럼이 VARCHAR2(100) — 바이트 기준으로 검사(한글 UTF-8 3바이트). */
    static final int MAX_BYTES = 100;
    private static final Pattern DIGITS = Pattern.compile("^\\d+$");
    /** 영수증 파일 구분자(^)·줄바꿈이 값에 섞이면 파일 레이아웃이 깨진다. */
    private static final Pattern FORBIDDEN = Pattern.compile("[\\^\\r\\n]");

    public Result flatten(List<OrderHeader> headers, List<OrderItem> items) {
        List<ExcludedRecord> excluded = new ArrayList<>();

        Map<String, OrderHeader> byUser = new LinkedHashMap<>();
        Set<String> duplicated = new HashSet<>();
        for (OrderHeader h : headers) {
            if (isBlank(h.userId())) {
                excluded.add(new ExcludedRecord(Level.HEADER, null, null, Reason.INVALID_FIELD, "USER_ID 누락"));
                continue;
            }
            if (byUser.putIfAbsent(h.userId(), h) != null) duplicated.add(h.userId());
        }

        List<OrderRecord> records = new ArrayList<>();
        Set<String> usersWithItem = new HashSet<>();
        for (OrderItem item : items) {
            String user = item.userId();
            if (isBlank(user)) {
                excluded.add(new ExcludedRecord(Level.ITEM, null, item.itemId(), Reason.INVALID_FIELD, "USER_ID 누락"));
                continue;
            }
            usersWithItem.add(user);
            if (duplicated.contains(user)) {
                excluded.add(new ExcludedRecord(Level.ITEM, user, item.itemId(), Reason.DUPLICATE_HEADER, "동일 USER_ID HEADER 복수"));
                continue;
            }
            OrderHeader h = byUser.get(user);
            if (h == null) {
                excluded.add(new ExcludedRecord(Level.ITEM, user, item.itemId(), Reason.NO_HEADER, "주문자(HEADER) 정보 없음"));
                continue;
            }
            String invalid = validate(h, item);
            if (invalid != null) {
                excluded.add(new ExcludedRecord(Level.ITEM, user, item.itemId(), Reason.INVALID_FIELD, invalid));
                continue;
            }
            records.add(new OrderRecord(null, null, user, item.itemId(), h.name(), h.address(),
                    item.itemName(), item.price(), h.status()));
        }

        for (String user : byUser.keySet()) {
            if (!usersWithItem.contains(user)) {
                excluded.add(new ExcludedRecord(Level.HEADER, user, null, Reason.NO_ITEM, "주문 상품(ITEM) 없음"));
            } else if (duplicated.contains(user)) {
                excluded.add(new ExcludedRecord(Level.HEADER, user, null, Reason.DUPLICATE_HEADER, "동일 USER_ID HEADER 복수"));
            }
        }
        return new Result(records, excluded);
    }

    /** @return 위반 내용, 정상이면 null */
    private static String validate(OrderHeader h, OrderItem i) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("USER_ID", i.userId());
        fields.put("ITEM_ID", i.itemId());
        fields.put("NAME", h.name());
        fields.put("ADDRESS", h.address());
        fields.put("ITEM_NAME", i.itemName());
        fields.put("PRICE", i.price());
        fields.put("STATUS", h.status());

        for (Map.Entry<String, String> f : fields.entrySet()) {
            String v = f.getValue();
            if (isBlank(v)) return f.getKey() + " 누락";
            if (v.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) return f.getKey() + " 길이 초과(" + MAX_BYTES + "바이트)";
            if (FORBIDDEN.matcher(v).find()) return f.getKey() + " 에 허용되지 않는 문자(^ 또는 줄바꿈)";
        }
        if (!DIGITS.matcher(i.price()).matches()) return "PRICE 숫자 아님";
        return null;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
