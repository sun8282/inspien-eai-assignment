package com.inspien.eai.id;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 과제 ID 규격: 알파벳 대문자 1 + 숫자 3 (예: A113). 순수 계산 — DB 와 무관.
 *
 * <p>A001 → A999 → B000 → ... → Z999 순으로 증가. 사전순 = 발급순이라 MAX() 로 최종 발급값을 찾을 수 있다.
 * 지원자 키별 최대 25,999 개(A000 은 사용 안 함). 소진 시 예외 — 조용히 겹치는 것보다 멈추는 게 안전.
 */
public final class SequentialIdFormat {

    public static final Pattern PATTERN = Pattern.compile("^[A-Z][0-9]{3}$");
    private static final int MAX_VALUE = 25 * 1000 + 999; // Z999

    private SequentialIdFormat() { }

    /** lastIssued 다음부터 count 개. lastIssued 가 null 이면 A001 부터. */
    public static List<String> next(String lastIssued, int count) {
        int base = lastIssued == null ? 0 : toValue(lastIssued);
        if (base + count > MAX_VALUE) {
            throw new IdExhaustedException("ID 한도 초과: 마지막=" + lastIssued + ", 요청=" + count + "개, 남은=" + (MAX_VALUE - base) + "개");
        }
        List<String> ids = new ArrayList<>(count);
        for (int v = base + 1; v <= base + count; v++) ids.add(toId(v));
        return ids;
    }

    public static int remaining(String lastIssued) {
        return MAX_VALUE - (lastIssued == null ? 0 : toValue(lastIssued));
    }

    static int toValue(String id) {
        if (!PATTERN.matcher(id).matches()) throw new IllegalArgumentException("ID 형식 아님: " + id);
        return (id.charAt(0) - 'A') * 1000 + Integer.parseInt(id.substring(1));
    }

    static String toId(int value) {
        return (char) ('A' + value / 1000) + String.format("%03d", value % 1000);
    }

    public static String max(String a, String b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.compareTo(b) >= 0 ? a : b;
    }

    public static final class IdExhaustedException extends RuntimeException {
        public IdExhaustedException(String message) { super(message); }
    }
}
