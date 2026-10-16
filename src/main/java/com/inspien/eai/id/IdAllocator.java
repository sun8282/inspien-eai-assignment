package com.inspien.eai.id;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * DB 기반 ID 채번기 (자동 채번이 없는 테이블용).
 *
 * <p>동시성: 프로세스 내에서는 메모리 상한(lastIssued)을 기준으로 발급해, 앞선 요청이 커밋 전이어도 겹치지 않는다.
 * 프로세스 밖(다른 인스턴스·수기 입력)과의 충돌은 PK 가 최종 방어선 — 중복키 발생 시 {@link #resync()} 후 재시도한다.
 * 롤백된 요청의 ID 는 재사용하지 않는다(시퀀스와 같은 정책, 결번 허용).
 */
public final class IdAllocator {

    private static final Pattern IDENT = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

    private final String table;
    private final String idColumn;
    private final String applicantKey;
    private String lastIssued;
    private boolean synced;

    public IdAllocator(String table, String idColumn, String applicantKey) {
        if (!IDENT.matcher(table).matches() || !IDENT.matcher(idColumn).matches()) {
            throw new IllegalArgumentException("테이블/컬럼명 형식 오류: " + table + "." + idColumn);
        }
        this.table = table;
        this.idColumn = idColumn;
        this.applicantKey = applicantKey;
    }

    public synchronized List<String> allocate(Connection c, int count) throws SQLException {
        if (!synced) {
            lastIssued = SequentialIdFormat.max(lastIssued, currentMax(c));
            synced = true;
        }
        List<String> ids = SequentialIdFormat.next(lastIssued, count);
        lastIssued = ids.get(ids.size() - 1);
        return ids;
    }

    /** 외부 충돌(중복키) 감지 시 다음 발급 전에 DB 최대값을 다시 읽는다. */
    public synchronized void resync() {
        synced = false;
    }

    public synchronized int remaining() {
        return SequentialIdFormat.remaining(lastIssued);
    }

    private String currentMax(Connection c) throws SQLException {
        String sql = "SELECT MAX(" + idColumn + ") FROM " + table
                + " WHERE APPLICANT_KEY = ? AND REGEXP_LIKE(" + idColumn + ", '^[A-Z][0-9]{3}$')";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setQueryTimeout(10);
            ps.setString(1, applicantKey);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }
}
