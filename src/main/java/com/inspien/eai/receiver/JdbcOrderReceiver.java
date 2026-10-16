package com.inspien.eai.receiver;

import com.inspien.eai.model.OrderRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Receiver Channel (JDBC → ORDER_TB).
 * 트랜잭션 경계는 Flow 가 소유한다 — Receiver 는 주어진 커넥션에 쓰기만 하고 커밋하지 않는다.
 * (FTP 전송까지 성공해야 커밋하기 위함)
 * CREATE_TIME 은 DB 기본값(SYSDATE)에 맡긴다.
 */
public final class JdbcOrderReceiver {

    private static final Pattern IDENT = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
    private final String sql;

    public JdbcOrderReceiver(String table) {
        if (!IDENT.matcher(table).matches()) throw new IllegalArgumentException("테이블명 형식 오류: " + table);
        this.sql = "INSERT INTO " + table
                + " (ORDER_ID, APPLICANT_KEY, USER_ID, ITEM_ID, NAME, ADDRESS, ITEM_NAME, PRICE, STATUS)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    public int insert(Connection c, List<OrderRecord> records) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setQueryTimeout(30);
            for (OrderRecord r : records) {
                ps.setString(1, r.orderId());
                ps.setString(2, r.applicantKey());
                ps.setString(3, r.userId());
                ps.setString(4, r.itemId());
                ps.setString(5, r.name());
                ps.setString(6, r.address());
                ps.setString(7, r.itemName());
                ps.setString(8, r.price());
                ps.setString(9, r.status());
                ps.addBatch();
            }
            ps.executeBatch();
            return records.size();
        }
    }
}
