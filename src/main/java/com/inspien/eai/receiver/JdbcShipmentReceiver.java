package com.inspien.eai.receiver;

import com.inspien.eai.model.PendingOrder;
import com.inspien.eai.model.ShipmentRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 배치용 JDBC 채널 — ORDER_TB 조회(Sender 측 Poll) / SHIPMENT_TB 적재 / ORDER_TB 상태 갱신(Receiver 측).
 * 트랜잭션 경계는 Flow 가 소유한다. 두 테이블은 같은 계정(점검 결과)이라 한 커넥션·한 트랜잭션으로 처리한다.
 */
public final class JdbcShipmentReceiver {

    private static final Pattern IDENT = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
    private static final int QUERY_TIMEOUT_SEC = 30;

    private final String orderTable;
    private final String shipmentTable;

    public JdbcShipmentReceiver(String orderTable, String shipmentTable) {
        if (!IDENT.matcher(orderTable).matches() || !IDENT.matcher(shipmentTable).matches()) {
            throw new IllegalArgumentException("테이블명 형식 오류: " + orderTable + ", " + shipmentTable);
        }
        this.orderTable = orderTable;
        this.shipmentTable = shipmentTable;
    }

    /**
     * 미전송 주문을 ORDER_ID 순으로 afterId 다음부터 limit 건(키셋 페이징).
     * OFFSET 대신 키셋을 쓰는 이유: 처리 중 STATUS 가 바뀌어도 건너뜀·중복이 없고, 실패 건을 지나쳐 다음 청크로 진행할 수 있다.
     */
    public List<PendingOrder> fetchPending(Connection c, String applicantKey, String afterId, int limit) throws SQLException {
        // 주의: Oracle 은 '' 를 NULL 로 취급 → "ORDER_ID > ''" 는 항상 거짓. 첫 페이지는 조건 자체를 뺀다.
        String sql = "SELECT ORDER_ID, ITEM_ID, ADDRESS FROM " + orderTable
                + " WHERE APPLICANT_KEY = ? AND STATUS = 'N'" + (afterId == null ? "" : " AND ORDER_ID > ?")
                + " ORDER BY ORDER_ID FETCH FIRST ? ROWS ONLY";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setQueryTimeout(QUERY_TIMEOUT_SEC);
            ps.setFetchSize(limit);
            int i = 1;
            ps.setString(i++, applicantKey);
            if (afterId != null) ps.setString(i++, afterId);
            ps.setInt(i, limit);
            List<PendingOrder> out = new ArrayList<>(limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new PendingOrder(rs.getString(1), rs.getString(2), rs.getString(3)));
            }
            return out;
        }
    }

    /** 이미 운송 테이블에 있는 주문 — 멱등성(재실행·수기 입력·부분 장애 후에도 중복 적재 금지). */
    public Set<String> existingShipments(Connection c, String applicantKey, Collection<String> orderIds) throws SQLException {
        Set<String> found = new HashSet<>();
        if (orderIds.isEmpty()) return found;
        String in = String.join(",", java.util.Collections.nCopies(orderIds.size(), "?"));
        String sql = "SELECT ORDER_ID FROM " + shipmentTable + " WHERE APPLICANT_KEY = ? AND ORDER_ID IN (" + in + ")";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setQueryTimeout(QUERY_TIMEOUT_SEC);
            int i = 1;
            ps.setString(i++, applicantKey);
            for (String id : orderIds) ps.setString(i++, id);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) found.add(rs.getString(1));
            }
        }
        return found;
    }

    /**
     * 처리권 선점: STATUS 'N'→'Y'. {@code AND STATUS = 'N'} 조건 덕분에 다른 실행이 먼저 처리한 건은 0 이 반환된다(낙관적 선점).
     * 같은 트랜잭션에서 운송 적재와 함께 커밋되므로 "적재 성공한 건만 Y" 가 보장된다.
     * @return 주문별 갱신 건수(0 또는 1)
     */
    public int[] claim(Connection c, String applicantKey, List<String> orderIds) throws SQLException {
        String sql = "UPDATE " + orderTable + " SET STATUS = 'Y' WHERE APPLICANT_KEY = ? AND ORDER_ID = ? AND STATUS = 'N'";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setQueryTimeout(QUERY_TIMEOUT_SEC);
            for (String id : orderIds) {
                ps.setString(1, applicantKey);
                ps.setString(2, id);
                ps.addBatch();
            }
            return ps.executeBatch();
        }
    }

    /** CREATE_DATE 는 DB 기본값(SYSDATE). */
    public void insert(Connection c, List<ShipmentRecord> rows) throws SQLException {
        if (rows.isEmpty()) return;
        String sql = "INSERT INTO " + shipmentTable + " (SHIPMENT_ID, APPLICANT_KEY, ORDER_ID, ITEM_ID, ADDRESS) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setQueryTimeout(QUERY_TIMEOUT_SEC);
            for (ShipmentRecord r : rows) {
                ps.setString(1, r.shipmentId());
                ps.setString(2, r.applicantKey());
                ps.setString(3, r.orderId());
                ps.setString(4, r.itemId());
                ps.setString(5, r.address());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

}
