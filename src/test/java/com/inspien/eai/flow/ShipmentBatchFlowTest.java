package com.inspien.eai.flow;

import com.inspien.eai.config.ProvisionInfo.DbConnInfo;
import com.inspien.eai.flow.BatchResult.ResultCode;
import com.inspien.eai.id.IdAllocator;
import com.inspien.eai.infra.ConnectionFactory;
import com.inspien.eai.monitor.InterfaceLogWriter;
import com.inspien.eai.receiver.JdbcShipmentReceiver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ShipmentBatchFlowTest {

    private static final String KEY = "TEST_KEY";

    @TempDir
    Path tmp;

    private String url;
    private boolean failConnect;

    @BeforeEach
    void setUp() throws SQLException {
        url = "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=Oracle;DB_CLOSE_DELAY=-1";
        try (Connection c = DriverManager.getConnection(url); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE ORDER_TB (ORDER_ID VARCHAR2(100) NOT NULL, APPLICANT_KEY VARCHAR2(100) NOT NULL,"
                    + " USER_ID VARCHAR2(100), ITEM_ID VARCHAR2(100), NAME VARCHAR2(100), ADDRESS VARCHAR2(100),"
                    + " ITEM_NAME VARCHAR2(100), PRICE VARCHAR2(100), STATUS VARCHAR2(100),"
                    + " CREATE_TIME TIMESTAMP DEFAULT SYSDATE, PRIMARY KEY (APPLICANT_KEY, ORDER_ID))");
            // 운송 테이블 ITEM_ID 를 좁게 — DB 단 오류(길이 초과)로 청크 실패 → 건별 재처리 경로 검증
            st.execute("CREATE TABLE SHIPMENT_TB (SHIPMENT_ID VARCHAR2(100) NOT NULL, APPLICANT_KEY VARCHAR2(100) NOT NULL,"
                    + " ORDER_ID VARCHAR2(100), ITEM_ID VARCHAR2(10), ADDRESS VARCHAR2(100),"
                    + " CREATE_DATE TIMESTAMP DEFAULT SYSDATE, PRIMARY KEY (APPLICANT_KEY, SHIPMENT_ID))");
        }
    }

    private ShipmentBatchFlow flow(int chunk, int maxRows) {
        ConnectionFactory db = new ConnectionFactory(new DbConnInfo(url, "", "", "SHIPMENT_TB")) {
            @Override
            public Connection open() throws SQLException {
                if (failConnect) throw new SQLException("주입된 연결 실패", "08001", 17002);
                Connection c = DriverManager.getConnection(url);
                c.setAutoCommit(false);
                return c;
            }
        };
        return new ShipmentBatchFlow(KEY, db, new IdAllocator("SHIPMENT_TB", "SHIPMENT_ID", KEY),
                new JdbcShipmentReceiver("ORDER_TB", "SHIPMENT_TB"), new InterfaceLogWriter(tmp),
                new ShipmentBatchFlow.Settings(chunk, maxRows, Duration.ofMinutes(1)));
    }

    private void order(String key, String id, String itemId, String address, String status) throws SQLException {
        try (Connection c = DriverManager.getConnection(url);
             PreparedStatement ps = c.prepareStatement("INSERT INTO ORDER_TB (ORDER_ID, APPLICANT_KEY, USER_ID, ITEM_ID, NAME, ADDRESS, ITEM_NAME, PRICE, STATUS)"
                     + " VALUES (?, ?, 'U', ?, '홍길동', ?, '청바지', '1000', ?)")) {
            ps.setString(1, id);
            ps.setString(2, key);
            ps.setString(3, itemId);
            ps.setString(4, address);
            ps.setString(5, status);
            ps.executeUpdate();
        }
    }

    private void orders(int n) throws SQLException {
        for (int i = 1; i <= n; i++) order(KEY, String.format("A%03d", i), "ITEM" + i, "서울특별시 " + i + "구", "N");
    }

    private List<String> query(String sql) throws SQLException {
        try (Connection c = DriverManager.getConnection(url); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) out.add(rs.getString(1));
            return out;
        }
    }

    private int count(String sql) throws SQLException {
        return Integer.parseInt(query(sql).get(0));
    }

    @Test
    void 청크_단위로_전부_적재하고_Y로_갱신_다음_실행은_NO_DATA() throws Exception {
        orders(5);
        ShipmentBatchFlow f = flow(2, 1000);
        BatchResult r = f.run("TEST");

        assertEquals(ResultCode.SUCCESS, r.resultCode());
        assertEquals(5, r.shipped());
        assertEquals(3, r.chunks());
        assertEquals("A001~A005", r.shipmentIdRange());
        assertEquals(0, count("SELECT COUNT(*) FROM ORDER_TB WHERE STATUS = 'N'"));
        assertEquals(List.of("A001|A001|ITEM1|서울특별시 1구"),
                query("SELECT SHIPMENT_ID || '|' || ORDER_ID || '|' || ITEM_ID || '|' || ADDRESS FROM SHIPMENT_TB WHERE ORDER_ID = 'A001'"));
        assertEquals(ResultCode.NO_DATA, f.run("TEST").resultCode());
    }

    @Test
    void 이미_적재된_주문은_중복적재_없이_상태만_보정() throws Exception {
        orders(3);
        try (Connection c = DriverManager.getConnection(url); Statement st = c.createStatement()) {
            st.execute("INSERT INTO SHIPMENT_TB (SHIPMENT_ID, APPLICANT_KEY, ORDER_ID, ITEM_ID, ADDRESS) VALUES ('A001', '" + KEY + "', 'A002', 'ITEM2', 'x')");
        }
        BatchResult r = flow(100, 1000).run("TEST");
        assertEquals(2, r.shipped());
        assertEquals(1, r.repaired());
        assertEquals(1, count("SELECT COUNT(*) FROM SHIPMENT_TB WHERE ORDER_ID = 'A002'"));
        assertEquals(List.of("A002", "A003"), query("SELECT SHIPMENT_ID FROM SHIPMENT_TB WHERE SHIPMENT_ID <> 'A001' ORDER BY 1"));
    }

    @Test
    void 검증_실패건만_남기고_나머지는_처리() throws Exception {
        orders(3);
        order(KEY, "A004", "ITEM4", null, "N");
        BatchResult r = flow(100, 1000).run("TEST");
        assertEquals(ResultCode.PARTIAL_SUCCESS, r.resultCode());
        assertEquals(3, r.shipped());
        assertEquals(1, r.failed());
        assertEquals("INVALID_FIELD", r.failures().get(0).errorCode());
        assertEquals(List.of("A004"), query("SELECT ORDER_ID FROM ORDER_TB WHERE STATUS = 'N'"), "실패 건은 N 유지 → 다음 주기 재시도");
    }

    @Test
    void DB오류로_청크가_실패하면_건별_재처리로_원인건만_격리() throws Exception {
        orders(4);
        order(KEY, "A005", "ITEM_ID_TOO_LONG_FOR_SHIPMENT", "서울", "N"); // SHIPMENT_TB.ITEM_ID(10) 초과
        BatchResult r = flow(100, 1000).run("TEST");
        assertEquals(4, r.shipped());
        assertEquals(1, r.failed());
        assertEquals("A005", r.failures().get(0).orderId());
        assertEquals(List.of("A005"), query("SELECT ORDER_ID FROM ORDER_TB WHERE STATUS = 'N'"));
        assertEquals(4, count("SELECT COUNT(*) FROM SHIPMENT_TB"));
    }

    @Test
    void 같은_건이_연속_실패하면_격리해_반복실패를_멈춘다() throws Exception {
        order(KEY, "A001", "ITEM1", null, "N");
        ShipmentBatchFlow f = flow(100, 1000);
        for (int i = 1; i <= ShipmentBatchFlow.QUARANTINE_AFTER; i++) {
            BatchResult r = f.run("TEST");
            assertEquals(i, r.failures().get(0).failCount());
            assertEquals(i == ShipmentBatchFlow.QUARANTINE_AFTER, r.failures().get(0).quarantined());
        }
        BatchResult after = f.run("TEST");
        assertEquals(0, after.failed());
        assertEquals(1, after.quarantinedSkipped());
    }

    @Test
    void 실행당_상한을_넘으면_멈추고_다음_실행이_이어서_처리() throws Exception {
        orders(5);
        ShipmentBatchFlow f = flow(2, 3);
        BatchResult first = f.run("TEST");
        assertEquals(3, first.shipped());
        assertEquals("ROW_LIMIT", first.stopReason());
        BatchResult second = f.run("TEST");
        assertEquals(2, second.shipped());
        assertEquals("A004~A005", second.shipmentIdRange());
        assertEquals(0, count("SELECT COUNT(*) FROM ORDER_TB WHERE STATUS = 'N'"));
    }

    @Test
    void 중지신호면_청크경계에서_멈춘다() throws Exception {
        orders(3);
        ShipmentBatchFlow f = flow(1, 1000);
        f.requestStop();
        BatchResult r = f.run("TEST");
        assertEquals("STOP_REQUESTED", r.stopReason());
        assertEquals(3, count("SELECT COUNT(*) FROM ORDER_TB WHERE STATUS = 'N'"));
    }

    @Test
    void 다른_지원자_데이터와_처리완료_건은_건드리지_않는다() throws Exception {
        order("OTHER", "A001", "ITEM1", "서울", "N");
        order(KEY, "A001", "ITEM1", "서울", "Y");
        assertEquals(ResultCode.NO_DATA, flow(100, 1000).run("TEST").resultCode());
        assertEquals(0, count("SELECT COUNT(*) FROM SHIPMENT_TB"));
    }

    @Test
    void 연결실패는_FAIL로_기록하고_다음_주기에_재시도() {
        failConnect = true;
        BatchResult r = flow(100, 1000).run("TEST");
        assertEquals(ResultCode.FAIL, r.resultCode());
        assertEquals("DB_CONNECTION", r.errorCode());
    }
}
