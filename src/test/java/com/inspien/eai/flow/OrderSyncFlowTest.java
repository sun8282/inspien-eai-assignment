package com.inspien.eai.flow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inspien.eai.config.ProvisionInfo.DbConnInfo;
import com.inspien.eai.core.ErrorCode;
import com.inspien.eai.flow.OrderResult.ResultCode;
import com.inspien.eai.id.IdAllocator;
import com.inspien.eai.infra.ConnectionFactory;
import com.inspien.eai.mapping.ReceiptFileMapper;
import com.inspien.eai.model.ReceiptFile;
import com.inspien.eai.monitor.InterfaceLogWriter;
import com.inspien.eai.monitor.OutboundArchive;
import com.inspien.eai.receiver.JdbcOrderReceiver;
import com.inspien.eai.receiver.ReceiptReceiver;
import com.inspien.eai.receiver.ReceiptSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DB+FTP 동기화 시나리오 검증 — 쓰기 전용 FTP·DELETE 불가 DB 전제.
 * 실패 지점마다 최종 상태(DB 행 수 / FTP 파일)가 불변식 "파일이 있으면 DB 에도 있다"를 지키는지 확인한다.
 */
class OrderSyncFlowTest {

    private static final String KEY = "TEST_KEY";
    private static final String XML = """
            <HEADER><USER_ID>U1</USER_ID><NAME>홍길동</NAME><ADDRESS>서울특별시 금천구</ADDRESS><STATUS>N</STATUS></HEADER>
            <ITEM><USER_ID>U1</USER_ID><ITEM_ID>I1</ITEM_ID><ITEM_NAME>청바지</ITEM_NAME><PRICE>21000</PRICE></ITEM>
            <ITEM><USER_ID>U1</USER_ID><ITEM_ID>I2</ITEM_ID><ITEM_NAME>티셔츠</ITEM_NAME><PRICE>15800</PRICE></ITEM>
            <ITEM><USER_ID>U9</USER_ID><ITEM_ID>I9</ITEM_ID><ITEM_NAME>양말</ITEM_NAME><PRICE>4500</PRICE></ITEM>
            """;

    @TempDir
    Path logDir;

    private String url;
    private FakeFtp ftp;
    private boolean failCommit;
    /** 실제로는 커밋된 뒤 예외 — "커밋 결과 불명(응답 유실)" 재현 */
    private boolean commitThenThrow;
    private int opens;
    private int failOpenFrom = Integer.MAX_VALUE;

    @BeforeEach
    void setUp() throws SQLException {
        url = "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=Oracle;DB_CLOSE_DELAY=-1";
        try (Connection c = DriverManager.getConnection(url); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE ORDER_TB (ORDER_ID VARCHAR2(100) NOT NULL, APPLICANT_KEY VARCHAR2(100) NOT NULL,"
                    + " USER_ID VARCHAR2(100), ITEM_ID VARCHAR2(100), NAME VARCHAR2(100), ADDRESS VARCHAR2(100),"
                    + " ITEM_NAME VARCHAR2(100), PRICE VARCHAR2(100), STATUS VARCHAR2(100),"
                    + " CREATE_TIME TIMESTAMP DEFAULT SYSDATE, PRIMARY KEY (APPLICANT_KEY, ORDER_ID))");
        }
        ftp = new FakeFtp();
    }

    private OrderSyncFlow flow() {
        ConnectionFactory db = new ConnectionFactory(new DbConnInfo(url, "", "", "ORDER_TB")) {
            @Override
            public Connection open() throws SQLException {
                if (++opens >= failOpenFrom) throw new SQLException("주입된 연결 실패", "08001", 17002);
                Connection real = DriverManager.getConnection(url);
                real.setAutoCommit(false);
                return faulty(real);
            }
        };
        Clock clock = Clock.fixed(Instant.parse("2026-10-17T01:02:03Z"), ZoneId.of("Asia/Seoul"));
        return new OrderSyncFlow(KEY, db, new IdAllocator("ORDER_TB", "ORDER_ID", KEY), new JdbcOrderReceiver("ORDER_TB"),
                new ReceiptFileMapper("홍길동", StandardCharsets.UTF_8, clock), ftp,
                new OutboundArchive(logDir.resolve("archive")), new InterfaceLogWriter(logDir.resolve("logs")));
    }

    /** commit 에 장애를 주입하는 커넥션 프록시 */
    private Connection faulty(Connection real) {
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class}, (p, m, args) -> {
            if (m.getName().equals("commit")) {
                if (failCommit) {
                    failCommit = false; // 첫 커밋만 실패 (roll-forward 커밋은 정상)
                    throw new SQLException("주입된 커밋 실패", "08006", 17002);
                }
                if (commitThenThrow) {
                    commitThenThrow = false;
                    real.commit();
                    throw new SQLException("주입: 커밋 후 응답 유실", "08006", 17002);
                }
            }
            try {
                return m.invoke(real, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }

    private OrderResult send() {
        return flow().handle(XML.getBytes(StandardCharsets.UTF_8), "application/xml; charset=UTF-8");
    }

    private int rows() throws SQLException {
        try (Connection c = DriverManager.getConnection(url); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM ORDER_TB")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private JsonNode lastLog() throws IOException {
        Path file;
        try (var files = Files.list(logDir.resolve("logs"))) {
            file = files.findFirst().orElseThrow();
        }
        List<String> lines = Files.readAllLines(file);
        return new ObjectMapper().readTree(lines.get(lines.size() - 1));
    }

    @Test
    void 정상_파일업로드_후_커밋_영수증_형식() throws Exception {
        OrderResult r = send();

        assertEquals(ResultCode.PARTIAL_SUCCESS, r.resultCode()); // U9 는 HEADER 없음 → 제외
        assertEquals(List.of("A001", "A002"), r.orderIds());
        assertEquals(2, rows());
        assertEquals("INSPIEN_홍길동_20261017100203.txt", r.receiptFile());
        assertEquals("A001^U1^I1^TEST_KEY^홍길동^서울특별시 금천구^청바지^21000\n"
                        + "A002^U1^I2^TEST_KEY^홍길동^서울특별시 금천구^티셔츠^15800\n",
                new String(ftp.files.get(r.receiptFile()), StandardCharsets.UTF_8));
        JsonNode steps = lastLog().get("steps");
        assertEquals("COMMIT", steps.get(steps.size() - 1).get("step").asText());
        Path archived;
        try (var dates = Files.list(logDir.resolve("archive").resolve("receipt"))) {
            archived = dates.findFirst().orElseThrow().resolve(r.receiptFile());
        }
        assertArrayEquals(ftp.files.get(r.receiptFile()), Files.readAllBytes(archived), "보낸 바이트 그대로 보관");
    }

    @Test
    void 같은_초에_두번_요청해도_파일명이_겹치지_않는다() {
        OrderSyncFlow f = flow();
        OrderResult a = f.handle(XML.getBytes(StandardCharsets.UTF_8), null);
        OrderResult b = f.handle(XML.getBytes(StandardCharsets.UTF_8), null);
        assertNotEquals(a.receiptFile(), b.receiptFile());
        assertEquals(List.of("A003", "A004"), b.orderIds());
    }

    @Test
    void 서버에_같은_이름이_있으면_덮어쓰지_않고_다음_이름() throws Exception {
        ftp.files.put("INSPIEN_홍길동_20261017100203.txt", new byte[0]); // 재기동 직후 같은 초 등
        OrderResult r = send();
        assertEquals("INSPIEN_홍길동_20261017100204.txt", r.receiptFile());
        assertEquals(0, ftp.files.get("INSPIEN_홍길동_20261017100203.txt").length, "기존 파일 보존");
    }

    @Test
    void FTP_연결실패_DB롤백() throws Exception {
        ftp.failConnect = true;
        OrderResult r = send();
        assertEquals("FTP_CONNECTION", r.errorCode());
        assertTrue(ErrorCode.FTP_CONNECTION.retryable());
        assertEquals(0, rows());
        assertTrue(ftp.files.isEmpty());
    }

    @Test
    void 업로드_거절_DB롤백_재전송_안전() throws Exception {
        ftp.failUpload = true;
        OrderResult r = send();
        assertEquals("FTP_TRANSFER", r.errorCode());
        assertEquals(0, rows());
        assertNull(lastLog().get("manualAction"), "파일이 안 남았으면 수동조치 불필요");

        ftp.failUpload = false;
        assertEquals(ResultCode.PARTIAL_SUCCESS, send().resultCode());
        assertEquals(2, rows());
    }

    @Test
    void 업로드_중_끊겨_부분파일이_남으면_DB롤백_수동조치_기록() throws Exception {
        ftp.partialUpload = true;
        OrderResult r = send();
        assertEquals("FTP_TRANSFER", r.errorCode());
        assertEquals(0, rows());
        assertTrue(lastLog().get("manualAction").asText().contains(r.receiptFile() == null ? "INSPIEN_" : r.receiptFile()));
    }

    @Test
    void 커밋실패시_파일에_맞춰_DB를_재적재한다_roll_forward() throws Exception {
        failCommit = true;
        OrderResult r = send();
        assertNull(r.errorCode());
        assertEquals(2, rows(), "파일이 공개됐으므로 DB 도 반드시 존재");
        assertEquals(1, ftp.files.size());
        assertTrue(lastLog().get("steps").toString().contains("동일 ID 재적재"));
    }

    @Test
    void 커밋은_됐는데_응답이_유실되면_중복적재하지_않는다() throws Exception {
        commitThenThrow = true;
        OrderResult r = send();
        assertNull(r.errorCode());
        assertEquals(2, rows(), "중복 적재 없음");
        assertTrue(lastLog().get("steps").toString().contains("커밋 성공 확인"));
    }

    @Test
    void roll_forward까지_실패하면_수동조치_정보를_남기고_재전송_금지() throws Exception {
        failCommit = true;
        failOpenFrom = 2; // 첫 연결만 정상, roll-forward 용 재연결 실패
        OrderResult r = send();
        assertEquals("RECOVERY_FAILED", r.errorCode());
        assertFalse(ErrorCode.RECOVERY_FAILED.retryable());
        assertEquals(0, rows());
        assertEquals(1, ftp.files.size(), "파일은 지울 수 없음");
        String manual = lastLog().get("manualAction").asText();
        assertTrue(manual.contains("A001~A002") && manual.contains("INSPIEN_홍길동_"));
    }

    /** 메모리 FTP — 쓰기 전용(삭제·이름변경 없음), 단계별 장애 주입 */
    static final class FakeFtp implements ReceiptReceiver {
        final Map<String, byte[]> files = new HashMap<>();
        boolean failConnect;
        boolean failUpload;
        boolean partialUpload;

        @Override
        public String target() {
            return "FTP:fake/";
        }

        @Override
        public ReceiptSession open() throws IOException {
            if (failConnect) throw new IOException("주입된 접속 실패");
            return new ReceiptSession() {
                @Override
                public boolean exists(String name) {
                    return files.containsKey(name);
                }

                @Override
                public void upload(ReceiptFile file) throws UploadException {
                    if (failUpload) throw new UploadException("주입된 업로드 거절", false, null);
                    if (partialUpload) {
                        files.put(file.name(), java.util.Arrays.copyOf(file.content(), file.content().length / 2));
                        throw new UploadException("주입된 전송 중 끊김", true, null);
                    }
                    files.put(file.name(), file.content());
                }

                @Override
                public void close() { }
            };
        }
    }
}
