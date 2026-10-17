package com.inspien.eai.support;

import com.inspien.eai.config.ProvisionInfo.DbConnInfo;
import com.inspien.eai.flow.BatchResult;
import com.inspien.eai.flow.OrderSyncFlow;
import com.inspien.eai.id.IdAllocator;
import com.inspien.eai.infra.ConnectionFactory;
import com.inspien.eai.mapping.ReceiptFileMapper;
import com.inspien.eai.model.ReceiptFile;
import com.inspien.eai.monitor.InterfaceLogWriter;
import com.inspien.eai.monitor.OutboundArchive;
import com.inspien.eai.receiver.JdbcOrderReceiver;
import com.inspien.eai.receiver.ReceiptReceiver;
import com.inspien.eai.receiver.ReceiptSession;
import com.inspien.eai.sender.BatchScheduler;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/** 테스트·부하 측정 공용 인프라 — 운영 DB·FTP 없이 실제 Flow 코드를 그대로 돌린다. */
public final class TestInfra {

    public static final String KEY = "TEST_KEY";

    private TestInfra() { }

    /** H2(Oracle 모드)에 운영과 같은 스키마 생성 */
    public static String newDatabase() throws SQLException {
        String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=Oracle;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=30000";
        try (Connection c = DriverManager.getConnection(url); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE ORDER_TB (ORDER_ID VARCHAR2(100) NOT NULL, APPLICANT_KEY VARCHAR2(100) NOT NULL,"
                    + " USER_ID VARCHAR2(100), ITEM_ID VARCHAR2(100), NAME VARCHAR2(100), ADDRESS VARCHAR2(100),"
                    + " ITEM_NAME VARCHAR2(100), PRICE VARCHAR2(100), STATUS VARCHAR2(100),"
                    + " CREATE_TIME TIMESTAMP DEFAULT SYSDATE, PRIMARY KEY (APPLICANT_KEY, ORDER_ID))");
        }
        return url;
    }

    /** 연결마다 지연을 주입할 수 있는 커넥션 팩토리 (실측: Oracle 접속 0.3~0.9초) */
    public static ConnectionFactory db(String url, Duration connectDelay) {
        return new ConnectionFactory(new DbConnInfo(url, "", "", "ORDER_TB")) {
            @Override
            public Connection open() throws SQLException {
                sleep(connectDelay);
                Connection c = DriverManager.getConnection(url);
                c.setAutoCommit(false);
                return c;
            }
        };
    }

    public static OrderSyncFlow orderFlow(String url, Duration dbDelay, ReceiptReceiver ftp, Path dir, Clock clock) {
        return new OrderSyncFlow(KEY, db(url, dbDelay), new IdAllocator("ORDER_TB", "ORDER_ID", KEY),
                new JdbcOrderReceiver("ORDER_TB"), new ReceiptFileMapper("홍길동", StandardCharsets.UTF_8, clock), ftp,
                new OutboundArchive(dir.resolve("archive")), new InterfaceLogWriter(dir.resolve("logs")));
    }

    public static Clock kst() {
        return Clock.system(ZoneId.of("Asia/Seoul"));
    }

    public static Clock fixed() {
        return Clock.fixed(Instant.parse("2026-10-17T01:02:03Z"), ZoneId.of("Asia/Seoul"));
    }

    /** 배치는 이 측정의 대상이 아니므로 아무 일도 하지 않는 스케줄러 */
    public static BatchScheduler idleBatch() {
        return new BatchScheduler("idle", t -> new BatchResult("x", "IF", t, BatchResult.ResultCode.NO_DATA, "",
                0, 0, 0, 0, 0, 0, 0, null, null, List.of(), null, 0), () -> { }, Duration.ofDays(1), Duration.ofDays(1));
    }

    /** 제공 샘플과 같은 모양: HEADER n명, ITEM은 사용자당 itemsPerUser개 */
    public static String sampleXml(int users, int itemsPerUser) {
        StringBuilder sb = new StringBuilder();
        for (int u = 1; u <= users; u++) {
            sb.append("<HEADER><USER_ID>USER").append(u).append("</USER_ID><NAME>홍길동").append(u)
                    .append("</NAME><ADDRESS>서울특별시 금천구 ").append(u).append("</ADDRESS><STATUS>N</STATUS></HEADER>\n");
            for (int i = 1; i <= itemsPerUser; i++) {
                sb.append("<ITEM><USER_ID>USER").append(u).append("</USER_ID><ITEM_ID>ITEM").append(u).append('_').append(i)
                        .append("</ITEM_ID><ITEM_NAME>청바지</ITEM_NAME><PRICE>21000</PRICE></ITEM>\n");
            }
        }
        return sb.toString();
    }

    /**
     * 메모리 FTP — 실서버처럼 업로드 전용. 접속·업로드 지연과 "멈춤"(latch)을 주입할 수 있다.
     * 실측: 접속 약 2.4초, 63행 업로드 약 40ms.
     */
    public static final class FakeFtp implements ReceiptReceiver {
        public final Map<String, byte[]> files = new ConcurrentHashMap<>();
        private final Duration connectDelay;
        private final Duration uploadDelay;
        private volatile CountDownLatch gate;

        public FakeFtp(Duration connectDelay, Duration uploadDelay) {
            this.connectDelay = connectDelay;
            this.uploadDelay = uploadDelay;
        }

        /** 이후 업로드를 release() 전까지 멈춘다 — "처리 중" 상태 재현 */
        public void hold() { gate = new CountDownLatch(1); }

        public void release() { if (gate != null) gate.countDown(); }

        @Override
        public String target() { return "FTP:fake/"; }

        @Override
        public ReceiptSession open() {
            sleep(connectDelay);
            return new ReceiptSession() {
                @Override
                public boolean exists(String name) { return files.containsKey(name); }

                @Override
                public void upload(ReceiptFile file) throws UploadException {
                    CountDownLatch g = gate;
                    if (g != null) {
                        try {
                            g.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    sleep(uploadDelay);
                    if (files.putIfAbsent(file.name(), file.content()) != null) {
                        throw new UploadException("동명 파일 존재: " + file.name(), false, null);
                    }
                }

                @Override
                public void close() { }
            };
        }
    }

    public static void sleep(Duration d) {
        if (d.isZero()) return;
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
