package com.inspien.eai.tool;

import com.inspien.eai.config.Env;
import com.inspien.eai.config.ProvisionInfo;
import com.inspien.eai.config.ProvisionInfo.DbConnInfo;
import com.inspien.eai.config.ProvisionInfo.FtpConnInfo;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 접속 점검 도구 (읽기 전용 — 어떤 데이터도 쓰지 않는다).
 * <ul>
 *   <li>DB: 접속, 테이블 컬럼·PK를 과제 명세와 대조, 내 APPLICANT_KEY 기준 기존 데이터 현황</li>
 *   <li>FTP: 접속·로그인, UTF-8 지원, 업로드 경로 이동, 기존 파일 수(파일명은 타 지원자 개인정보라 출력하지 않음)</li>
 * </ul>
 * 출력에는 접속 주소·계정·비밀번호를 남기지 않는다.
 */
public final class ConnectivityCheck {

    private static final Map<String, List<String>> SPEC = Map.of(
            "ORDER_TB", List.of("ORDER_ID", "APPLICANT_KEY", "USER_ID", "ITEM_ID", "NAME", "ADDRESS", "ITEM_NAME", "PRICE", "STATUS"),
            "SHIPMENT_TB", List.of("SHIPMENT_ID", "APPLICANT_KEY", "ORDER_ID", "ITEM_ID", "ADDRESS"));
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private int failures = 0;

    public static void main(String[] args) {
        Env env = Env.load();
        ProvisionInfo info = ProvisionInfo.load(env);
        ConnectivityCheck check = new ConnectivityCheck();

        System.out.println("APPLICANT_KEY = " + info.applicantKey());
        System.out.println("ORDER_TB / SHIPMENT_TB 동일 계정: " + info.orderDb().sameAccountAs(info.shipmentDb())
                + " (true 면 한 트랜잭션으로 묶을 수 있음)");

        check.db(info.orderDb(), info.applicantKey(), "ORDER_ID");
        check.db(info.shipmentDb(), info.applicantKey(), "SHIPMENT_ID");
        check.ftp(info.ftp(), env.get("APPLICANT_NAME"));

        System.out.println("\n=== 결과: " + (check.failures == 0 ? "모두 정상" : "실패 " + check.failures + "건"));
        if (check.failures > 0) System.exit(1);
    }

    private void db(DbConnInfo conn, String applicantKey, String idColumn) {
        String table = conn.table().toUpperCase();
        System.out.println("\n[DB " + table + "] " + conn);
        Properties props = new Properties();
        props.setProperty("user", conn.user());
        props.setProperty("password", conn.password());
        props.setProperty("oracle.net.CONNECT_TIMEOUT", String.valueOf(TIMEOUT.toMillis()));
        props.setProperty("oracle.jdbc.ReadTimeout", String.valueOf(TIMEOUT.toMillis() * 3));
        DriverManager.setLoginTimeout((int) TIMEOUT.toSeconds());

        long started = System.nanoTime();
        try (Connection c = DriverManager.getConnection(conn.url(), props)) {
            DatabaseMetaData md = c.getMetaData();
            System.out.printf("  접속 OK (%d ms) — %s%n", (System.nanoTime() - started) / 1_000_000,
                    md.getDatabaseProductVersion().lines().findFirst().orElse(""));

            Set<String> schemas = new LinkedHashSet<>();
            Map<String, String> columns = new LinkedHashMap<>();
            try (ResultSet rs = md.getColumns(null, null, table, null)) {
                while (rs.next()) {
                    // Oracle: COLUMN_DEF 가 LONG 스트림이라 결과셋 컬럼 순서대로 읽어야 함
                    String schema = rs.getString("TABLE_SCHEM");
                    String name = rs.getString("COLUMN_NAME");
                    String typeName = rs.getString("TYPE_NAME");
                    int size = rs.getInt("COLUMN_SIZE");
                    String def = rs.getString("COLUMN_DEF");
                    String nullable = rs.getString("IS_NULLABLE");
                    schemas.add(schema);
                    columns.putIfAbsent(name, typeName + "(" + size + ")"
                            + ("NO".equals(nullable) ? " NOT NULL" : "")
                            + (def == null ? "" : " DEFAULT " + def.strip()));
                }
            }
            if (columns.isEmpty()) {
                fail("테이블 " + table + " 을 찾을 수 없음 (권한/시노님 확인 필요)");
                return;
            }
            boolean ownSchema = schemas.stream().anyMatch(s -> s.equalsIgnoreCase(conn.user()));
            System.out.println("  테이블 위치: 스키마 " + schemas.size() + "개" + (ownSchema ? " (접속 계정 소유)" : " (타 스키마 — 시노님/스키마 접두 필요할 수 있음)"));
            columns.forEach((name, type) -> System.out.printf("    %-14s %s%n", name, type));

            List<String> spec = SPEC.getOrDefault(table, List.of());
            List<String> missing = new ArrayList<>(spec);
            missing.removeAll(columns.keySet());
            List<String> extra = new ArrayList<>(columns.keySet());
            extra.removeAll(spec);
            if (!missing.isEmpty()) fail("명세 대비 누락 컬럼: " + missing);
            System.out.println("  명세에 없는 추가 컬럼: " + (extra.isEmpty() ? "없음" : extra));

            List<String> pk = new ArrayList<>();
            for (String schema : schemas) {
                try (ResultSet rs = md.getPrimaryKeys(null, schema, table)) {
                    while (rs.next()) pk.add(rs.getString("COLUMN_NAME"));
                }
            }
            System.out.println("  PK: " + (pk.isEmpty() ? "(조회 불가)" : pk));

            String sql = "SELECT COUNT(*), MAX(" + idColumn + ")"
                    + (columns.containsKey("STATUS") ? ", SUM(CASE WHEN STATUS = 'N' THEN 1 ELSE 0 END)" : "")
                    + " FROM " + table + " WHERE APPLICANT_KEY = ?";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setQueryTimeout((int) TIMEOUT.toSeconds());
                ps.setString(1, applicantKey);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    System.out.println("  내 데이터: " + rs.getLong(1) + "건, 최대 " + idColumn + "=" + rs.getString(2)
                            + (columns.containsKey("STATUS") ? ", STATUS='N' " + rs.getLong(3) + "건" : ""));
                }
            }
        } catch (SQLException e) {
            fail("DB 오류 [" + e.getSQLState() + "/" + e.getErrorCode() + "] " + firstLine(e.getMessage()));
        }
    }

    private void ftp(FtpConnInfo conn, String applicantName) {
        System.out.println("\n[FTP] " + conn);
        FTPClient ftp = new FTPClient();
        ftp.setControlEncoding(StandardCharsets.UTF_8.name()); // 한글 파일명
        ftp.setConnectTimeout((int) TIMEOUT.toMillis());
        ftp.setDefaultTimeout((int) TIMEOUT.toMillis());
        long started = System.nanoTime();
        try {
            ftp.connect(conn.host(), conn.port());
            if (!FTPReply.isPositiveCompletion(ftp.getReplyCode())) {
                fail("FTP 접속 거부: reply " + ftp.getReplyCode());
                return;
            }
            if (!ftp.login(conn.user(), conn.password())) {
                fail("FTP 로그인 실패: reply " + ftp.getReplyCode());
                return;
            }
            System.out.printf("  접속·로그인 OK (%d ms), 시스템: %s%n", (System.nanoTime() - started) / 1_000_000, ftp.getSystemType());
            ftp.enterLocalPassiveMode();
            boolean utf8 = ftp.hasFeature("UTF8");
            System.out.println("  UTF8 지원(FEAT): " + utf8 + (utf8 ? "" : " — 한글 파일명 깨짐 주의"));
            if (utf8) {
                System.out.println("  OPTS UTF8 ON: reply " + ftp.sendCommand("OPTS UTF8 ON"));
            }
            if (!ftp.changeWorkingDirectory(conn.path())) {
                fail("업로드 경로 이동 실패: " + conn.path() + " (reply " + ftp.getReplyCode() + ")");
                return;
            }
            System.out.println("  업로드 경로 이동 OK: " + ftp.printWorkingDirectory());
            String[] names = ftp.listNames();
            if (names == null) {
                fail("파일 목록 조회 실패 (passive 데이터 연결 확인): reply " + ftp.getReplyCode());
                return;
            }
            String mine = applicantName == null ? null : "INSPIEN_" + applicantName + "_";
            long myFiles = mine == null ? 0 : java.util.Arrays.stream(names).filter(n -> n.contains(mine)).count();
            System.out.println("  경로 내 파일: " + names.length + "개, 내 파일(INSPIEN_<이름>_*): " + myFiles + "개");
            ftp.logout();
        } catch (Exception e) {
            fail("FTP 오류: " + e.getClass().getSimpleName() + " " + firstLine(e.getMessage()));
        } finally {
            try {
                if (ftp.isConnected()) ftp.disconnect();
            } catch (Exception ignored) {
                // 정리 단계 오류는 무시
            }
        }
    }

    private void fail(String message) {
        failures++;
        System.out.println("  ✗ " + message);
    }

    private static String firstLine(String s) {
        return s == null ? "" : s.lines().findFirst().orElse("");
    }
}
