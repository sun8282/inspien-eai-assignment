package com.inspien.eai.infra;

import com.inspien.eai.config.ProvisionInfo;
import com.inspien.eai.config.ProvisionInfo.DbConnInfo;
import com.inspien.eai.config.ProvisionInfo.FtpConnInfo;
import com.inspien.eai.id.SequentialIdFormat;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 기동 시 사전 점검 (읽기 전용). 오류를 "런타임에 첫 요청이 실패하며" 발견하지 않고 기동 시점에 드러낸다.
 *
 * <p>심각도 구분이 핵심이다:
 * <ul>
 *   <li><b>FATAL</b> — 재시도해도 바뀌지 않는 설정 오류(계정·테이블·컬럼·FTP 경로). 기동 중단.</li>
 *   <li><b>WARN</b>  — 일시적일 수 있는 장애(네트워크 단절·타임아웃) 또는 임박한 위험(ID 잔량). 기동은 계속, 런타임 재시도에 맡긴다.</li>
 * </ul>
 * 출력에는 접속 주소·계정·비밀번호를 남기지 않는다.
 */
public final class Preflight {

    public enum Level { OK, WARN, FATAL }

    public record Check(Level level, String target, String message) {
        @Override
        public String toString() {
            String mark = switch (level) { case OK -> "✓"; case WARN -> "!"; case FATAL -> "✗"; };
            return "  " + mark + " [" + target + "] " + message;
        }
    }

    static final Map<String, List<String>> REQUIRED_COLUMNS = Map.of(
            "ORDER_TB", List.of("ORDER_ID", "APPLICANT_KEY", "USER_ID", "ITEM_ID", "NAME", "ADDRESS", "ITEM_NAME", "PRICE", "STATUS"),
            "SHIPMENT_TB", List.of("SHIPMENT_ID", "APPLICANT_KEY", "ORDER_ID", "ITEM_ID", "ADDRESS"));
    private static final int ID_WARN_REMAINING = 1_000;
    private static final int TIMEOUT_MS = 10_000;

    private final List<Check> checks = new ArrayList<>();

    public static List<Check> run(ProvisionInfo info) {
        Preflight p = new Preflight();
        p.db(info.orderDb(), info.applicantKey(), "ORDER_ID");
        p.db(info.shipmentDb(), info.applicantKey(), "SHIPMENT_ID");
        p.ftp(info.ftp());
        return List.copyOf(p.checks);
    }

    public static boolean hasFatal(List<Check> checks) {
        return checks.stream().anyMatch(c -> c.level() == Level.FATAL);
    }

    private void db(DbConnInfo conn, String applicantKey, String idColumn) {
        String table = conn.table().toUpperCase();
        try (Connection c = new ConnectionFactory(conn).open()) {
            DatabaseMetaData md = c.getMetaData();
            Set<String> columns = new LinkedHashSet<>();
            try (ResultSet rs = md.getColumns(null, null, table, null)) {
                while (rs.next()) columns.add(rs.getString("COLUMN_NAME"));
            }
            if (columns.isEmpty()) {
                add(Level.FATAL, table, "테이블을 찾을 수 없음 (권한·시노님 확인)");
                return;
            }
            List<String> missing = new ArrayList<>(REQUIRED_COLUMNS.getOrDefault(table, List.of()));
            missing.removeAll(columns);
            if (!missing.isEmpty()) {
                add(Level.FATAL, table, "필수 컬럼 누락: " + missing);
                return;
            }
            String max;
            try (PreparedStatement ps = c.prepareStatement("SELECT MAX(" + idColumn + ") FROM " + table
                    + " WHERE APPLICANT_KEY = ? AND REGEXP_LIKE(" + idColumn + ", '^[A-Z][0-9]{3}$')")) {
                ps.setQueryTimeout(10);
                ps.setString(1, applicantKey);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    max = rs.getString(1);
                }
            }
            int remaining = SequentialIdFormat.remaining(max);
            add(remaining < ID_WARN_REMAINING ? Level.WARN : Level.OK, table,
                    "접속·컬럼 정상, " + idColumn + " 최종=" + max + ", 잔여 " + remaining + "개"
                            + (remaining < ID_WARN_REMAINING ? " — 소진 임박" : ""));
        } catch (SQLException e) {
            // ORA-01017 계정 오류 / ORA-00942 테이블 없음·권한 없음 → 설정 문제
            boolean config = e.getErrorCode() == 1017 || e.getErrorCode() == 942 || e.getErrorCode() == 1031;
            add(config || !SqlErrors.isConnectionError(e) ? Level.FATAL : Level.WARN, table,
                    "[ORA-" + e.getErrorCode() + "] " + firstLine(e.getMessage())
                            + (config ? "" : SqlErrors.isConnectionError(e) ? " — 일시 장애일 수 있음, 런타임 재시도" : ""));
        }
    }

    private void ftp(FtpConnInfo conn) {
        String target = "FTP " + conn.path();
        FTPClient ftp = new FTPClient();
        ftp.setControlEncoding(StandardCharsets.UTF_8.name());
        ftp.setConnectTimeout(TIMEOUT_MS);
        ftp.setDefaultTimeout(TIMEOUT_MS);
        try {
            ftp.connect(conn.host(), conn.port());
            if (!FTPReply.isPositiveCompletion(ftp.getReplyCode())) {
                add(Level.WARN, target, "접속 거부 (reply " + ftp.getReplyCode() + ") — 런타임 재시도");
                return;
            }
            if (!ftp.login(conn.user(), conn.password())) {
                add(Level.FATAL, target, "로그인 실패 (reply " + ftp.getReplyCode() + ")");
                return;
            }
            ftp.enterLocalPassiveMode();
            if (!ftp.changeWorkingDirectory(conn.path())) {
                add(Level.FATAL, target, "업로드 경로 없음/권한 없음 (reply " + ftp.getReplyCode() + ")");
                return;
            }
            boolean utf8 = ftp.hasFeature("UTF8");
            add(utf8 ? Level.OK : Level.WARN, target, "접속·로그인·경로 정상" + (utf8 ? ", UTF8 지원" : " — UTF8 미지원: 한글 파일명 깨짐 위험"));
            ftp.logout();
        } catch (IOException e) {
            add(Level.WARN, target, e.getClass().getSimpleName() + " " + firstLine(e.getMessage()) + " — 일시 장애일 수 있음, 런타임 재시도");
        } finally {
            try {
                if (ftp.isConnected()) ftp.disconnect();
            } catch (IOException ignored) {
                // 정리 단계
            }
        }
    }

    private void add(Level level, String target, String message) {
        checks.add(new Check(level, target, message));
    }

    private static String firstLine(String s) {
        return s == null ? "" : s.lines().findFirst().orElse("");
    }
}
