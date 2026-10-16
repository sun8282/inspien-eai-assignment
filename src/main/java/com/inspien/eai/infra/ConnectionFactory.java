package com.inspien.eai.infra;

import com.inspien.eai.config.ProvisionInfo.DbConnInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * DB 커넥션 생성 (Receiver Channel 의 접속 설정).
 * 연결 실패는 일시적일 수 있어 짧은 백오프로 재시도한다 — 동기 API 라 총 대기시간 상한을 둔다.
 */
public class ConnectionFactory {

    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 30_000;
    private static final long[] BACKOFF_MS = {300, 1_000};

    private final DbConnInfo info;

    public ConnectionFactory(DbConnInfo info) {
        this.info = info;
    }

    public Connection open() throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", info.user());
        p.setProperty("password", info.password());
        p.setProperty("oracle.net.CONNECT_TIMEOUT", String.valueOf(CONNECT_TIMEOUT_MS));
        p.setProperty("oracle.jdbc.ReadTimeout", String.valueOf(READ_TIMEOUT_MS));

        SQLException last = null;
        for (int attempt = 0; attempt <= BACKOFF_MS.length; attempt++) {
            try {
                Connection c = DriverManager.getConnection(info.url(), p);
                c.setAutoCommit(false);
                return c;
            } catch (SQLException e) {
                last = e;
                if (!SqlErrors.isConnectionError(e) || attempt == BACKOFF_MS.length) break;
                sleep(BACKOFF_MS[attempt]);
            }
        }
        throw last;
    }

    public DbConnInfo info() { return info; }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
