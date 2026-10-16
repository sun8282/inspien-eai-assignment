package com.inspien.eai.infra;

import com.inspien.eai.core.EaiException;
import com.inspien.eai.core.ErrorCode;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;

/** SQLException → 표준 에러코드. 재시도 여부 판단의 근거. */
public final class SqlErrors {

    private SqlErrors() { }

    /** ORA-00001: unique constraint violated */
    public static boolean isDuplicateKey(SQLException e) {
        return e.getErrorCode() == 1 || "23000".equals(e.getSQLState()) && e.getMessage() != null && e.getMessage().contains("ORA-00001");
    }

    public static boolean isConnectionError(SQLException e) {
        String state = e.getSQLState();
        int code = e.getErrorCode();
        // 08xxx: 연결 예외 / ORA-17002 IO 오류 / 17008 닫힌 연결 / 17410 소켓 읽기 불가 / 12514·12541 리스너
        return state != null && state.startsWith("08")
                || code == 17002 || code == 17008 || code == 17410 || code == 12514 || code == 12541;
    }

    public static boolean isTimeout(SQLException e) {
        // ORA-01013: 사용자 요청 취소(쿼리 타임아웃)
        return e instanceof SQLTimeoutException || e.getErrorCode() == 1013;
    }

    public static EaiException translate(SQLException e, String action) {
        String msg = action + " — [ORA-" + e.getErrorCode() + "] " + firstLine(e.getMessage());
        if (isDuplicateKey(e)) return new EaiException(ErrorCode.DB_DUPLICATE_KEY, msg, e);
        if (isTimeout(e)) return new EaiException(ErrorCode.DB_TIMEOUT, msg, e);
        if (isConnectionError(e)) return new EaiException(ErrorCode.DB_CONNECTION, msg, e);
        return new EaiException(ErrorCode.DB_ERROR, msg, e);
    }

    private static String firstLine(String s) {
        return s == null ? "" : s.lines().findFirst().orElse("");
    }
}
