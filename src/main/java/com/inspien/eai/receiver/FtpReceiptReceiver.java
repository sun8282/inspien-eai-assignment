package com.inspien.eai.receiver;

import com.inspien.eai.config.ProvisionInfo.FtpConnInfo;
import com.inspien.eai.model.ReceiptFile;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Receiver Channel (FTP → 회계 담당자 영수증).
 * 서버 특성(점검 결과): UNIX, FEAT UTF8 지원(한글 파일명), passive 모드, 비표준 포트,
 * <b>쓰기 전용</b>(RNTO·DELE 불가 → 451/550 Operation not permitted).
 */
public final class FtpReceiptReceiver implements ReceiptReceiver {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final long[] BACKOFF_MS = {300, 1_000};

    private final FtpConnInfo info;

    public FtpReceiptReceiver(FtpConnInfo info) {
        this.info = info;
    }

    @Override
    public String target() {
        return "FTP:" + info.path();
    }

    @Override
    public ReceiptSession open() throws IOException {
        return new Session(connectWithRetry());
    }

    private FTPClient connectWithRetry() throws IOException {
        IOException last = null;
        for (int attempt = 0; attempt <= BACKOFF_MS.length; attempt++) {
            try {
                return connect();
            } catch (FtpLoginException e) {
                throw e; // 계정 오류는 재시도해도 같음
            } catch (IOException e) {
                last = e;
                if (attempt < BACKOFF_MS.length) sleep(BACKOFF_MS[attempt]);
            }
        }
        throw last;
    }

    private FTPClient connect() throws IOException {
        FTPClient ftp = new FTPClient();
        ftp.setControlEncoding(StandardCharsets.UTF_8.name());
        ftp.setConnectTimeout((int) TIMEOUT.toMillis());
        ftp.setDefaultTimeout((int) TIMEOUT.toMillis());
        ftp.setDataTimeout(TIMEOUT);
        try {
            ftp.connect(info.host(), info.port());
            if (!FTPReply.isPositiveCompletion(ftp.getReplyCode())) {
                throw new IOException("FTP 접속 거부 (reply " + ftp.getReplyCode() + ")");
            }
            ftp.setSoTimeout((int) TIMEOUT.toMillis());
            if (!ftp.login(info.user(), info.password())) {
                throw new FtpLoginException("FTP 로그인 실패 (reply " + ftp.getReplyCode() + ")");
            }
            ftp.enterLocalPassiveMode();
            ftp.setFileType(FTP.BINARY_FILE_TYPE); // 바이트 그대로 전송 — LF·인코딩 변형 방지
            if (!ftp.changeWorkingDirectory(info.path())) {
                throw new IOException("FTP 경로 이동 실패: " + info.path() + " (reply " + ftp.getReplyCode() + ")");
            }
            return ftp;
        } catch (IOException e) {
            disconnectQuietly(ftp);
            throw e;
        }
    }

    private static final class Session implements ReceiptSession {
        private final FTPClient ftp;

        private Session(FTPClient ftp) {
            this.ftp = ftp;
        }

        @Override
        public boolean exists(String name) throws IOException {
            // SIZE: 있으면 213, 없으면 550 → getSize 가 null
            return ftp.getSize(name) != null;
        }

        @Override
        public void upload(ReceiptFile file) throws UploadException {
            boolean stored;
            try {
                stored = ftp.storeFile(file.name(), new ByteArrayInputStream(file.content()));
            } catch (IOException e) {
                // 데이터 전송 중 끊김 — 일부 바이트가 서버에 남았을 수 있음
                throw new UploadException("업로드 중 연결 오류: " + e.getMessage(), true, e);
            }
            if (!stored) {
                // 거절 응답(권한/경로 등) — 파일이 생성되지 않았을 가능성이 높지만 확인한다
                throw new UploadException("업로드 거절 (reply " + ftp.getReplyCode() + " " + ftp.getReplyString().strip() + ")",
                        existsQuietly(file.name()), null);
            }
            String size;
            try {
                size = ftp.getSize(file.name());
            } catch (IOException e) {
                throw new UploadException("업로드 검증(SIZE) 실패: " + e.getMessage(), true, e);
            }
            if (size == null || Long.parseLong(size.strip()) != file.content().length) {
                throw new UploadException("업로드 크기 불일치: 기대 " + file.content().length + " / 서버 " + size, true, null);
            }
        }

        private boolean existsQuietly(String name) {
            try {
                return exists(name);
            } catch (IOException e) {
                return true; // 모르면 남았다고 가정(보수적)
            }
        }

        @Override
        public void close() {
            try {
                if (ftp.isConnected()) ftp.logout();
            } catch (IOException ignored) {
                // 종료 단계
            }
            disconnectQuietly(ftp);
        }
    }

    public static final class FtpLoginException extends IOException {
        public FtpLoginException(String message) { super(message); }
    }

    private static void disconnectQuietly(FTPClient ftp) {
        try {
            if (ftp.isConnected()) ftp.disconnect();
        } catch (IOException ignored) {
            // 정리 단계
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
