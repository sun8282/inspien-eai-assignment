package com.inspien.eai.receiver;

import com.inspien.eai.model.ReceiptFile;

import java.io.IOException;

/**
 * 영수증 파일 전송 세션.
 *
 * <p>수신 FTP 는 <b>쓰기 전용(write-once)</b> 이다 — 점검 결과 STOR 만 허용되고 RNTO(rename)·DELE(delete)는
 * {@code Operation not permitted}. 즉 한 번 올린 파일은 되돌릴 수 없다. 따라서 세션은 "존재 확인 + 1회 업로드 + 검증"만 제공한다.
 */
public interface ReceiptSession extends AutoCloseable {

    boolean exists(String name) throws IOException;

    /** 업로드 후 서버 크기로 검증. 실패 시 {@link UploadException#possiblyWritten()} 로 잔존 파일 가능성을 알린다. */
    void upload(ReceiptFile file) throws UploadException;

    @Override
    void close();

    final class UploadException extends IOException {
        private final boolean possiblyWritten;

        public UploadException(String message, boolean possiblyWritten, Throwable cause) {
            super(message, cause);
            this.possiblyWritten = possiblyWritten;
        }

        /** true = 서버에 (불완전한) 파일이 남았을 수 있음 — 삭제 권한이 없으므로 수동 조치 대상 */
        public boolean possiblyWritten() { return possiblyWritten; }
    }
}
