package com.inspien.eai.monitor;

import com.inspien.eai.model.ReceiptFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * 송신 파일 로컬 보관. 수신 FTP 는 업로드 전용(RETR·DELE·RNTO 불가)이라 보낸 내용을 다시 확인할 방법이 없다 →
 * 증빙·장애 분석·재전송 근거로 보낸 바이트 그대로 보관한다. 위치: archive/receipt/yyyy-MM-dd/파일명
 *
 * <p>개인정보(이름·주소) 포함 → git 제외 대상, 운영 시 보관 기간 정책 필요.
 * 보관 실패가 업무 처리를 실패시키지 않는다(파일은 이미 수신측에 전달됨).
 */
public final class OutboundArchive {

    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");
    private final Path root;

    public OutboundArchive(Path root) {
        this.root = root;
    }

    /** @return 보관 경로, 실패 시 null */
    public Path save(ReceiptFile file) {
        try {
            Path dir = root.resolve("receipt").resolve(LocalDate.now(ZONE).toString());
            Files.createDirectories(dir);
            return Files.write(dir.resolve(file.name()), file.content());
        } catch (IOException e) {
            System.err.println("[WARN] 송신 파일 보관 실패: " + file.name() + " — " + e.getMessage());
            return null;
        }
    }
}
