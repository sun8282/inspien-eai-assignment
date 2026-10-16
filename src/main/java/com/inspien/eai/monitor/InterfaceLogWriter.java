package com.inspien.eai.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * 인터페이스 모니터링 로그 — 운영자용. 요청(또는 배치 실행) 1건 = JSON 한 줄, 일자별 파일.
 * 위치: logs/interface-yyyy-MM-dd.log
 *
 * <p>개인정보(이름·주소)는 쓰지 않는다. 식별자·건수·단계·에러코드만 남긴다.
 * 로그 기록 실패가 업무 처리를 실패시키지 않도록 예외를 삼키고 stderr 로만 알린다.
 */
public final class InterfaceLogWriter {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");

    private final Path dir;

    public InterfaceLogWriter(Path dir) {
        this.dir = dir;
    }

    public ObjectNode newRecord() {
        return JSON.createObjectNode();
    }

    public synchronized void write(ObjectNode record) {
        try {
            Files.createDirectories(dir);
            Path file = dir.resolve("interface-" + LocalDate.now(ZONE) + ".log");
            Files.writeString(file, JSON.writeValueAsString(record) + "\n", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("[WARN] 모니터링 로그 기록 실패: " + e.getMessage());
        }
    }
}
