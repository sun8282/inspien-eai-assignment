package com.inspien.eai.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * 비밀값 주입 경로. 우선순위: OS 환경변수 → 작업 디렉터리의 .env 파일.
 * 비밀값은 코드·커밋 대상 설정에 두지 않는다.
 */
public final class Env {

    private final Map<String, String> dotEnv;

    private Env(Map<String, String> dotEnv) {
        this.dotEnv = dotEnv;
    }

    public static Env load() {
        return load(Path.of(".env"));
    }

    static Env load(Path dotEnvFile) {
        Map<String, String> values = new HashMap<>();
        if (Files.isRegularFile(dotEnvFile)) {
            try {
                for (String line : Files.readAllLines(dotEnvFile, StandardCharsets.UTF_8)) {
                    String trimmed = line.strip();
                    if (trimmed.isEmpty() || trimmed.startsWith("#")) continue;
                    int eq = trimmed.indexOf('=');
                    if (eq <= 0) continue;
                    values.put(trimmed.substring(0, eq).strip(), unquote(trimmed.substring(eq + 1).strip()));
                }
            } catch (IOException e) {
                throw new UncheckedIOException(".env 읽기 실패: " + dotEnvFile, e);
            }
        }
        return new Env(values);
    }

    private static String unquote(String v) {
        if (v.length() >= 2 && (v.startsWith("\"") && v.endsWith("\"") || v.startsWith("'") && v.endsWith("'"))) {
            return v.substring(1, v.length() - 1);
        }
        return v;
    }

    public String get(String name) {
        String fromOs = System.getenv(name);
        if (fromOs != null && !fromOs.isBlank()) return fromOs.strip();
        String fromFile = dotEnv.get(name);
        return fromFile == null || fromFile.isBlank() ? null : fromFile;
    }

    /** 없으면 기동 중단(Fail-fast). */
    public String require(String name) {
        String v = get(name);
        if (v == null) {
            throw new IllegalStateException("필수 설정 누락: " + name + " (환경변수 또는 .env 에 지정)");
        }
        return v;
    }
}
