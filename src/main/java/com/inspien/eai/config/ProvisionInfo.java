package com.inspien.eai.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

/**
 * 과제 제공 정보(접속정보 + 지원자 키 + 샘플).
 *
 * <p>원천은 암호문 그대로 보관된 응답 파일이며, 접속정보는 {@link #load} 시점에만 복호화되어 메모리에 머문다.
 * 각 접속정보 레코드의 {@code toString()}은 주소·계정·비밀번호를 가린다 — 로그에 실수로 찍혀도 안전하도록.
 */
public final class ProvisionInfo {

    public static final Path DEFAULT_FILE = Path.of("secrets", "received.json");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Charset EUC_KR = Charset.forName("EUC-KR");

    private final String applicantKey;
    private final DbConnInfo orderDb;
    private final DbConnInfo shipmentDb;
    private final FtpConnInfo ftp;
    private final String sampleXml;

    private ProvisionInfo(String applicantKey, DbConnInfo orderDb, DbConnInfo shipmentDb, FtpConnInfo ftp, String sampleXml) {
        this.applicantKey = applicantKey;
        this.orderDb = orderDb;
        this.shipmentDb = shipmentDb;
        this.ftp = ftp;
        this.sampleXml = sampleXml;
    }

    /** 환경변수/.env 의 PROVISION_FILE(기본 secrets/received.json)과 APPLICANT_PHONE(복호화 키)으로 로딩. */
    public static ProvisionInfo load(Env env) {
        String file = env.get("PROVISION_FILE");
        return load(file == null ? DEFAULT_FILE : Path.of(file), new ConnectionInfoCipher(env.require("APPLICANT_PHONE")));
    }

    public static ProvisionInfo load(Path file, ConnectionInfoCipher cipher) {
        JsonNode root;
        try {
            root = JSON.readTree(Files.readString(file, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("제공 정보 파일을 읽을 수 없습니다: " + file + " (먼저 gradle fetchInfo 실행)", e);
        }
        JsonNode order = decrypt(cipher, root.path("ORDER_TB_CONN"));
        JsonNode shipment = decrypt(cipher, root.path("SHIPMENT_TB_CONN"));
        JsonNode ftp = decrypt(cipher, root.path("FTP_CONN"));
        String sample = root.path("SAMPLE_DATA").asText(null);

        return new ProvisionInfo(
                required(root, "APPLICANT_KEY"),
                new DbConnInfo(required(order, "URL"), required(order, "ID"), required(order, "PASSWORD"), required(order, "TABLE")),
                new DbConnInfo(required(shipment, "URL"), required(shipment, "ID"), required(shipment, "PASSWORD"), required(shipment, "TABLE")),
                new FtpConnInfo(required(ftp, "URL"), Integer.parseInt(required(ftp, "PORT")),
                        required(ftp, "ID"), required(ftp, "PASSWORD"), required(ftp, "PATH")),
                sample == null ? null : new String(Base64.getDecoder().decode(sample.trim()), EUC_KR));
    }

    /** 문자열이면 통째로, 객체면 필드별로 복호화(복호화되지 않는 필드는 평문으로 간주). 결과가 JSON 이면 파싱. */
    public static JsonNode decrypt(ConnectionInfoCipher cipher, JsonNode node) {
        if (node.isTextual()) {
            return parseIfJson(cipher.decrypt(node.asText()));
        }
        if (node.isObject()) {
            ObjectNode out = JSON.createObjectNode();
            node.fields().forEachRemaining(f -> {
                if (f.getValue().isTextual()) {
                    try {
                        out.set(f.getKey(), parseIfJson(cipher.decrypt(f.getValue().asText())));
                    } catch (ConnectionInfoCipher.DecryptionException e) {
                        out.set(f.getKey(), f.getValue());
                    }
                } else {
                    out.set(f.getKey(), f.getValue());
                }
            });
            return out;
        }
        return node;
    }

    private static JsonNode parseIfJson(String text) {
        String t = text.strip();
        if (t.startsWith("{") || t.startsWith("[")) {
            try {
                return JSON.readTree(t);
            } catch (IOException ignored) {
                // JSON 이 아니면 문자열로
            }
        }
        return TextNode.valueOf(text);
    }

    private static String required(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull() || v.asText().isBlank()) {
            throw new IllegalStateException("제공 정보에 필수 필드 누락: " + field);
        }
        return v.asText().strip();
    }

    public String applicantKey() { return applicantKey; }
    public DbConnInfo orderDb() { return orderDb; }
    public DbConnInfo shipmentDb() { return shipmentDb; }
    public FtpConnInfo ftp() { return ftp; }
    public String sampleXml() { return sampleXml; }

    public record DbConnInfo(String url, String user, String password, String table) {
        /** 같은 DB·같은 계정이면 한 커넥션(한 트랜잭션)으로 묶을 수 있다. */
        public boolean sameAccountAs(DbConnInfo other) {
            return url.equals(other.url) && user.equals(other.user);
        }

        @Override
        public String toString() {
            int at = url.indexOf('@');
            String safeUrl = at > 0 ? url.substring(0, at + 1) + "***" : "***";
            return "DbConnInfo[url=" + safeUrl + ", user=***, password=****, table=" + table + "]";
        }
    }

    public record FtpConnInfo(String host, int port, String user, String password, String path) {
        @Override
        public String toString() {
            return "FtpConnInfo[host=***, port=" + port + ", user=***, password=****, path=" + path + "]";
        }
    }
}
