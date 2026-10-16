package com.inspien.eai.tool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.inspien.eai.config.ConnectionInfoCipher;
import com.inspien.eai.config.Env;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 과제 준비용 1회성 도구.
 * <ol>
 *   <li>제공 정보 API 호출 (응답 원문은 secrets/received.json 에 저장 — 이미 있으면 재호출하지 않음)</li>
 *   <li>*_CONN 접속정보 복호화 결과 출력 (비밀번호류는 기본 마스킹, --reveal 시 평문)</li>
 *   <li>SAMPLE_DATA(base64, EUC-KR) 디코딩 → secrets/sample.xml (UTF-8)</li>
 * </ol>
 * 옵션: --refetch (저장된 응답 무시하고 재호출), --reveal (비밀번호 평문 출력)
 */
public final class FetchProvisionInfo {

    private static final String ENDPOINT =
            "https://inspien.it-cpi002-rt.cfapps.ap10.hana.ondemand.com/http/RecruitingTest";
    private static final Path SECRETS_DIR = Path.of("secrets");
    private static final Path RESPONSE_FILE = SECRETS_DIR.resolve("received.json");
    private static final Path SAMPLE_FILE = SECRETS_DIR.resolve("sample.xml");

    private static final Pattern PHONE = Pattern.compile("^010-\\d{4}-\\d{4}$");
    private static final Pattern EMAIL = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");
    private static final Pattern SECRET_KEY = Pattern.compile("(?i).*(pass|pwd|pw|secret).*");
    private static final Charset EUC_KR = Charset.forName("EUC-KR");

    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        boolean refetch = hasFlag(args, "--refetch");
        boolean reveal = hasFlag(args, "--reveal");
        Env env = Env.load();

        String name = env.require("APPLICANT_NAME");
        String phone = env.require("APPLICANT_PHONE");
        String email = env.require("APPLICANT_EMAIL");
        validate(name, phone, email);

        Files.createDirectories(SECRETS_DIR);
        String body;
        if (Files.exists(RESPONSE_FILE) && !refetch) {
            System.out.println("[INFO] 저장된 응답 사용: " + RESPONSE_FILE + " (재호출은 --refetch)");
            body = Files.readString(RESPONSE_FILE, StandardCharsets.UTF_8);
        } else {
            body = call(name, phone, email, env.require("INSPIEN_API_USER"), env.require("INSPIEN_API_PASSWORD"));
            Files.writeString(RESPONSE_FILE, body, StandardCharsets.UTF_8);
            System.out.println("[INFO] 응답 원문 저장: " + RESPONSE_FILE);
        }

        JsonNode root = JSON.readTree(body);
        System.out.println("[INFO] 응답 최상위 필드: " + fieldNames(root));
        System.out.println("APPLICANT_KEY = " + root.path("APPLICANT_KEY").asText("(없음)"));

        ConnectionInfoCipher cipher = new ConnectionInfoCipher(phone);
        for (Iterator<Map.Entry<String, JsonNode>> it = root.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> field = it.next();
            if (!field.getKey().endsWith("_CONN")) continue;
            JsonNode decrypted = decryptNode(cipher, field.getValue());
            System.out.println("\n[" + field.getKey() + "]");
            System.out.println(JSON.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(reveal ? decrypted : mask(decrypted)));
        }

        String sample = root.path("SAMPLE_DATA").asText(null);
        if (sample != null) {
            String xml = new String(Base64.getDecoder().decode(sample.trim()), EUC_KR);
            Files.writeString(SAMPLE_FILE, xml, StandardCharsets.UTF_8);
            System.out.println("\n[SAMPLE_DATA] EUC-KR 디코딩 → " + SAMPLE_FILE + " (UTF-8 저장)");
            System.out.println(xml);
        }
    }

    private static String call(String name, String phone, String email, String user, String password) throws Exception {
        ObjectNode req = JSON.createObjectNode();
        req.put("NAME", name);
        req.put("PHONE_NUMBER", phone);
        req.put("E_MAIL", email);

        String basic = Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
        HttpRequest request = HttpRequest.newBuilder(URI.create(ENDPOINT))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json; charset=UTF-8")
                .header("Authorization", "Basic " + basic)
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(req), StandardCharsets.UTF_8))
                .build();

        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        HttpResponse<String> res = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        System.out.println("[INFO] HTTP " + res.statusCode());
        if (res.statusCode() / 100 != 2) {
            String b = res.body();
            throw new IllegalStateException("제공 정보 API 호출 실패: HTTP " + res.statusCode()
                    + "\n" + (b.length() > 2000 ? b.substring(0, 2000) + "..." : b));
        }
        return res.body();
    }

    /** 문자열이면 통째로, 객체면 필드별로 복호화. 복호화 결과가 JSON 이면 파싱. */
    private static JsonNode decryptNode(ConnectionInfoCipher cipher, JsonNode node) {
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
                        out.set(f.getKey(), f.getValue()); // 평문 필드는 그대로
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
            } catch (Exception ignored) {
                // JSON 이 아니면 문자열로
            }
        }
        return TextNode.valueOf(text);
    }

    private static JsonNode mask(JsonNode node) {
        if (!node.isObject()) return node;
        ObjectNode out = JSON.createObjectNode();
        node.fields().forEachRemaining(f -> out.set(f.getKey(),
                SECRET_KEY.matcher(f.getKey()).matches() ? TextNode.valueOf("****") : mask(f.getValue())));
        return out;
    }

    private static void validate(String name, String phone, String email) {
        if (!PHONE.matcher(phone).matches()) {
            throw new IllegalArgumentException("APPLICANT_PHONE 형식은 010-1234-5678 이어야 합니다: " + phone);
        }
        if (!EMAIL.matcher(email).matches()) {
            throw new IllegalArgumentException("APPLICANT_EMAIL 형식 오류: " + email);
        }
        if (name.isBlank()) {
            throw new IllegalArgumentException("APPLICANT_NAME 이 비어 있습니다");
        }
    }

    private static String fieldNames(JsonNode root) {
        StringBuilder sb = new StringBuilder();
        root.fieldNames().forEachRemaining(n -> sb.append(sb.length() == 0 ? "" : ", ").append(n));
        return sb.toString();
    }

    private static boolean hasFlag(String[] args, String flag) {
        for (String a : args) if (a.equals(flag)) return true;
        return false;
    }
}
