package com.inspien.eai.sender;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inspien.eai.support.TestInfra;
import com.inspien.eai.support.TestInfra.FakeFtp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RestSenderOverloadTest {

    @TempDir
    Path dir;

    @Test
    void 동시처리_상한을_넘으면_처리_전에_즉시_503_재시도가능() throws Exception {
        String url = TestInfra.newDatabase();
        FakeFtp ftp = new FakeFtp(Duration.ZERO, Duration.ZERO);
        ftp.hold(); // 첫 요청을 "처리 중"에 묶어둔다
        RestSender rest = new RestSender(0, TestInfra.orderFlow(url, Duration.ZERO, ftp, dir, TestInfra.kst()),
                TestInfra.idleBatch(), new RestSender.Limits(1, 4, 10));
        rest.start();
        HttpClient http = HttpClient.newHttpClient();
        URI orders = URI.create("http://localhost:" + rest.port() + "/orders");
        HttpRequest req = HttpRequest.newBuilder(orders)
                .POST(HttpRequest.BodyPublishers.ofString(TestInfra.sampleXml(1, 2))).header("Content-Type", "application/xml; charset=UTF-8").build();
        try {
            CompletableFuture<HttpResponse<String>> first = http.sendAsync(req, HttpResponse.BodyHandlers.ofString());
            waitUntilInFlight(http, rest.port());

            long t = System.nanoTime();
            HttpResponse<String> second = http.send(req, HttpResponse.BodyHandlers.ofString());
            long ms = (System.nanoTime() - t) / 1_000_000;
            JsonNode body = new ObjectMapper().readTree(second.body());
            assertEquals(503, second.statusCode());
            assertEquals("OVERLOADED", body.get("errorCode").asText());
            assertTrue(body.get("retryable").asBoolean());
            assertEquals("1", second.headers().firstValue("Retry-After").orElse(null));
            assertTrue(ms < 1_000, "대기하지 않고 즉시 거절: " + ms + "ms");

            ftp.release();
            assertEquals(200, first.get(10, TimeUnit.SECONDS).statusCode(), "이미 받은 요청은 정상 완료");
            assertEquals(1, rest.rejectedCount());
            assertEquals(1, ftp.files.size(), "거절된 요청은 아무것도 쓰지 않음");
        } finally {
            ftp.release();
            rest.stop(0);
        }
    }

    private static void waitUntilInFlight(HttpClient http, int port) throws Exception {
        HttpRequest health = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/health")).build();
        for (int i = 0; i < 200; i++) {
            JsonNode h = new ObjectMapper().readTree(http.send(health, HttpResponse.BodyHandlers.ofString()).body());
            if (h.get("ordersInFlight").asInt() == 1) return;
            Thread.sleep(10);
        }
        fail("첫 요청이 처리 중 상태가 되지 않음");
    }
}
