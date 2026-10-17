package com.inspien.eai.load;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inspien.eai.sender.RestSender;
import com.inspien.eai.support.TestInfra;
import com.inspien.eai.support.TestInfra.FakeFtp;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 로컬 부하 측정 (운영 DB·FTP 미사용 — 회사 서버에는 지울 수 없는 파일·행이 쌓이므로 절대 부하를 주지 않는다).
 *
 * <p>실제 RestSender·OrderSyncFlow 코드를 그대로 띄우고, 외부 시스템만 H2 + 가짜 FTP 로 바꾼 뒤
 * 실서버에서 잰 지연(DB 접속 ~0.3초, FTP 접속 ~2.4초, 업로드 ~40ms)을 주입한다.
 * 부하는 open-loop(초당 고정 도착률) — 실제 사용자는 서버가 느려졌다고 덜 보내지 않기 때문.
 *
 * <pre>./gradlew loadTest</pre>
 */
public final class LoadTest {

    record Scenario(String name, RestSender.Limits limits, double ratePerSec, int durationSec,
                    Duration dbDelay, Duration ftpConnect, Duration ftpUpload, Duration clientTimeout) { }

    record Sample(int status, String errorCode, long latencyMs, String receiptFile, LocalDateTime completedAt) { }

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final int USERS = 5;
    private static final int ITEMS_PER_USER = 2; // 요청당 10행 (ID 한도 내에서 다수 요청)

    public static void main(String[] args) throws Exception {
        Duration db = Duration.ofMillis(300);
        Duration ftpConnect = Duration.ofMillis(2_400);
        Duration ftpUpload = Duration.ofMillis(40);
        Duration timeout = Duration.ofSeconds(30);
        List<Scenario> scenarios = List.of(
                new Scenario("A. 평시 2건/초 — 과부하 차단", RestSender.Limits.defaults(), 2, 15, db, ftpConnect, ftpUpload, timeout),
                new Scenario("B. 몰림 10건/초 — 차단 없음(도입 전)", RestSender.Limits.legacyUnbounded(), 10, 15, db, ftpConnect, ftpUpload, timeout),
                new Scenario("C. 몰림 10건/초 — 과부하 차단", RestSender.Limits.defaults(), 10, 15, db, ftpConnect, ftpUpload, timeout),
                new Scenario("D. 외부 지연 0 · 50건/초 — 과부하 차단", RestSender.Limits.defaults(), 50, 10, Duration.ZERO, Duration.ZERO, Duration.ZERO, timeout));

        StringBuilder md = new StringBuilder();
        md.append("| 시나리오 | 요청 | 200 | 503(거절) | 클라이언트 타임아웃 | 처리량(성공/초) | 성공 응답 p50 / p95 / max | 거절 응답 p95 | DB 적재 요청 수 | 적재됐지만 응답 못 받음 | 영수증 파일명 최대 앞당김 |\n");
        md.append("|---|---|---|---|---|---|---|---|---|---|---|\n");
        for (Scenario s : scenarios) {
            System.out.println("\n▶ " + s.name() + " 실행 중...");
            md.append(run(s)).append('\n');
        }
        Path out = Path.of("build", "load-test", "result.md");
        Files.createDirectories(out.getParent());
        String env = "측정 환경: " + System.getProperty("os.name") + " / Java " + Runtime.version() + " / CPU " + Runtime.getRuntime().availableProcessors()
                + "코어, 요청당 " + USERS * ITEMS_PER_USER + "행, 주입 지연: DB 접속 " + db.toMillis() + "ms · FTP 접속 " + ftpConnect.toMillis() + "ms · 업로드 " + ftpUpload.toMillis()
                + "ms (D 제외), 클라이언트 타임아웃 " + timeout.toSeconds() + "초\n\n";
        Files.writeString(out, env + md, StandardCharsets.UTF_8);
        System.out.println("\n" + env + md + "\n→ " + out);
    }

    private static String run(Scenario s) throws Exception {
        Path dir = Files.createTempDirectory("loadtest");
        String url = TestInfra.newDatabase();
        FakeFtp ftp = new FakeFtp(s.ftpConnect(), s.ftpUpload());
        RestSender rest = new RestSender(0, TestInfra.orderFlow(url, s.dbDelay(), ftp, dir, TestInfra.kst()), TestInfra.idleBatch(), s.limits());
        rest.start();

        HttpClient http = HttpClient.newBuilder().executor(Executors.newCachedThreadPool()).connectTimeout(Duration.ofSeconds(5)).build();
        byte[] body = TestInfra.sampleXml(USERS, ITEMS_PER_USER).getBytes(StandardCharsets.UTF_8);
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + rest.port() + "/orders"))
                .timeout(s.clientTimeout()).header("Content-Type", "application/xml; charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();

        int total = (int) Math.round(s.ratePerSec() * s.durationSec());
        long intervalNanos = (long) (1_000_000_000L / s.ratePerSec());
        ConcurrentLinkedQueue<Sample> samples = new ConcurrentLinkedQueue<>();
        List<CompletableFuture<Void>> inflight = new ArrayList<>();
        var ticker = Executors.newSingleThreadScheduledExecutor();
        long t0 = System.nanoTime();
        for (int i = 0; i < total; i++) {
            long due = t0 + i * intervalNanos;
            CompletableFuture<Void> f = new CompletableFuture<>();
            inflight.add(f);
            ticker.schedule(() -> {
                long start = System.nanoTime();
                http.sendAsync(req, HttpResponse.BodyHandlers.ofString()).whenComplete((res, err) -> {
                    long ms = (System.nanoTime() - start) / 1_000_000;
                    if (err != null) {
                        samples.add(new Sample(-1, "CLIENT_TIMEOUT", ms, null, LocalDateTime.now(KST)));
                    } else {
                        String code = null;
                        String file = null;
                        try {
                            JsonNode n = JSON.readTree(res.body());
                            code = n.path("errorCode").isNull() ? null : n.path("errorCode").asText(null);
                            file = n.path("receiptFile").asText(null);
                        } catch (Exception ignored) {
                            // 본문 해석 실패는 상태코드만 기록
                        }
                        samples.add(new Sample(res.statusCode(), code, ms, file, LocalDateTime.now(KST)));
                    }
                    f.complete(null);
                });
            }, Math.max(0, due - System.nanoTime()), TimeUnit.NANOSECONDS);
        }
        CompletableFuture.allOf(inflight.toArray(new CompletableFuture[0])).get(10, TimeUnit.MINUTES);
        double wallSec = (System.nanoTime() - t0) / 1e9;
        ticker.shutdown();

        // 클라이언트가 포기한 뒤에도 서버는 계속 처리한다 → 대기열이 비울 때까지 기다린 뒤 DB 를 센다
        int committedRequests = waitStableOrderCount(url) / (USERS * ITEMS_PER_USER);
        rest.stop(0);

        List<Sample> all = new ArrayList<>(samples);
        List<Long> ok = latencies(all, 200);
        List<Long> rejected = latencies(all, 503);
        long timeouts = all.stream().filter(x -> x.status() == -1).count();
        long drift = all.stream().filter(x -> x.receiptFile() != null).mapToLong(x -> {
            String ts = x.receiptFile().replaceAll(".*_(\\d{14})\\.txt$", "$1");
            return Duration.between(x.completedAt(), LocalDateTime.parse(ts, TS)).toSeconds();
        }).max().orElse(0);
        long lostResponses = Math.max(0, committedRequests - ok.size());

        return String.format("| %s | %d | %d | %d | %d | %.1f | %s / %s / %s | %s | %d | %s | %s |",
                s.name(), all.size(), ok.size(), rejected.size(), timeouts, ok.size() / wallSec,
                fmt(pct(ok, 50)), fmt(pct(ok, 95)), fmt(pct(ok, 100)), fmt(pct(rejected, 95)),
                committedRequests, lostResponses == 0 ? "0" : "**" + lostResponses + "**",
                drift <= 1 ? drift + "초" : "**" + drift + "초**");
    }

    private static int waitStableOrderCount(String url) throws Exception {
        int last = -1;
        int stable = 0;
        while (stable < 3) {
            Thread.sleep(1_000);
            int now;
            try (Connection c = DriverManager.getConnection(url); Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM ORDER_TB")) {
                rs.next();
                now = rs.getInt(1);
            }
            stable = now == last ? stable + 1 : 0;
            last = now;
        }
        return last;
    }

    private static List<Long> latencies(List<Sample> all, int status) {
        List<Long> l = new ArrayList<>(all.stream().filter(x -> x.status() == status).map(Sample::latencyMs).toList());
        Collections.sort(l);
        return l;
    }

    private static Long pct(List<Long> sorted, int p) {
        if (sorted.isEmpty()) return null;
        int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
    }

    private static String fmt(Long ms) {
        if (ms == null) return "-";
        return ms >= 1_000 ? String.format("%.1f초", ms / 1000.0) : ms + "ms";
    }
}
