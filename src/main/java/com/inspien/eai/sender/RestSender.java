package com.inspien.eai.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inspien.eai.core.ErrorCode;
import com.inspien.eai.flow.OrderResult;
import com.inspien.eai.flow.OrderSyncFlow;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sender Channel (REST, SYNC). 웹 프레임워크 없이 JDK HttpServer 사용.
 * <ul>
 *   <li>POST /orders — 주문 XML 수신 → 처리 결과 JSON 동기 반환</li>
 *   <li>POST /batch/shipments/run — 운송 배치 즉시 실행(운영자 수동 실행·시연용). 실행 중이면 409</li>
 *   <li>GET  /health — 생존 확인 + 처리 중/거절 건수</li>
 * </ul>
 *
 * <p><b>과부하 차단(admission control)</b>: 주문 1건은 외부 DB·FTP 를 기다리느라 수 초가 걸린다.
 * 요청을 무한정 대기열에 쌓으면 몰리는 순간 모든 요청의 응답이 함께 느려지고(대기 시간이 처리 시간을 넘어 타임아웃),
 * 결국 이미 처리한 주문의 응답까지 잃는다. 그래서 동시 처리 수에 상한을 두고, 넘치는 요청은 <b>처리 시작 전에</b>
 * 즉시 503 OVERLOADED(재시도 가능, Retry-After)로 돌려준다 — 받은 요청은 제시간에, 못 받는 요청은 빨리.
 */
public final class RestSender {

    private static final int MAX_BODY_BYTES = 1024 * 1024; // 1MB — 비정상 대용량 요청 차단
    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * @param maxConcurrentOrders 동시에 처리할 주문 수 (DB·FTP 커넥션 수와 같은 의미). 0 이하 = 상한 없음(비교 측정용)
     * @param workerThreads       HTTP 처리 스레드 — 상한보다 넉넉히 둬서 초과 요청도 즉시 503 을 받게 한다
     * @param queueCapacity       스레드가 모두 바쁠 때의 대기열. 0 이하 = 무제한(비교 측정용)
     */
    public record Limits(int maxConcurrentOrders, int workerThreads, int queueCapacity) {
        public static Limits defaults() {
            return new Limits(8, 32, 200);
        }

        /** 과부하 차단 도입 전 구성(고정 8스레드 + 무제한 대기열) — 부하 측정의 비교 기준 */
        public static Limits legacyUnbounded() {
            return new Limits(0, 8, 0);
        }
    }

    private final HttpServer server;
    private final ExecutorService workers;
    private final Semaphore orderPermits;
    private final int maxConcurrent;
    private final AtomicLong rejected = new AtomicLong();

    public RestSender(int port, OrderSyncFlow orderFlow, BatchScheduler shipmentBatch) throws IOException {
        this(port, orderFlow, shipmentBatch, Limits.defaults());
    }

    public RestSender(int port, OrderSyncFlow orderFlow, BatchScheduler shipmentBatch, Limits limits) throws IOException {
        this.maxConcurrent = limits.maxConcurrentOrders();
        this.orderPermits = maxConcurrent > 0 ? new Semaphore(maxConcurrent) : null;
        this.workers = new ThreadPoolExecutor(limits.workerThreads(), limits.workerThreads(), 0L, TimeUnit.MILLISECONDS,
                limits.queueCapacity() > 0 ? new ArrayBlockingQueue<>(limits.queueCapacity()) : new LinkedBlockingQueue<>(),
                namedThreads("http-worker"),
                new ThreadPoolExecutor.CallerRunsPolicy()); // 대기열까지 차면 수신 스레드가 직접 처리 → 자연스러운 역압
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(workers);
        server.createContext("/orders", ex -> handleOrders(ex, orderFlow));
        server.createContext("/batch/shipments/run", ex -> handleBatchRun(ex, shipmentBatch));
        server.createContext("/health", ex -> {
            ObjectNode h = JSON.createObjectNode().put("status", "UP");
            if (orderPermits != null) {
                h.put("ordersInFlight", maxConcurrent - orderPermits.availablePermits());
                h.put("maxConcurrentOrders", maxConcurrent);
            }
            h.put("ordersRejected", rejected.get());
            respond(ex, 200, h);
        });
    }

    public void start() { server.start(); }

    public void stop(int graceSeconds) {
        server.stop(graceSeconds);
        workers.shutdown();
    }

    public int port() { return server.getAddress().getPort(); }

    private void handleOrders(HttpExchange ex, OrderSyncFlow flow) throws IOException {
        try {
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                respond(ex, 405, error(ErrorCode.METHOD_NOT_ALLOWED, "POST 만 허용됩니다"));
                return;
            }
            byte[] body = readLimited(ex.getRequestBody());
            if (body == null) {
                respond(ex, 413, error(ErrorCode.PAYLOAD_TOO_LARGE, "최대 " + MAX_BODY_BYTES + " 바이트"));
                return;
            }
            if (orderPermits != null && !orderPermits.tryAcquire()) {
                rejected.incrementAndGet();
                ex.getResponseHeaders().add("Retry-After", "1");
                respond(ex, 503, error(ErrorCode.OVERLOADED, "동시 처리 상한 " + maxConcurrent + "건 초과 — 처리 시작 전 거절, 재전송 안전"));
                return;
            }
            OrderResult r;
            try {
                r = flow.handle(body, ex.getRequestHeaders().getFirst("Content-Type"));
            } finally {
                if (orderPermits != null) orderPermits.release();
            }
            ex.getResponseHeaders().add("X-Message-Id", r.msgId());
            int status = r.errorCode() == null ? 200 : ErrorCode.valueOf(r.errorCode()).httpStatus();
            respond(ex, status, toJson(r));
        } catch (Exception e) {
            respond(ex, 500, error(ErrorCode.INTERNAL, e.getClass().getSimpleName()));
        } finally {
            ex.close();
        }
    }

    private void handleBatchRun(HttpExchange ex, BatchScheduler batch) throws IOException {
        try {
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                respond(ex, 405, error(ErrorCode.METHOD_NOT_ALLOWED, "POST 만 허용됩니다"));
                return;
            }
            var result = batch.runNow("MANUAL");
            if (result.isEmpty()) {
                respond(ex, 409, JSON.createObjectNode().put("resultCode", "SKIPPED").put("message", "배치가 이미 실행 중입니다"));
                return;
            }
            respond(ex, result.get().errorCode() == null ? 200 : 500, JSON.valueToTree(result.get()));
        } finally {
            ex.close();
        }
    }

    static ObjectNode toJson(OrderResult r) {
        ObjectNode n = JSON.createObjectNode();
        n.put("msgId", r.msgId());
        n.put("interfaceId", r.interfaceId());
        n.put("resultCode", r.resultCode().name());
        n.put("message", r.message());
        n.put("errorCode", r.errorCode());
        if (r.errorCode() != null) n.put("retryable", ErrorCode.valueOf(r.errorCode()).retryable());
        n.put("receivedHeaderCount", r.receivedHeaderCount());
        n.put("receivedItemCount", r.receivedItemCount());
        n.put("processedCount", r.processedCount());
        n.put("excludedCount", r.excludedCount());
        if (r.receiptFile() != null && r.errorCode() == null) n.put("receiptFile", r.receiptFile());
        ArrayNode ids = n.putArray("orderIds");
        r.orderIds().forEach(ids::add);
        ArrayNode ex = n.putArray("excluded");
        r.excluded().forEach(x -> {
            ObjectNode e = ex.addObject();
            e.put("level", x.level().name());
            e.put("userId", x.userId());
            if (x.itemId() != null) e.put("itemId", x.itemId());
            e.put("reason", x.reason().name());
            e.put("detail", x.detail());
        });
        return n;
    }

    private static ObjectNode error(ErrorCode code, String message) {
        return JSON.createObjectNode()
                .put("resultCode", "FAIL")
                .put("errorCode", code.name())
                .put("retryable", code.retryable())
                .put("message", code.description() + " — " + message);
    }

    public long rejectedCount() { return rejected.get(); }

    private static ThreadFactory namedThreads(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    private static byte[] readLimited(InputStream in) throws IOException {
        byte[] data = in.readNBytes(MAX_BODY_BYTES + 1);
        return data.length > MAX_BODY_BYTES ? null : data;
    }

    private static void respond(HttpExchange ex, int status, ObjectNode body) throws IOException {
        byte[] bytes = JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(body);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
