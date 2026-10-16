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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Sender Channel (REST, SYNC). 웹 프레임워크 없이 JDK HttpServer 사용.
 * <ul>
 *   <li>POST /orders — 주문 XML 수신 → 처리 결과 JSON 동기 반환</li>
 *   <li>GET  /health — 생존 확인</li>
 * </ul>
 */
public final class RestSender {

    private static final int MAX_BODY_BYTES = 1024 * 1024; // 1MB — 비정상 대용량 요청 차단
    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpServer server;
    private final ExecutorService workers = Executors.newFixedThreadPool(8);

    public RestSender(int port, OrderSyncFlow orderFlow) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(workers);
        server.createContext("/orders", ex -> handleOrders(ex, orderFlow));
        server.createContext("/health", ex -> respond(ex, 200, JSON.createObjectNode().put("status", "UP")));
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
            OrderResult r = flow.handle(body, ex.getRequestHeaders().getFirst("Content-Type"));
            ex.getResponseHeaders().add("X-Message-Id", r.msgId());
            int status = r.errorCode() == null ? 200 : ErrorCode.valueOf(r.errorCode()).httpStatus();
            respond(ex, status, toJson(r));
        } catch (Exception e) {
            respond(ex, 500, error(ErrorCode.INTERNAL, e.getClass().getSimpleName()));
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
