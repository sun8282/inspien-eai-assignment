package com.inspien.eai.flow;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inspien.eai.core.EaiException;
import com.inspien.eai.core.ErrorCode;
import com.inspien.eai.core.MessageContext;
import com.inspien.eai.flow.OrderResult.ResultCode;
import com.inspien.eai.id.IdAllocator;
import com.inspien.eai.id.SequentialIdFormat.IdExhaustedException;
import com.inspien.eai.infra.ConnectionFactory;
import com.inspien.eai.infra.SqlErrors;
import com.inspien.eai.mapping.OrderFlattener;
import com.inspien.eai.mapping.OrderXmlParser;
import com.inspien.eai.mapping.PayloadDecoder;
import com.inspien.eai.model.ExcludedRecord;
import com.inspien.eai.model.OrderRecord;
import com.inspien.eai.monitor.InterfaceLogWriter;
import com.inspien.eai.receiver.JdbcOrderReceiver;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * IF_ORD_001 — 주문 실시간 연계 (REST Sender → Mapping → JDBC Receiver [→ FTP Receiver, 1-b]).
 *
 * <pre>
 * DECODE → PARSE → MAP(조인·검증) → ID_ALLOC → JDBC_INSERT(미커밋) → [FTP_UPLOAD] → COMMIT
 * </pre>
 * 어느 단계에서 실패하든 커밋 전이면 DB 는 롤백되어 "아무것도 안 쓴" 상태로 남는다.
 * 중복키(외부 충돌)는 채번 재동기화 후 최대 {@value #MAX_ID_RETRY}회 재시도한다.
 */
public final class OrderSyncFlow {

    public static final String INTERFACE_ID = "IF_ORD_001";
    private static final int MAX_ID_RETRY = 3;
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    private final String applicantKey;
    private final ConnectionFactory orderDb;
    private final IdAllocator orderIds;
    private final JdbcOrderReceiver jdbcReceiver;
    private final InterfaceLogWriter log;
    private final OrderXmlParser parser = new OrderXmlParser();
    private final OrderFlattener flattener = new OrderFlattener();

    public OrderSyncFlow(String applicantKey, ConnectionFactory orderDb, IdAllocator orderIds,
                         JdbcOrderReceiver jdbcReceiver, InterfaceLogWriter log) {
        this.applicantKey = applicantKey;
        this.orderDb = orderDb;
        this.orderIds = orderIds;
        this.jdbcReceiver = jdbcReceiver;
        this.log = log;
    }

    public OrderResult handle(byte[] body, String contentType) {
        MessageContext ctx = new MessageContext(INTERFACE_ID);
        int headerCount = 0;
        int itemCount = 0;
        List<ExcludedRecord> excluded = List.of();
        String charset = null;
        OrderResult result;
        try {
            PayloadDecoder.Decoded decoded = ctx.step("DECODE", () -> PayloadDecoder.decode(body, contentType));
            charset = decoded.charset().name() + " (" + decoded.source() + ")";
            OrderXmlParser.Parsed parsed = ctx.step("PARSE", () -> parser.parse(decoded.text()));
            headerCount = parsed.headers().size();
            itemCount = parsed.items().size();

            OrderFlattener.Result mapped = ctx.step("MAP", () -> flattener.flatten(parsed.headers(), parsed.items()));
            excluded = mapped.excluded();
            if (mapped.records().isEmpty()) {
                throw new EaiException(ErrorCode.NO_VALID_RECORD, "조인·검증을 통과한 주문이 없습니다 (제외 " + excluded.size() + "건)");
            }

            List<String> ids = persist(ctx, mapped.records());
            ResultCode code = excluded.isEmpty() ? ResultCode.SUCCESS : ResultCode.PARTIAL_SUCCESS;
            result = new OrderResult(ctx.msgId(), INTERFACE_ID, code,
                    "주문 " + ids.size() + "건 적재" + (excluded.isEmpty() ? "" : ", " + excluded.size() + "건 제외"),
                    headerCount, itemCount, ids, excluded, null);
        } catch (EaiException e) {
            result = fail(ctx, e.code(), e.getMessage(), headerCount, itemCount, excluded);
        } catch (Exception e) {
            result = fail(ctx, ErrorCode.INTERNAL, e.getClass().getSimpleName() + ": " + e.getMessage(), headerCount, itemCount, excluded);
        }
        writeLog(ctx, result, charset);
        return result;
    }

    private List<String> persist(MessageContext ctx, List<OrderRecord> records) throws Exception {
        try (Connection c = ctx.step("DB_CONNECT", orderDb::open)) {
            try {
                for (int attempt = 1; ; attempt++) {
                    List<String> ids = ctx.step("ID_ALLOC", () -> allocate(c, records.size()));
                    List<OrderRecord> withIds = new ArrayList<>(records.size());
                    for (int i = 0; i < records.size(); i++) {
                        withIds.add(records.get(i).withOrderId(ids.get(i), applicantKey));
                    }
                    try {
                        ctx.step("JDBC_INSERT", () -> jdbcReceiver.insert(c, withIds));
                    } catch (SQLException e) {
                        c.rollback();
                        if (SqlErrors.isDuplicateKey(e) && attempt < MAX_ID_RETRY) {
                            ctx.note("ID_RESYNC", "RETRY", "중복키 → 채번 재동기화 (" + attempt + "/" + MAX_ID_RETRY + ")");
                            orderIds.resync();
                            continue;
                        }
                        throw SqlErrors.translate(e, "ORDER_TB 적재");
                    }
                    // 1-b: FTP 영수증 업로드가 이 자리에 들어간다 (실패 시 롤백)
                    ctx.step("COMMIT", () -> {
                        c.commit();
                        return null;
                    });
                    return ids;
                }
            } catch (Exception e) {
                safeRollback(c);
                throw e;
            }
        } catch (SQLException e) {
            throw SqlErrors.translate(e, "ORDER_TB 연결/커밋");
        }
    }

    private List<String> allocate(Connection c, int count) throws SQLException {
        try {
            return orderIds.allocate(c, count);
        } catch (IdExhaustedException e) {
            throw new EaiException(ErrorCode.ID_EXHAUSTED, e.getMessage(), e);
        }
    }

    private static void safeRollback(Connection c) {
        try {
            c.rollback();
        } catch (SQLException ignored) {
            // 연결이 이미 끊긴 경우 — 미커밋 트랜잭션은 DB 가 정리한다
        }
    }

    private static OrderResult fail(MessageContext ctx, ErrorCode code, String message,
                                    int headerCount, int itemCount, List<ExcludedRecord> excluded) {
        return new OrderResult(ctx.msgId(), INTERFACE_ID, ResultCode.FAIL, message,
                headerCount, itemCount, List.of(), excluded, code.name());
    }

    private void writeLog(MessageContext ctx, OrderResult r, String charset) {
        ObjectNode rec = log.newRecord();
        rec.put("timestamp", ctx.startedAt().atZone(KST).toOffsetDateTime().toString());
        rec.put("msgId", r.msgId());
        rec.put("interfaceId", r.interfaceId());
        rec.put("type", "REALTIME");
        rec.put("sender", "REST");
        rec.put("receivers", "JDBC:ORDER_TB");
        rec.put("resultCode", r.resultCode().name());
        rec.put("durationMs", ctx.elapsedMs());
        rec.put("charset", charset);
        rec.put("receivedHeaders", r.receivedHeaderCount());
        rec.put("receivedItems", r.receivedItemCount());
        rec.put("processed", r.processedCount());
        rec.put("excluded", r.excludedCount());
        if (!r.orderIds().isEmpty()) {
            rec.put("orderIdRange", r.orderIds().get(0) + "~" + r.orderIds().get(r.orderIds().size() - 1));
        }
        Map<String, Integer> byReason = new TreeMap<>();
        r.excluded().forEach(x -> byReason.merge(x.reason().name(), 1, Integer::sum));
        ObjectNode reasons = rec.putObject("excludedByReason");
        byReason.forEach(reasons::put);
        rec.put("errorCode", r.errorCode());
        if (r.errorCode() != null) rec.put("errorMessage", r.message());
        ArrayNode steps = rec.putArray("steps");
        ctx.steps().forEach(s -> {
            ObjectNode n = steps.addObject();
            n.put("step", s.name());
            n.put("status", s.status());
            n.put("ms", s.durationMs());
            if (s.detail() != null) n.put("detail", s.detail());
        });
        log.write(rec);
    }
}
