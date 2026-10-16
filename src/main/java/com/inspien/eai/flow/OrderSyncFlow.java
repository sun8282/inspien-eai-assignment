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
import com.inspien.eai.mapping.ReceiptFileMapper;
import com.inspien.eai.model.ExcludedRecord;
import com.inspien.eai.model.OrderRecord;
import com.inspien.eai.model.ReceiptFile;
import com.inspien.eai.monitor.InterfaceLogWriter;
import com.inspien.eai.monitor.OutboundArchive;
import com.inspien.eai.receiver.JdbcOrderReceiver;
import com.inspien.eai.receiver.ReceiptReceiver;
import com.inspien.eai.receiver.ReceiptSession;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * IF_ORD_001/002 — 주문 실시간 연계 (REST Sender → Mapping → JDBC Receiver + FTP Receiver, SYNC).
 *
 * <p>제약(실환경 점검 결과): DB 계정은 DELETE 불가, FTP 는 rename·delete 불가 — <b>두 수신처 모두 한 번 쓰면 되돌릴 수 없다</b>.
 * 되돌릴 수 있는 유일한 수단은 "커밋 전 DB 트랜잭션"이므로, 이것으로 되돌릴 수 없는 FTP 업로드를 감싼다:
 * <pre>
 * DECODE → PARSE → MAP → ID_ALLOC → JDBC_INSERT(미커밋) → RECEIPT_MAP → FTP_UPLOAD(+크기검증) → ARCHIVE → COMMIT
 *
 *  실패 지점        처리                                              결과
 *  ~ FTP_UPLOAD    DB 롤백 (파일 잔존 가능성 있으면 수동조치 로그)         DB 미적재, 재전송 안전
 *  COMMIT          파일은 이미 공개됨 → 되돌리지 않고 DB 를 파일에 맞춤(roll-forward):
 *                  새 연결로 해당 ID 존재 확인 → 이미 있음(커밋 성공·응답 유실) / 없음 → 동일 ID 재적재·커밋   일치, 성공
 *  roll-forward 실패  RECOVERY_FAILED + 수동조치 로그                     파일만 존재, 재전송 금지
 * </pre>
 * 불변식: <b>파일이 공개됐다면 DB 에도 반드시 같은 주문이 있다</b>(또는 수동조치 로그가 있다).
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
    private final ReceiptFileMapper receiptMapper;
    private final ReceiptReceiver receiptReceiver;
    private final OutboundArchive archive;
    private final InterfaceLogWriter log;
    private final OrderXmlParser parser = new OrderXmlParser();
    private final OrderFlattener flattener = new OrderFlattener();

    public OrderSyncFlow(String applicantKey, ConnectionFactory orderDb, IdAllocator orderIds,
                         JdbcOrderReceiver jdbcReceiver, ReceiptFileMapper receiptMapper,
                         ReceiptReceiver receiptReceiver, OutboundArchive archive, InterfaceLogWriter log) {
        this.applicantKey = applicantKey;
        this.orderDb = orderDb;
        this.orderIds = orderIds;
        this.jdbcReceiver = jdbcReceiver;
        this.receiptMapper = receiptMapper;
        this.receiptReceiver = receiptReceiver;
        this.archive = archive;
        this.log = log;
    }

    public OrderResult handle(byte[] body, String contentType) {
        MessageContext ctx = new MessageContext(INTERFACE_ID);
        int headerCount = 0;
        int itemCount = 0;
        List<ExcludedRecord> excluded = List.of();
        String charset = null;
        Outcome outcome = new Outcome();
        OrderResult result;
        try {
            PayloadDecoder.Decoded decoded = ctx.step("DECODE", () -> PayloadDecoder.decode(body, contentType));
            charset = decoded.describe();
            OrderXmlParser.Parsed parsed = ctx.step("PARSE", () -> parser.parse(decoded.text()));
            headerCount = parsed.headers().size();
            itemCount = parsed.items().size();

            OrderFlattener.Result mapped = ctx.step("MAP", () -> flattener.flatten(parsed.headers(), parsed.items()));
            excluded = mapped.excluded();
            if (mapped.records().isEmpty()) {
                throw new EaiException(ErrorCode.NO_VALID_RECORD, "조인·검증을 통과한 주문이 없습니다 (제외 " + excluded.size() + "건)");
            }

            List<String> ids = persist(ctx, mapped.records(), outcome);
            ResultCode code = excluded.isEmpty() ? ResultCode.SUCCESS : ResultCode.PARTIAL_SUCCESS;
            result = new OrderResult(ctx.msgId(), INTERFACE_ID, code,
                    "주문 " + ids.size() + "건 적재·영수증 전송" + (excluded.isEmpty() ? "" : ", " + excluded.size() + "건 제외"),
                    headerCount, itemCount, ids, excluded, null, outcome.receiptFile);
        } catch (EaiException e) {
            result = fail(ctx, e.code(), e.getMessage(), headerCount, itemCount, excluded);
        } catch (Exception e) {
            result = fail(ctx, ErrorCode.INTERNAL, e.getClass().getSimpleName() + ": " + e.getMessage(), headerCount, itemCount, excluded);
        }
        writeLog(ctx, result, charset, outcome);
        return result;
    }

    /** 처리 중 생긴 부가 정보(로그용). */
    private static final class Outcome {
        String receiptFile;
        String manualAction;
    }

    private List<String> persist(MessageContext ctx, List<OrderRecord> records, Outcome outcome) throws Exception {
        try (Connection c = ctx.step("DB_CONNECT", orderDb::open)) {
            List<OrderRecord> withIds;
            ReceiptFile file;
            try {
                withIds = insertWithIds(ctx, c, records);
                try (ReceiptSession ftp = ctx.step("FTP_CONNECT", this::openFtp)) {
                    file = ctx.step("RECEIPT_MAP", () -> uniqueName(ftp, receiptMapper.map(withIds)));
                    outcome.receiptFile = file.name();
                    ctx.step("FTP_UPLOAD", () -> {
                        upload(ftp, file, outcome);
                        return null;
                    });
                }
                Path archived = archive.save(file);
                ctx.note("ARCHIVE", archived == null ? "FAIL" : "OK", archived == null ? null : archived.toString());
            } catch (Exception e) {
                safeRollback(c); // 파일 공개 전 실패 → DB 는 아무것도 남기지 않는다
                throw e;
            }

            List<String> ids = withIds.stream().map(OrderRecord::orderId).toList();
            try {
                ctx.step("COMMIT", () -> {
                    c.commit();
                    return null;
                });
            } catch (Exception commitError) {
                // 원 트랜잭션이 살아 있으면 잠금을 쥔 채라 재적재가 막힌다 → 먼저 정리.
                // (커밋이 실제로 성공했던 경우 rollback 은 아무 영향 없음)
                safeRollback(c);
                rollForward(ctx, withIds, file, outcome, commitError);
            }
            return ids;
        } catch (SQLException e) {
            throw SqlErrors.translate(e, "ORDER_TB 연결");
        }
    }

    /** 같은 이름이 이미 있으면(재기동 직후 같은 초 등) 다음 초 이름으로 — 쓰기 전용 서버라 덮어쓰기 금지. */
    private ReceiptFile uniqueName(ReceiptSession ftp, ReceiptFile file) throws IOException {
        ReceiptFile f = file;
        for (int i = 0; i < 5 && ftp.exists(f.name()); i++) f = receiptMapper.rename(f);
        if (ftp.exists(f.name())) throw new EaiException(ErrorCode.FTP_TRANSFER, "고유 파일명 확보 실패: " + f.name());
        return f;
    }

    private void upload(ReceiptSession ftp, ReceiptFile file, Outcome outcome) {
        try {
            ftp.upload(file);
        } catch (ReceiptSession.UploadException e) {
            if (e.possiblyWritten()) {
                outcome.manualAction = "FTP 에 불완전 파일 잔존 가능: " + file.name()
                        + " (삭제 권한 없음 — 수신측에 해당 파일 무시 요청, DB 는 롤백됨)";
            }
            throw new EaiException(ErrorCode.FTP_TRANSFER, e.getMessage(), e);
        }
    }

    /**
     * 파일은 공개됐는데 커밋이 실패(또는 결과 불명)한 경우. 파일을 지울 수 없으니 DB 를 파일에 맞춘다.
     * 새 연결에서 해당 ID 가 이미 있으면 커밋이 실제로 성공했던 것(응답만 유실), 없으면 같은 ID 로 재적재한다.
     */
    private void rollForward(MessageContext ctx, List<OrderRecord> withIds, ReceiptFile file, Outcome outcome, Exception commitError) {
        List<String> ids = withIds.stream().map(OrderRecord::orderId).toList();
        String range = ids.get(0) + "~" + ids.get(ids.size() - 1);
        try {
            String how = ctx.step("ROLL_FORWARD", () -> {
                try (Connection c2 = orderDb.open()) {
                    int existing = jdbcReceiver.countExisting(c2, applicantKey, ids);
                    if (existing == ids.size()) return "커밋 성공 확인(응답 유실)";
                    if (existing != 0) throw new IllegalStateException("일부만 존재(" + existing + "/" + ids.size() + ")");
                    jdbcReceiver.insert(c2, withIds);
                    c2.commit();
                    return "동일 ID 재적재";
                }
            });
            ctx.note("RECOVERED", "OK", how + " — 원인: " + commitError.getMessage());
        } catch (Exception e) {
            outcome.manualAction = "FTP 파일 " + file.name() + " 공개됨, ORDER_TB 미적재 — APPLICANT_KEY=" + applicantKey
                    + ", ORDER_ID " + range + " 를 파일 내용대로 수동 적재 필요";
            throw new EaiException(ErrorCode.RECOVERY_FAILED,
                    "영수증 공개 후 DB 반영 실패 — 재전송 금지, 운영자 확인 필요 (" + e.getMessage() + ")", e);
        }
    }

    private ReceiptSession openFtp() {
        try {
            return receiptReceiver.open();
        } catch (IOException e) {
            throw new EaiException(ErrorCode.FTP_CONNECTION, e.getMessage(), e);
        }
    }

    /** ID 채번 + INSERT(미커밋). 외부 충돌로 중복키가 나면 재동기화 후 재시도. */
    private List<OrderRecord> insertWithIds(MessageContext ctx, Connection c, List<OrderRecord> records) throws Exception {
        for (int attempt = 1; ; attempt++) {
            List<String> ids = ctx.step("ID_ALLOC", () -> allocate(c, records.size()));
            List<OrderRecord> withIds = new ArrayList<>(records.size());
            for (int i = 0; i < records.size(); i++) {
                withIds.add(records.get(i).withOrderId(ids.get(i), applicantKey));
            }
            try {
                ctx.step("JDBC_INSERT", () -> jdbcReceiver.insert(c, withIds));
                return withIds;
            } catch (SQLException e) {
                c.rollback();
                if (SqlErrors.isDuplicateKey(e) && attempt < MAX_ID_RETRY) {
                    ctx.note("ID_RESYNC", "RETRY", "중복키 → 채번 재동기화 (" + attempt + "/" + MAX_ID_RETRY + ")");
                    orderIds.resync();
                    continue;
                }
                throw SqlErrors.translate(e, "ORDER_TB 적재");
            }
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
                headerCount, itemCount, List.of(), excluded, code.name(), null);
    }

    private void writeLog(MessageContext ctx, OrderResult r, String charset, Outcome outcome) {
        ObjectNode rec = log.newRecord();
        rec.put("timestamp", ctx.startedAt().atZone(KST).toOffsetDateTime().toString());
        rec.put("msgId", r.msgId());
        rec.put("interfaceId", r.interfaceId());
        rec.put("type", "REALTIME");
        rec.put("sender", "REST");
        rec.put("receivers", "JDBC:ORDER_TB," + receiptReceiver.target());
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
        rec.put("receiptFile", outcome.receiptFile);
        rec.put("errorCode", r.errorCode());
        if (r.errorCode() != null) rec.put("errorMessage", r.message());
        if (outcome.manualAction != null) rec.put("manualAction", outcome.manualAction);
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
