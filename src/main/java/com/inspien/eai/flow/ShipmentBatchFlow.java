package com.inspien.eai.flow;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inspien.eai.core.EaiException;
import com.inspien.eai.core.ErrorCode;
import com.inspien.eai.core.MessageContext;
import com.inspien.eai.flow.BatchResult.Failure;
import com.inspien.eai.flow.BatchResult.ResultCode;
import com.inspien.eai.id.IdAllocator;
import com.inspien.eai.id.SequentialIdFormat.IdExhaustedException;
import com.inspien.eai.infra.ConnectionFactory;
import com.inspien.eai.infra.SqlErrors;
import com.inspien.eai.mapping.ShipmentMapper;
import com.inspien.eai.model.PendingOrder;
import com.inspien.eai.model.ShipmentRecord;
import com.inspien.eai.monitor.InterfaceLogWriter;
import com.inspien.eai.receiver.JdbcShipmentReceiver;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * IF_SHP_001 — 운송사 전송 배치 (Scheduler + JDBC Poll → Mapping → JDBC Receiver).
 *
 * <pre>
 * 반복(청크): FETCH(STATUS='N', 키셋 페이징) → 검증 → 기적재 확인(멱등) → CLAIM(N→Y) → SHIPMENT_ID 채번 → INSERT → COMMIT
 * </pre>
 * 운영 원칙(인사연동 30만 건 장애 교훈):
 * <ul>
 *   <li><b>메모리는 데이터 양과 무관</b> — 청크 단위 조회·커밋. 1회 실행 상한(행 수·시간)을 넘으면 다음 주기로 넘긴다.</li>
 *   <li><b>실패 격리</b> — 청크가 실패하면 그 청크만 건별로 재처리해 원인 건만 남긴다. 나머지는 계속 진행.</li>
 *   <li><b>재개 가능</b> — 상태는 DB(STATUS)가 정본. 중단·재기동돼도 다음 실행이 남은 'N' 부터 이어서 처리한다.</li>
 *   <li><b>독성 데이터 격리</b> — 같은 주문이 {@value #QUARANTINE_AFTER}회 연속 실패하면 격리해 매 주기 반복 실패를 멈추고 수동 조치로 넘긴다.</li>
 * </ul>
 * 적재와 상태 갱신은 같은 트랜잭션 → "적재 성공한 건만 Y" 가 원자적으로 보장된다.
 */
public final class ShipmentBatchFlow {

    public static final String INTERFACE_ID = "IF_SHP_001";
    static final int QUARANTINE_AFTER = 3;
    private static final int MAX_ID_RETRY = 3;
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    public record Settings(int chunkSize, int maxRowsPerRun, Duration timeBudget) {
        public static Settings defaults() {
            return new Settings(100, 5_000, Duration.ofMinutes(4)); // 주기(5분)보다 짧게
        }
    }

    private final String applicantKey;
    private final ConnectionFactory db;
    private final IdAllocator shipmentIds;
    private final JdbcShipmentReceiver rx;
    private final ShipmentMapper mapper = new ShipmentMapper();
    private final InterfaceLogWriter log;
    private final Settings settings;
    private final Map<String, Integer> failCounts = new ConcurrentHashMap<>();
    private volatile boolean stopRequested;

    public ShipmentBatchFlow(String applicantKey, ConnectionFactory db, IdAllocator shipmentIds,
                             JdbcShipmentReceiver rx, InterfaceLogWriter log, Settings settings) {
        this.applicantKey = applicantKey;
        this.db = db;
        this.shipmentIds = shipmentIds;
        this.rx = rx;
        this.log = log;
        this.settings = settings;
    }

    /** 협조적 중지 — 진행 중 청크는 끝까지 커밋하고, 다음 청크 경계에서 멈춘다. */
    public void requestStop() {
        stopRequested = true;
    }

    /** 실행 단위 집계 */
    private static final class Run {
        int fetched, shipped, repaired, skipped, failed, quarantinedSkipped, chunks;
        String firstShipmentId, lastShipmentId, stopReason;
        final List<Failure> failures = new ArrayList<>();
    }

    public BatchResult run(String trigger) {
        MessageContext ctx = new MessageContext(INTERFACE_ID);
        Run run = new Run();
        String errorCode = null;
        String message;
        try (Connection c = ctx.step("DB_CONNECT", db::open)) {
            String after = null;
            while (true) {
                if (stopRequested) { run.stopReason = "STOP_REQUESTED"; break; }
                if (run.fetched >= settings.maxRowsPerRun()) { run.stopReason = "ROW_LIMIT"; break; }
                if (ctx.elapsedMs() >= settings.timeBudget().toMillis()) { run.stopReason = "TIME_BUDGET"; break; }

                int limit = Math.min(settings.chunkSize(), settings.maxRowsPerRun() - run.fetched);
                String from = after;
                List<PendingOrder> page = rx.fetchPending(c, applicantKey, from, limit);
                if (page.isEmpty()) break;
                after = page.get(page.size() - 1).orderId();
                run.fetched += page.size();
                run.chunks++;
                processPage(c, page, run);
            }
            message = summary(run);
        } catch (EaiException e) {
            errorCode = e.code().name();
            message = e.getMessage() + " — 처리분은 커밋됨, 남은 건은 다음 주기에 이어서 처리";
        } catch (SQLException e) {
            EaiException t = SqlErrors.translate(e, "배치");
            errorCode = t.code().name();
            message = t.getMessage() + " — 처리분은 커밋됨, 남은 건은 다음 주기에 이어서 처리";
        } catch (Exception e) {
            errorCode = ErrorCode.INTERNAL.name();
            message = e.getClass().getSimpleName() + ": " + e.getMessage();
        }

        ResultCode code = errorCode != null ? ResultCode.FAIL
                : run.fetched == 0 ? ResultCode.NO_DATA
                : run.failed > 0 || run.quarantinedSkipped > 0 ? ResultCode.PARTIAL_SUCCESS
                : ResultCode.SUCCESS;
        BatchResult result = new BatchResult(ctx.msgId(), INTERFACE_ID, trigger, code, message,
                run.fetched, run.shipped, run.repaired, run.skipped, run.failed, run.quarantinedSkipped, run.chunks,
                run.firstShipmentId == null ? null : run.firstShipmentId + "~" + run.lastShipmentId,
                run.stopReason, List.copyOf(run.failures), errorCode, ctx.elapsedMs());
        writeLog(ctx, result);
        return result;
    }

    /** 청크 하나: 한 트랜잭션으로 시도 → 실패 시 건별 트랜잭션으로 재처리(실패 격리). */
    private void processPage(Connection c, List<PendingOrder> page, Run run) throws SQLException {
        List<PendingOrder> work = new ArrayList<>(page.size());
        for (PendingOrder o : page) {
            int fails = failCounts.getOrDefault(o.orderId(), 0);
            if (fails >= QUARANTINE_AFTER) {
                run.quarantinedSkipped++;
                continue;
            }
            String invalid = mapper.validate(o);
            if (invalid != null) {
                fail(run, o, "INVALID_FIELD", invalid);
                continue;
            }
            work.add(o);
        }
        if (work.isEmpty()) return;

        try {
            applyInTransaction(c, work, run);
        } catch (Exception chunkError) {
            safeRollback(c);
            if (isFatal(chunkError)) throw asSql(chunkError);
            for (PendingOrder o : work) { // 원인 건만 남기고 나머지는 처리
                try {
                    applyInTransaction(c, List.of(o), run);
                } catch (Exception e) {
                    safeRollback(c);
                    if (isFatal(e)) throw asSql(e);
                    EaiException t = e instanceof SQLException se ? SqlErrors.translate(se, "운송 적재") :
                            e instanceof EaiException ee ? ee : new EaiException(ErrorCode.INTERNAL, e.getMessage(), e);
                    fail(run, o, t.code().name(), t.getMessage());
                }
            }
        }
    }

    /** 멱등 확인 → 선점(N→Y) → 채번 → 적재 → 커밋. 외부 충돌(중복키)은 채번 재동기화 후 재시도. */
    private void applyInTransaction(Connection c, List<PendingOrder> orders, Run run) throws SQLException {
        for (int attempt = 1; ; attempt++) {
            List<String> ids = orders.stream().map(PendingOrder::orderId).toList();
            Set<String> already = rx.existingShipments(c, applicantKey, ids);
            int[] claimed = rx.claim(c, applicantKey, ids);

            List<PendingOrder> toShip = new ArrayList<>();
            int repaired = 0;
            int skipped = 0;
            for (int i = 0; i < orders.size(); i++) {
                boolean mine = claimed[i] > 0 || claimed[i] == Statement.SUCCESS_NO_INFO;
                if (!mine) { skipped++; continue; }                 // 다른 실행이 먼저 처리
                if (already.contains(ids.get(i))) { repaired++; continue; } // 적재돼 있는데 N 으로 남았던 건 → 상태만 보정
                toShip.add(orders.get(i));
            }

            List<String> newIds;
            try {
                newIds = toShip.isEmpty() ? List.of() : shipmentIds.allocate(c, toShip.size());
            } catch (IdExhaustedException e) {
                throw new EaiException(ErrorCode.ID_EXHAUSTED, e.getMessage(), e);
            }
            List<ShipmentRecord> rows = new ArrayList<>(toShip.size());
            for (int i = 0; i < toShip.size(); i++) rows.add(mapper.map(toShip.get(i), newIds.get(i), applicantKey));

            try {
                rx.insert(c, rows);
                c.commit();
            } catch (SQLException e) {
                safeRollback(c);
                if (SqlErrors.isDuplicateKey(e) && attempt < MAX_ID_RETRY) {
                    shipmentIds.resync();
                    continue;
                }
                throw e;
            }

            run.shipped += rows.size();
            run.repaired += repaired;
            run.skipped += skipped;
            if (!newIds.isEmpty()) {
                if (run.firstShipmentId == null) run.firstShipmentId = newIds.get(0);
                run.lastShipmentId = newIds.get(newIds.size() - 1);
            }
            ids.forEach(failCounts::remove);
            return;
        }
    }

    private void fail(Run run, PendingOrder o, String code, String detail) {
        int count = failCounts.merge(o.orderId(), 1, Integer::sum);
        boolean quarantined = count >= QUARANTINE_AFTER;
        run.failed++;
        run.failures.add(new Failure(o.orderId(), code, detail, count, quarantined));
    }

    /** 연결 계열 오류는 건별 재처리해도 소용없다 → 실행 중단, 다음 주기에 재개. */
    private static boolean isFatal(Exception e) {
        return e instanceof SQLException se && SqlErrors.isConnectionError(se)
                || e instanceof EaiException ee && ee.code() == ErrorCode.ID_EXHAUSTED;
    }

    private static SQLException asSql(Exception e) {
        if (e instanceof SQLException se) return se;
        if (e instanceof EaiException ee) throw ee;
        return new SQLException(e);
    }

    private static void safeRollback(Connection c) {
        try {
            c.rollback();
        } catch (SQLException ignored) {
            // 연결 단절 — 미커밋분은 DB 가 정리
        }
    }

    private static String summary(Run run) {
        if (run.fetched == 0) return "전송 대상 없음";
        return "대상 " + run.fetched + "건: 적재 " + run.shipped + ", 상태보정 " + run.repaired
                + ", 타실행처리 " + run.skipped + ", 실패 " + run.failed + ", 격리제외 " + run.quarantinedSkipped
                + (run.stopReason == null ? "" : " (중단: " + run.stopReason + " — 남은 건은 다음 주기)");
    }

    private void writeLog(MessageContext ctx, BatchResult r) {
        ObjectNode rec = log.newRecord();
        rec.put("timestamp", ctx.startedAt().atZone(KST).toOffsetDateTime().toString());
        rec.put("msgId", r.runId());
        rec.put("interfaceId", r.interfaceId());
        rec.put("type", "BATCH");
        rec.put("trigger", r.trigger());
        rec.put("sender", "JDBC:ORDER_TB(STATUS=N)");
        rec.put("receivers", "JDBC:SHIPMENT_TB,JDBC:ORDER_TB(STATUS=Y)");
        rec.put("resultCode", r.resultCode().name());
        rec.put("durationMs", r.durationMs());
        rec.put("fetched", r.fetched());
        rec.put("shipped", r.shipped());
        rec.put("repaired", r.repaired());
        rec.put("skipped", r.skipped());
        rec.put("failed", r.failed());
        rec.put("quarantinedSkipped", r.quarantinedSkipped());
        rec.put("chunks", r.chunks());
        rec.put("shipmentIdRange", r.shipmentIdRange());
        rec.put("stopReason", r.stopReason());
        rec.put("errorCode", r.errorCode());
        if (r.errorCode() != null) rec.put("errorMessage", r.message());
        ArrayNode failures = rec.putArray("failures");
        r.failures().forEach(f -> {
            ObjectNode n = failures.addObject();
            n.put("orderId", f.orderId());
            n.put("errorCode", f.errorCode());
            n.put("detail", f.detail());
            n.put("failCount", f.failCount());
            if (f.quarantined()) n.put("manualAction", "격리됨 — ORDER_TB 원천 데이터 확인·수정 후 재기동 시 재시도");
        });
        log.write(rec);
    }
}
