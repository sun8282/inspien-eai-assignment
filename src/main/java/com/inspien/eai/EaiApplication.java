package com.inspien.eai;

import com.inspien.eai.config.Env;
import com.inspien.eai.config.ProvisionInfo;
import com.inspien.eai.flow.OrderSyncFlow;
import com.inspien.eai.flow.ShipmentBatchFlow;
import com.inspien.eai.id.IdAllocator;
import com.inspien.eai.infra.ConnectionFactory;
import com.inspien.eai.infra.Preflight;
import com.inspien.eai.monitor.InterfaceLogWriter;
import com.inspien.eai.monitor.OutboundArchive;
import com.inspien.eai.mapping.ReceiptFileMapper;
import com.inspien.eai.receiver.FtpReceiptReceiver;
import com.inspien.eai.receiver.JdbcOrderReceiver;
import com.inspien.eai.receiver.JdbcShipmentReceiver;
import com.inspien.eai.sender.BatchScheduler;
import com.inspien.eai.sender.RestSender;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;

/**
 * 연계 애플리케이션 진입점 — Sender/Mapper/Receiver 를 조립해 Flow 를 띄운다.
 * 설정 누락·복호화 실패·접속/스키마 설정 오류는 기동 단계에서 즉시 중단(Fail-fast).
 */
public final class EaiApplication {

    public static void main(String[] args) throws Exception {
        Env env = Env.load();
        ProvisionInfo info = ProvisionInfo.load(env);
        int port = Integer.parseInt(env.get("SERVER_PORT") == null ? "8080" : env.get("SERVER_PORT"));
        InterfaceLogWriter log = new InterfaceLogWriter(Path.of(env.get("LOG_DIR") == null ? "logs" : env.get("LOG_DIR")));

        System.out.println("[INFO] 기동 점검 (APPLICANT_KEY=" + info.applicantKey() + ")");
        List<Preflight.Check> checks = Preflight.run(info);
        checks.forEach(System.out::println);
        writeStartupLog(log, checks);
        if (Preflight.hasFatal(checks)) {
            System.err.println("[FATAL] 설정 오류로 기동 중단 — 위 ✗ 항목 확인 (재시도로 해결되지 않는 오류)");
            System.exit(1);
        }

        String orderTable = info.orderDb().table();
        OrderSyncFlow orderFlow = new OrderSyncFlow(
                info.applicantKey(),
                new ConnectionFactory(info.orderDb()),
                new IdAllocator(orderTable, "ORDER_ID", info.applicantKey()),
                new JdbcOrderReceiver(orderTable),
                new ReceiptFileMapper(env.require("APPLICANT_NAME"), receiptCharset(env), Clock.system(ZoneId.of("Asia/Seoul"))),
                new FtpReceiptReceiver(info.ftp()),
                new OutboundArchive(Path.of("archive")),
                log);

        // 시나리오2: 같은 계정이어야 적재+상태갱신을 한 트랜잭션으로 묶을 수 있다(점검 결과 동일). 다르면 기동 중단.
        if (!info.orderDb().sameAccountAs(info.shipmentDb())) {
            throw new IllegalStateException("ORDER_TB 와 SHIPMENT_TB 접속 계정이 다릅니다 — 단일 트랜잭션 전제 위반(분리 계정 모드는 미지원)");
        }
        ShipmentBatchFlow shipmentFlow = new ShipmentBatchFlow(
                info.applicantKey(),
                new ConnectionFactory(info.shipmentDb()),
                new IdAllocator(info.shipmentDb().table(), "SHIPMENT_ID", info.applicantKey()),
                new JdbcShipmentReceiver(orderTable, info.shipmentDb().table()),
                log,
                ShipmentBatchFlow.Settings.defaults());
        Duration interval = Duration.ofSeconds(intEnv(env, "SHIPMENT_INTERVAL_SEC", 300));
        BatchScheduler shipmentBatch = new BatchScheduler("shipment-batch", shipmentFlow::run, shipmentFlow::requestStop,
                Duration.ofSeconds(intEnv(env, "SHIPMENT_INITIAL_DELAY_SEC", 10)), interval);

        RestSender rest = new RestSender(port, orderFlow, shipmentBatch);
        rest.start();
        shipmentBatch.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[INFO] 종료 신호 — 신규 요청 차단, 배치는 청크 경계에서 중지 (최대 30초)");
            rest.stop(5);
            shipmentBatch.stop(Duration.ofSeconds(30));
        }, "shutdown"));

        System.out.println("[INFO] EAI 연계 서비스 기동 — POST http://localhost:" + rest.port() + "/orders  (IF_ORD_001 DB + IF_ORD_002 FTP)");
        System.out.println("[INFO] 운송 배치(IF_SHP_001) " + interval.toSeconds() + "초 주기 — 수동 실행: POST http://localhost:" + rest.port() + "/batch/shipments/run");
        System.out.println("[INFO] 모니터링 로그: logs/interface-<날짜>.log");
    }

    /** 기동 점검 결과도 모니터링 로그에 1줄 — 화면 출력은 사라지지만 "그 시점 기동 상태"는 남아야 한다. */
    private static void writeStartupLog(InterfaceLogWriter log, List<Preflight.Check> checks) {
        var rec = log.newRecord();
        rec.put("timestamp", java.time.OffsetDateTime.now(ZoneId.of("Asia/Seoul")).toString());
        rec.put("interfaceId", "SYSTEM");
        rec.put("type", "STARTUP");
        Preflight.Level worst = checks.stream().map(Preflight.Check::level).max(Enum::compareTo).orElse(Preflight.Level.OK);
        rec.put("resultCode", worst.name());
        var arr = rec.putArray("checks");
        checks.forEach(c -> arr.addObject().put("target", c.target()).put("level", c.level().name()).put("message", c.message()));
        log.write(rec);
    }

    private static int intEnv(Env env, String name, int defaultValue) {
        String v = env.get(name);
        return v == null ? defaultValue : Integer.parseInt(v);
    }

    /** 영수증 파일 인코딩. 기본 UTF-8 — FTP 서버가 UTF-8(한글 파일명)이고 무손실. 수신측 요구 시 RECEIPT_CHARSET=EUC-KR. */
    private static Charset receiptCharset(Env env) {
        String cs = env.get("RECEIPT_CHARSET");
        return cs == null ? StandardCharsets.UTF_8 : Charset.forName(cs);
    }
}
