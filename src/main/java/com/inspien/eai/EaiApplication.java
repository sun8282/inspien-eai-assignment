package com.inspien.eai;

import com.inspien.eai.config.Env;
import com.inspien.eai.config.ProvisionInfo;
import com.inspien.eai.flow.OrderSyncFlow;
import com.inspien.eai.id.IdAllocator;
import com.inspien.eai.infra.ConnectionFactory;
import com.inspien.eai.monitor.InterfaceLogWriter;
import com.inspien.eai.monitor.OutboundArchive;
import com.inspien.eai.mapping.ReceiptFileMapper;
import com.inspien.eai.receiver.FtpReceiptReceiver;
import com.inspien.eai.receiver.JdbcOrderReceiver;
import com.inspien.eai.sender.RestSender;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.ZoneId;

/**
 * 연계 애플리케이션 진입점 — Sender/Mapper/Receiver 를 조립해 Flow 를 띄운다.
 * 설정 누락·복호화 실패는 기동 단계에서 즉시 중단(Fail-fast).
 */
public final class EaiApplication {

    public static void main(String[] args) throws Exception {
        Env env = Env.load();
        ProvisionInfo info = ProvisionInfo.load(env);
        int port = Integer.parseInt(env.get("SERVER_PORT") == null ? "8080" : env.get("SERVER_PORT"));
        InterfaceLogWriter log = new InterfaceLogWriter(Path.of(env.get("LOG_DIR") == null ? "logs" : env.get("LOG_DIR")));

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

        RestSender rest = new RestSender(port, orderFlow);
        rest.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[INFO] 종료 신호 — 처리 중 요청 마무리 후 중지 (최대 5초)");
            rest.stop(5);
        }, "shutdown"));

        System.out.println("[INFO] EAI 연계 서비스 기동 — POST http://localhost:" + rest.port() + "/orders  (IF_ORD_001 DB + IF_ORD_002 FTP)");
        System.out.println("[INFO] 모니터링 로그: logs/interface-<날짜>.log");
    }

    /** 영수증 파일 인코딩. 기본 UTF-8 — FTP 서버가 UTF-8(한글 파일명)이고 무손실. 수신측 요구 시 RECEIPT_CHARSET=EUC-KR. */
    private static Charset receiptCharset(Env env) {
        String cs = env.get("RECEIPT_CHARSET");
        return cs == null ? StandardCharsets.UTF_8 : Charset.forName(cs);
    }
}
