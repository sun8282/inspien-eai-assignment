package com.inspien.eai.sender;

import com.inspien.eai.flow.BatchResult;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BatchSchedulerTest {

    private static BatchResult ok(String trigger) {
        return new BatchResult("id", "IF", trigger, BatchResult.ResultCode.NO_DATA, "ok",
                0, 0, 0, 0, 0, 0, 0, null, null, List.of(), null, 0);
    }

    @Test
    void 실행중_예외가_나도_다음_주기는_계속된다() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch three = new CountDownLatch(3);
        BatchScheduler s = new BatchScheduler("t", trigger -> {
            calls.incrementAndGet();
            three.countDown();
            if (calls.get() == 1) throw new IllegalStateException("주입된 예외");
            return ok(trigger);
        }, () -> { }, Duration.ZERO, Duration.ofMillis(20));
        s.start();
        assertTrue(three.await(2, TimeUnit.SECONDS), "첫 실행 예외 후에도 주기 실행 지속");
        s.stop(Duration.ofSeconds(1));
    }

    @Test
    void 실행중이면_수동실행은_건너뛴다() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        BatchScheduler s = new BatchScheduler("t", trigger -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return ok(trigger);
        }, () -> { }, Duration.ZERO, Duration.ofHours(1));
        s.start();
        assertTrue(started.await(2, TimeUnit.SECONDS));
        assertTrue(s.runNow("MANUAL").isEmpty(), "중복 실행 거부");
        release.countDown();
        s.stop(Duration.ofSeconds(1));
    }
}
