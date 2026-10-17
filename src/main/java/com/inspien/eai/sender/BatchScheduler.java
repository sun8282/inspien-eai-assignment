package com.inspien.eai.sender;

import com.inspien.eai.flow.BatchResult;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Sender Channel (Scheduler). 주기 실행 + 수동 실행.
 * <ul>
 *   <li>fixed-delay: 이전 실행이 끝난 뒤 주기를 잰다 — 실행이 길어져도 겹치지 않는다.</li>
 *   <li>중복 실행 거부: 주기 실행과 수동 실행이 동시에 들어와도 하나만 돈다.</li>
 *   <li>예외 차단: ScheduledExecutorService 는 작업이 예외를 던지면 <b>이후 주기를 조용히 취소</b>한다 → 모든 Throwable 을 잡는다.</li>
 *   <li>정상 종료: 중지 신호 → Flow 가 청크 경계에서 멈추고, 최대 대기 후 종료.</li>
 * </ul>
 */
public final class BatchScheduler {

    private final String name;
    private final Function<String, BatchResult> job;
    private final Runnable stopSignal;
    private final Duration initialDelay;
    private final Duration interval;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService executor;

    public BatchScheduler(String name, Function<String, BatchResult> job, Runnable stopSignal,
                          Duration initialDelay, Duration interval) {
        this.name = name;
        this.job = job;
        this.stopSignal = stopSignal;
        this.initialDelay = initialDelay;
        this.interval = interval;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(false);
            return t;
        });
    }

    public void start() {
        executor.scheduleWithFixedDelay(() -> runGuarded("SCHEDULE"),
                initialDelay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** @return 실행 결과, 이미 실행 중이면 empty */
    public Optional<BatchResult> runNow(String trigger) {
        return runGuarded(trigger);
    }

    private Optional<BatchResult> runGuarded(String trigger) {
        if (!running.compareAndSet(false, true)) {
            System.out.println("[INFO] " + name + " 이미 실행 중 — " + trigger + " 실행 건너뜀");
            return Optional.empty();
        }
        try {
            BatchResult r = job.apply(trigger);
            System.out.println("[INFO] " + name + " " + r.resultCode() + " — " + r.message());
            return Optional.of(r);
        } catch (Throwable t) {
            System.err.println("[ERROR] " + name + " 실행 중 예외(다음 주기는 계속됨): " + t);
            return Optional.empty();
        } finally {
            running.set(false);
        }
    }

    public void stop(Duration grace) {
        stopSignal.run();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(grace.toMillis(), TimeUnit.MILLISECONDS)) executor.shutdownNow();
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
