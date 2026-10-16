package com.inspien.eai.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * 요청(메시지) 1건의 추적 컨텍스트. MSG_ID 하나로 응답·로그·DB·파일을 잇는다.
 * 단계별 소요시간·결과를 쌓아 "어디서 끊겼는지"를 로그 한 줄로 보이게 한다.
 */
public final class MessageContext {

    public record Step(String name, String status, long durationMs, String detail) { }

    private final String msgId = UUID.randomUUID().toString();
    private final String interfaceId;
    private final Instant startedAt = Instant.now();
    private final long startedNanos = System.nanoTime();
    private final List<Step> steps = new ArrayList<>();

    public MessageContext(String interfaceId) {
        this.interfaceId = interfaceId;
    }

    @FunctionalInterface
    public interface StepBody<T> {
        T run() throws Exception;
    }

    /** 단계를 실행하고 결과(OK/FAIL)와 소요시간을 기록한다. 예외는 그대로 전파. */
    public <T> T step(String name, StepBody<T> body) throws Exception {
        long t = System.nanoTime();
        try {
            T result = body.run();
            steps.add(new Step(name, "OK", elapsed(t), null));
            return result;
        } catch (Exception e) {
            steps.add(new Step(name, "FAIL", elapsed(t), e.getClass().getSimpleName()));
            throw e;
        }
    }

    public void note(String name, String status, String detail) {
        steps.add(new Step(name, status, 0, detail));
    }

    private static long elapsed(long fromNanos) {
        return (System.nanoTime() - fromNanos) / 1_000_000;
    }

    public String msgId() { return msgId; }
    public String interfaceId() { return interfaceId; }
    public Instant startedAt() { return startedAt; }
    public long elapsedMs() { return elapsed(startedNanos); }
    public List<Step> steps() { return Collections.unmodifiableList(steps); }
}
