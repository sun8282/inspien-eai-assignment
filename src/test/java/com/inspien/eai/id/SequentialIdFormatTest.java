package com.inspien.eai.id;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SequentialIdFormatTest {

    @Test
    void 처음은_A001부터() {
        assertEquals(List.of("A001", "A002", "A003"), SequentialIdFormat.next(null, 3));
    }

    @Test
    void 숫자_자리를_넘기면_다음_알파벳() {
        assertEquals(List.of("A999", "B000", "B001"), SequentialIdFormat.next("A998", 3));
    }

    @Test
    void 모든_ID는_규격을_만족() {
        SequentialIdFormat.next("A113", 2000).forEach(id -> assertTrue(SequentialIdFormat.PATTERN.matcher(id).matches(), id));
    }

    @Test
    void Z999_이후는_소진_예외() {
        assertEquals(List.of("Z999"), SequentialIdFormat.next("Z998", 1));
        assertThrows(SequentialIdFormat.IdExhaustedException.class, () -> SequentialIdFormat.next("Z998", 2));
        assertEquals(0, SequentialIdFormat.remaining("Z999"));
    }

    @Test
    void 사전순과_발급순이_같다() {
        assertEquals("B000", SequentialIdFormat.max("A999", "B000"));
        assertEquals("A113", SequentialIdFormat.max(null, "A113"));
    }
}
