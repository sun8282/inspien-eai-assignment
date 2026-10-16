package com.inspien.eai.tool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FetchProvisionInfoMaskTest {

    @Test
    void 기본_출력에는_비밀번호_주소_계정이_남지_않는다() throws Exception {
        JsonNode conn = new ObjectMapper().readTree(
                "{\"URL\":\"jdbc:oracle:thin:@10.0.0.1:1521:XE\",\"ID\":\"scott\",\"PASSWORD\":\"tiger\","
                        + "\"PORT\":\"30021\",\"PATH\":\"Recruit/2026/\",\"HOST\":\"10.0.0.2\"}");

        String out = FetchProvisionInfo.mask(conn).toString();

        assertFalse(out.contains("10.0.0.1"));
        assertFalse(out.contains("10.0.0.2"));
        assertFalse(out.contains("scott"));
        assertFalse(out.contains("tiger"));
        assertTrue(out.contains("jdbc:oracle:thin:@***"), "드라이버 종류는 진단용으로 남긴다");
        assertTrue(out.contains("Recruit/2026/"), "비식별 설정값은 그대로");
    }
}
