package com.inspien.eai.mapping;

import com.inspien.eai.core.EaiException;
import com.inspien.eai.core.ErrorCode;
import com.inspien.eai.model.ExcludedRecord.Reason;
import com.inspien.eai.model.OrderRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OrderMappingTest {

    /** 실제 제공 샘플과 같은 형태: 루트 없음, HEADER/ITEM 뒤섞임, 고아 ITEM, ITEM 없는 HEADER, STATUS 개행 */
    private static final String SAMPLE = """
            <HEADER>
                <USER_ID>USER1</USER_ID>
                <NAME>홍길동</NAME>
                <ADDRESS>서울특별시 금천구</ADDRESS>
                <STATUS>
                    N
                </STATUS>
            </HEADER>
            <ITEM>
                <USER_ID>USER2</USER_ID>
                <ITEM_ID>ITEM2</ITEM_ID>
                <ITEM_NAME>티셔츠</ITEM_NAME>
                <PRICE>15800</PRICE>
            </ITEM>
            <ITEM>
                <USER_ID>USER1</USER_ID>
                <ITEM_ID>ITEM1</ITEM_ID>
                <ITEM_NAME>청바지</ITEM_NAME>
                <PRICE>21000</PRICE>
            </ITEM>
            <HEADER>
                <USER_ID>USER2</USER_ID>
                <NAME>유관순</NAME>
                <ADDRESS>서울특별시 구로구</ADDRESS>
                <STATUS>N</STATUS>
            </HEADER>
            <HEADER>
                <USER_ID>USER3</USER_ID>
                <NAME>이순신</NAME>
                <ADDRESS>서울특별시 중구</ADDRESS>
                <STATUS>N</STATUS>
            </HEADER>
            <ITEM>
                <USER_ID>USER9</USER_ID>
                <ITEM_ID>ITEM9</ITEM_ID>
                <ITEM_NAME>양말</ITEM_NAME>
                <PRICE>4500</PRICE>
            </ITEM>
            """;

    private final OrderXmlParser parser = new OrderXmlParser();
    private final OrderFlattener flattener = new OrderFlattener();

    private OrderFlattener.Result map(String xml) {
        OrderXmlParser.Parsed p = parser.parse(xml);
        return flattener.flatten(p.headers(), p.items());
    }

    @Test
    void 루트없는_XML을_파싱하고_STATUS를_trim한다() {
        OrderXmlParser.Parsed p = parser.parse(SAMPLE);
        assertEquals(3, p.headers().size());
        assertEquals(3, p.items().size());
        assertEquals("N", p.headers().get(0).status());
    }

    @Test
    void 위치가_아니라_USER_ID로_조인한다() {
        OrderFlattener.Result r = map(SAMPLE);
        assertEquals(2, r.records().size());
        OrderRecord first = r.records().get(0); // 문서 순서상 첫 ITEM 은 USER2 — 뒤에 나오는 HEADER 와 조인돼야 함
        assertEquals("USER2", first.userId());
        assertEquals("유관순", first.name());
        assertEquals("서울특별시 구로구", first.address());
        assertEquals("티셔츠", first.itemName());
    }

    @Test
    void 고아_ITEM과_ITEM없는_HEADER는_사유와_함께_제외된다() {
        OrderFlattener.Result r = map(SAMPLE);
        assertEquals(2, r.excluded().size());
        assertTrue(r.excluded().stream().anyMatch(x -> x.reason() == Reason.NO_HEADER && "USER9".equals(x.userId())));
        assertTrue(r.excluded().stream().anyMatch(x -> x.reason() == Reason.NO_ITEM && "USER3".equals(x.userId())));
    }

    @Test
    void XML선언과_기존_루트가_있어도_동작한다() {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ORDERS>" + SAMPLE + "</ORDERS>";
        assertEquals(2, map(xml).records().size());
    }

    @Test
    void 중복_HEADER는_모호하므로_제외() {
        String xml = SAMPLE + "<HEADER><USER_ID>USER1</USER_ID><NAME>다른사람</NAME><ADDRESS>부산</ADDRESS><STATUS>N</STATUS></HEADER>";
        OrderFlattener.Result r = map(xml);
        assertTrue(r.records().stream().noneMatch(x -> "USER1".equals(x.userId())));
        assertTrue(r.excluded().stream().anyMatch(x -> x.reason() == Reason.DUPLICATE_HEADER));
    }

    @Test
    void 필드_검증_가격숫자_구분자_길이() {
        String bad = """
                <HEADER><USER_ID>U1</USER_ID><NAME>홍^길동</NAME><ADDRESS>서울</ADDRESS><STATUS>N</STATUS></HEADER>
                <ITEM><USER_ID>U1</USER_ID><ITEM_ID>I1</ITEM_ID><ITEM_NAME>a</ITEM_NAME><PRICE>100</PRICE></ITEM>
                <HEADER><USER_ID>U2</USER_ID><NAME>김</NAME><ADDRESS>서울</ADDRESS><STATUS>N</STATUS></HEADER>
                <ITEM><USER_ID>U2</USER_ID><ITEM_ID>I2</ITEM_ID><ITEM_NAME>a</ITEM_NAME><PRICE>1,000</PRICE></ITEM>
                <HEADER><USER_ID>U3</USER_ID><NAME>박</NAME><ADDRESS>%s</ADDRESS><STATUS>N</STATUS></HEADER>
                <ITEM><USER_ID>U3</USER_ID><ITEM_ID>I3</ITEM_ID><ITEM_NAME>a</ITEM_NAME><PRICE>100</PRICE></ITEM>
                <HEADER><USER_ID>U4</USER_ID><NAME>최</NAME><ADDRESS>서울</ADDRESS><STATUS>N</STATUS></HEADER>
                <ITEM><USER_ID>U4</USER_ID><ITEM_ID>I4</ITEM_ID><PRICE>100</PRICE></ITEM>
                """.formatted("가".repeat(34)); // 34자 * 3바이트 = 102바이트
        OrderFlattener.Result r = map(bad);
        assertEquals(0, r.records().size());
        assertEquals(4, r.excluded().size());
        assertTrue(r.excluded().stream().allMatch(x -> x.reason() == Reason.INVALID_FIELD));
    }

    @Test
    void 깨진_XML과_DTD는_XML_PARSE() {
        assertEquals(ErrorCode.XML_PARSE, assertThrows(EaiException.class, () -> parser.parse("<HEADER><USER_ID>")).code());
        String xxe = "<!DOCTYPE x [<!ENTITY e SYSTEM \"file:///etc/passwd\">]><HEADER><USER_ID>&e;</USER_ID></HEADER>";
        assertEquals(ErrorCode.XML_PARSE, assertThrows(EaiException.class, () -> parser.parse(xxe)).code());
    }

    @Test
    void 인코딩_선언_우선_미선언시_UTF8_실패하면_EUC_KR() {
        Charset eucKr = Charset.forName("EUC-KR");
        byte[] euc = SAMPLE.getBytes(eucKr);
        assertEquals(SAMPLE, PayloadDecoder.decode(euc, "application/xml; charset=EUC-KR").text());
        assertEquals(SAMPLE, PayloadDecoder.decode(euc, "application/xml").text());
        assertEquals("EUC-KR", PayloadDecoder.decode(euc, null).charset().name());
        byte[] utf8 = SAMPLE.getBytes(StandardCharsets.UTF_8);
        assertEquals(SAMPLE, PayloadDecoder.decode(utf8, null).text());
        assertEquals(ErrorCode.UNSUPPORTED_ENCODING,
                assertThrows(EaiException.class, () -> PayloadDecoder.decode(euc, "text/xml; charset=UTF-8")).code());
    }

    @Test
    void 결과행은_문서상_ITEM_순서를_유지한다() {
        List<String> items = map(SAMPLE).records().stream().map(OrderRecord::itemId).toList();
        assertEquals(List.of("ITEM2", "ITEM1"), items);
    }
}
