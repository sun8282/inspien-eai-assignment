package com.inspien.eai.mapping;

import com.inspien.eai.core.EaiException;
import com.inspien.eai.core.ErrorCode;
import com.inspien.eai.model.OrderHeader;
import com.inspien.eai.model.OrderItem;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 주문 XML 파서 (Sender 측 메시지 → 원천 구조).
 *
 * <p>제공 샘플은 루트 엘리먼트 없이 HEADER/ITEM 이 나열된 형태라 그대로는 well-formed XML 이 아니다.
 * XML 선언을 떼고 가상 루트로 감싸 파싱하며, 이미 루트가 있는 문서도 같은 방식으로 처리된다(하위 HEADER/ITEM 탐색).
 * 값은 trim 한다 — 원 과제 예시의 STATUS 처럼 개행·들여쓰기가 섞여 들어올 수 있다.
 */
public final class OrderXmlParser {

    public record Parsed(List<OrderHeader> headers, List<OrderItem> items) { }

    private static final Pattern XML_DECL = Pattern.compile("^\\s*<\\?xml[^>]*\\?>");
    private static final String WRAP_ROOT = "EAI_WRAPPED_ROOT";

    public Parsed parse(String xml) {
        if (xml == null || xml.isBlank()) {
            throw new EaiException(ErrorCode.XML_PARSE, "요청 본문이 비어 있습니다");
        }
        String body = XML_DECL.matcher(xml).replaceFirst("");
        Document doc = toDocument("<" + WRAP_ROOT + ">" + body + "</" + WRAP_ROOT + ">");

        List<OrderHeader> headers = new ArrayList<>();
        for (Element h : elements(doc, "HEADER")) {
            headers.add(new OrderHeader(text(h, "USER_ID"), text(h, "NAME"), text(h, "ADDRESS"), text(h, "STATUS")));
        }
        List<OrderItem> items = new ArrayList<>();
        for (Element i : elements(doc, "ITEM")) {
            items.add(new OrderItem(text(i, "USER_ID"), text(i, "ITEM_ID"), text(i, "ITEM_NAME"), text(i, "PRICE")));
        }
        if (headers.isEmpty() && items.isEmpty()) {
            throw new EaiException(ErrorCode.XML_PARSE, "HEADER/ITEM 엘리먼트가 없습니다");
        }
        return new Parsed(headers, items);
    }

    private static Document toDocument(String xml) {
        try {
            return secureBuilder().parse(new InputSource(new StringReader(xml)));
        } catch (SAXException e) {
            throw new EaiException(ErrorCode.XML_PARSE, "XML 형식 오류: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new EaiException(ErrorCode.XML_PARSE, "XML 읽기 실패", e);
        }
    }

    /** 외부 입력 XML → XXE(외부 엔티티)·DTD 차단. */
    private static DocumentBuilder secureBuilder() {
        try {
            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            f.setFeature("http://xml.org/sax/features/external-general-entities", false);
            f.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            f.setXIncludeAware(false);
            f.setExpandEntityReferences(false);
            DocumentBuilder b = f.newDocumentBuilder();
            b.setErrorHandler(null); // 파싱 오류를 stderr 대신 예외로만
            return b;
        } catch (ParserConfigurationException e) {
            throw new IllegalStateException("XML 파서 구성 실패", e);
        }
    }

    private static List<Element> elements(Document doc, String tag) {
        NodeList list = doc.getElementsByTagName(tag);
        List<Element> out = new ArrayList<>(list.getLength());
        for (int i = 0; i < list.getLength(); i++) out.add((Element) list.item(i));
        return out;
    }

    /** 직계 자식 엘리먼트의 trim 된 텍스트. 없으면 null. */
    private static String text(Element parent, String tag) {
        for (Node n = parent.getFirstChild(); n != null; n = n.getNextSibling()) {
            if (n.getNodeType() == Node.ELEMENT_NODE && tag.equals(n.getNodeName())) {
                return n.getTextContent().strip();
            }
        }
        return null;
    }
}
