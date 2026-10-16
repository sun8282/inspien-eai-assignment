package com.inspien.eai.mapping;

import com.inspien.eai.core.EaiException;
import com.inspien.eai.core.ErrorCode;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 요청 바이트 → 문자열. 한글 깨짐을 막기 위해 "추측"보다 "선언"을 우선한다.
 * 우선순위: Content-Type charset → XML 선언 encoding → UTF-8(엄격) → EUC-KR(제공 샘플 원본 인코딩).
 */
public final class PayloadDecoder {

    public record Decoded(String text, Charset charset, String source) {
        /** 로그용: "EUC-KR (Content-Type 선언)" */
        public String describe() { return charset.name() + " (" + source + ")"; }
    }

    private static final Pattern CT_CHARSET = Pattern.compile("(?i)charset\\s*=\\s*\"?([\\w.:-]+)\"?");
    private static final Pattern XML_ENCODING = Pattern.compile("^\\s*<\\?xml[^>]*encoding\\s*=\\s*[\"']([\\w.:-]+)[\"']");
    private static final Charset EUC_KR = Charset.forName("EUC-KR");

    private PayloadDecoder() { }

    public static Decoded decode(byte[] body, String contentType) {
        byte[] bytes = stripUtf8Bom(body);

        Charset declared = contentType == null ? null : charsetFrom(CT_CHARSET.matcher(contentType));
        if (declared != null) return new Decoded(strict(bytes, declared), declared, "Content-Type 선언");

        // XML 선언은 ASCII 범위라 ISO-8859-1 로 앞부분만 읽어도 안전
        String head = new String(bytes, 0, Math.min(bytes.length, 200), StandardCharsets.ISO_8859_1);
        Charset xmlDeclared = charsetFrom(XML_ENCODING.matcher(head));
        if (xmlDeclared != null) return new Decoded(strict(bytes, xmlDeclared), xmlDeclared, "XML 선언");

        try {
            return new Decoded(strictOrThrow(bytes, StandardCharsets.UTF_8), StandardCharsets.UTF_8, "미선언, 기본값");
        } catch (CharacterCodingException notUtf8) {
            return new Decoded(strict(bytes, EUC_KR), EUC_KR, "미선언, UTF-8 해석 실패 → 대체");
        }
    }

    private static Charset charsetFrom(Matcher m) {
        if (!m.find()) return null;
        try {
            return Charset.forName(m.group(1));
        } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
            throw new EaiException(ErrorCode.UNSUPPORTED_ENCODING, "지원하지 않는 charset: " + m.group(1));
        }
    }

    private static String strict(byte[] bytes, Charset cs) {
        try {
            return strictOrThrow(bytes, cs);
        } catch (CharacterCodingException e) {
            throw new EaiException(ErrorCode.UNSUPPORTED_ENCODING, cs.name() + " 로 해석할 수 없는 바이트 포함", e);
        }
    }

    private static String strictOrThrow(byte[] bytes, Charset cs) throws CharacterCodingException {
        return cs.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
    }

    private static byte[] stripUtf8Bom(byte[] b) {
        if (b.length >= 3 && (b[0] & 0xFF) == 0xEF && (b[1] & 0xFF) == 0xBB && (b[2] & 0xFF) == 0xBF) {
            byte[] out = new byte[b.length - 3];
            System.arraycopy(b, 3, out, 0, out.length);
            return out;
        }
        return b;
    }
}
