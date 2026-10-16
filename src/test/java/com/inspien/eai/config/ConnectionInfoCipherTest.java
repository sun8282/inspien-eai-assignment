package com.inspien.eai.config;

import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionInfoCipherTest {

    private static final String PHONE = "010-1234-5678";

    @Test
    void 가이드_규칙대로_암호화된_값을_복호화한다() throws Exception {
        // 가이드 규칙을 테스트 안에서 독립적으로 재구현해 암호문 생성
        byte[] sha1 = MessageDigest.getInstance("SHA-1").digest(PHONE.getBytes(StandardCharsets.UTF_8));
        Cipher c = Cipher.getInstance("AES/ECB/PKCS5Padding");
        c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(Arrays.copyOf(sha1, 16), "AES"));
        String plain = "{\"HOST\":\"db.example\",\"USER\":\"홍길동\"}";
        String encrypted = Base64.getEncoder().encodeToString(c.doFinal(plain.getBytes(StandardCharsets.UTF_8)));

        assertEquals(plain, new ConnectionInfoCipher(PHONE).decrypt(encrypted));
    }

    @Test
    void 키가_다르면_원인을_알려주는_예외() {
        String encrypted = new ConnectionInfoCipher(PHONE).encrypt("secret");
        var e = assertThrows(ConnectionInfoCipher.DecryptionException.class,
                () -> new ConnectionInfoCipher("01012345678").decrypt(encrypted));
        assertTrue(e.getMessage().contains("휴대폰 번호"));
    }

    @Test
    void Base64가_아니면_예외() {
        assertThrows(ConnectionInfoCipher.DecryptionException.class,
                () -> new ConnectionInfoCipher(PHONE).decrypt("not base64 !!"));
    }

    @Test
    void 키는_16바이트() {
        assertEquals(16, ConnectionInfoCipher.deriveKey(PHONE).getEncoded().length);
    }
}
