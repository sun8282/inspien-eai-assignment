package com.inspien.eai.config;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;

/**
 * 과제 제공 접속정보 복호화기.
 *
 * <p>규칙: AES-128 / ECB / PKCS5Padding,
 * Key = SHA-1(휴대폰 번호 UTF-8 바이트)의 앞 16바이트, 암호문은 Base64.
 *
 * <p>운영 원칙: 접속정보는 암호문 그대로 보관하고 <b>사용 시점에만</b> 이 클래스로 복호화한다.
 * 키(휴대폰 번호)는 코드·파일에 두지 않고 환경변수로 주입받는다.
 */
public final class ConnectionInfoCipher {

    private static final String TRANSFORMATION = "AES/ECB/PKCS5Padding";
    private static final int KEY_BYTES = 16;

    private final SecretKeySpec key;

    public ConnectionInfoCipher(String phoneNumber) {
        if (phoneNumber == null || phoneNumber.isBlank()) {
            throw new IllegalArgumentException("복호화 키(휴대폰 번호)가 비어 있습니다");
        }
        this.key = deriveKey(phoneNumber);
    }

    static SecretKeySpec deriveKey(String phoneNumber) {
        try {
            byte[] sha1 = MessageDigest.getInstance("SHA-1")
                    .digest(phoneNumber.getBytes(StandardCharsets.UTF_8));
            return new SecretKeySpec(Arrays.copyOf(sha1, KEY_BYTES), "AES");
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("SHA-1 을 사용할 수 없는 JVM 입니다", e);
        }
    }

    public String decrypt(String base64CipherText) {
        if (base64CipherText == null || base64CipherText.isBlank()) {
            throw new DecryptionException("암호문이 비어 있습니다", null);
        }
        try {
            byte[] encrypted = Base64.getDecoder().decode(base64CipherText.trim());
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key);
            return new String(cipher.doFinal(encrypted), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new DecryptionException("Base64 형식이 아닙니다", e);
        } catch (GeneralSecurityException e) {
            // 패딩 오류 = 대부분 키 불일치(입력한 전화번호와 다른 값)
            throw new DecryptionException("복호화 실패 — 휴대폰 번호(키)가 요청 시 값과 같은지 확인하세요", e);
        }
    }

    /** 테스트·검증용 역연산. */
    String encrypt(String plainText) {
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, key);
            return Base64.getEncoder().encodeToString(cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    public static final class DecryptionException extends RuntimeException {
        public DecryptionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
