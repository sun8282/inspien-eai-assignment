package com.inspien.eai.receiver;

import java.io.IOException;

/** 영수증 파일 Receiver Channel. 세션 단위로 "임시 업로드 → 확정/폐기" 2단계 전송을 제공한다. */
public interface ReceiptReceiver {

    ReceiptSession open() throws IOException;

    /** 대상 위치 표시용(로그) */
    String target();
}
