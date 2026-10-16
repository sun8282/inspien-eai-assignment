package com.inspien.eai.model;

/** 회계 담당자에게 보내는 영수증 파일 (FTP Receiver 입력). */
public record ReceiptFile(String name, byte[] content, int lineCount) { }
