package com.rbkmoney.payout.manager.exception;

public class PayoutAlreadyExistsException extends RuntimeException {

    public PayoutAlreadyExistsException(String message) {
        super(message);
    }
}
