package com.couchbase.sdk.test;

import org.apache.http.HttpResponse;

public class ResponseValidationException extends Exception {
    private static final long serialVersionUID = 1L;
    private final HttpResponse response;

    public ResponseValidationException(HttpResponse response) {
        this.response = response;
    }

    public HttpResponse getResponse() {
        return response;
    }

}
