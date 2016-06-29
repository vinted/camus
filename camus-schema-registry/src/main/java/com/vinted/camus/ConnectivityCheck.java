package com.vinted.camus.schemaregistry;

import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpResponse;
import org.apache.http.HttpEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.client.methods.HttpGet;


public class ConnectivityCheck {
    private static final Integer HTTP_SUCCESS_CODE = 200;

    public static void check(URI schemaIndexAddress) throws Exception
    {
        HttpResponse indexResponse = getResponse(schemaIndexAddress);

        assert(indexResponse.getStatusLine().getStatusCode() == HTTP_SUCCESS_CODE);
    }

    private static HttpResponse getResponse(URI schemaIndexAddress) throws IOException
    {
        DefaultHttpClient httpClient = new DefaultHttpClient();

        HttpGet requestMethod = new HttpGet(schemaIndexAddress);
        return httpClient.execute(requestMethod);
    }

}
