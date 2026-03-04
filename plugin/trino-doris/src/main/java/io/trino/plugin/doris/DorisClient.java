package io.trino.plugin.doris;

import jakarta.inject.Inject;

public class DorisClient {
    private DorisFeClient feClient;
    private DorisBeClient beClient;

    @Inject
    public DorisClient(DorisFeClient feClient, DorisBeClient beClient) {
        this.feClient = feClient;
        this.beClient = beClient;
    }

    public DorisBeClient getBeClient()
    {
        return beClient;
    }

    public DorisFeClient getFeClient()
    {
        return feClient;
    }
}
