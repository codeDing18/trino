package io.trino.plugin.doris;

import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorMetadata;
import jakarta.inject.Inject;

import java.util.Set;

public class DefaultDorisMetadataFactory implements DorisMetadataFactory {
    private final DorisFeClient feClient;
//    private final Set<SessionPropertiesProvider> sessionPropertiesProviders;
//    private final DorisConfig dorisConfig;

    @Inject
    public DefaultDorisMetadataFactory(DorisFeClient feClient) {
        this.feClient = feClient;
//        this.sessionPropertiesProviders = sessionPropertiesProviders;
//        this.dorisConfig = dorisConfig;
    }

    @Override
    public DorisMetadata create(DorisTransactionHandle transaction)
    {
        return new DorisMetadata(feClient);
    }
}
