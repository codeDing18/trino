package io.trino.plugin.doris;

import io.trino.spi.connector.ConnectorMetadata;

public interface DorisMetadataFactory {
    DorisMetadata create(DorisTransactionHandle transaction);
}
