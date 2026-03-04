package io.trino.plugin.doris;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import jakarta.inject.Inject;

import java.util.List;

public class DorisPageSourceProvider implements ConnectorPageSourceProvider
{
    private final DorisClient dorisClient;

    @Inject
    public DorisPageSourceProvider(DorisClient dorisClient) {
        this.dorisClient = dorisClient;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        return new DorisPageSource(
                dorisClient,
                (DorisTableHandle) table,
                (DorisSplit) split,
                columns);
    }
}
