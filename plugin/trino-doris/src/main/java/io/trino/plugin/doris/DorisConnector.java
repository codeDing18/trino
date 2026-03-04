package io.trino.plugin.doris;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import static java.util.Objects.requireNonNull;

public class DorisConnector implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final DorisMetadata metadata;
    private final DorisSplitManager splitManager;
    private final DorisPageSourceProvider pageSourceProvider;

    @Inject
    public DorisConnector(
            LifeCycleManager lifeCycleManager,
            DorisMetadata metadata,
            DorisSplitManager splitManager,
            DorisPageSourceProvider pageSourceProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return null;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public DorisPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }
}
