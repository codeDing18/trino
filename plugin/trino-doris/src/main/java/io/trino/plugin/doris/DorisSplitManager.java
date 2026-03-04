package io.trino.plugin.doris;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.TableNotFoundException;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.trino.plugin.doris.DorisSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.spi.connector.FixedSplitSource.emptySplitSource;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DorisSplitManager implements ConnectorSplitManager {
    private final DorisClient dorisClient;
    private final ListeningExecutorService splitSourceExecutor;


    @Inject
    public DorisSplitManager(
            DorisClient dorisClient,
            @ForDorisSplitManager ListeningExecutorService splitSourceExecutor)
    {
        this.dorisClient = requireNonNull(dorisClient, "DorisClient is null");
        this.splitSourceExecutor = requireNonNull(splitSourceExecutor, "DorisClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        DorisTableHandle table = (DorisTableHandle) connectorTableHandle;

//        if (table.getSnapshotId().isEmpty()) {
//            if (table.isRecordScannedFiles()) {
//                return new FixedSplitSource(ImmutableList.of(), ImmutableList.of());
//            }
//            return emptySplitSource();
//        }

//        IcebergMetadata icebergMetadata = transactionManager.get(transaction, session.getIdentity());
//        Table icebergTable = icebergMetadata.getIcebergTable(session, table.getSchemaTableName());
        Duration dynamicFilteringWaitTimeout = getDynamicFilteringWaitTimeout(session);

//        Scan scan = getScan(icebergMetadata, icebergTable, table, icebergPlanningExecutor);

        DorisSplitSource splitSource = new DorisSplitSource(
                dorisClient,
                session,
                dynamicFilter,
                dynamicFilteringWaitTimeout.toMillis(),
                Stopwatch.createStarted(),
                splitSourceExecutor,
                table,
                constraint);

        return new ClassLoaderSafeConnectorSplitSource(splitSource, DorisSplitManager.class.getClassLoader());
    }
}
