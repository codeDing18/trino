package io.trino.plugin.doris;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class DorisTransactionManager
{
    private final ConcurrentMap<ConnectorTransactionHandle, DorisMetadata> transactions = new ConcurrentHashMap<>();
    private final DorisMetadataFactory dorisMetadataFactory;

    @Inject
    public DorisTransactionManager(DorisMetadataFactory dorisMetadataFactory)
    {
        this.dorisMetadataFactory = requireNonNull(dorisMetadataFactory, "dorisMetadata is null");
    }

    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        DorisTransactionHandle transaction = new DorisTransactionHandle(true);
        transactions.put(transaction, dorisMetadataFactory.create(transaction));
        return transaction;
    }

    public DorisMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        DorisMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

//    public void commit(ConnectorTransactionHandle transaction)
//    {
//        return;
////        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
//    }
//
//    public void rollback(ConnectorTransactionHandle transaction)
//    {
//        return;
////        DorisMetadata metadata = transactions.remove(transaction);
////        checkArgument(metadata != null, "no such transaction: %s", transaction);
//        // todo
////        metadata.rollback();
//    }
}
