package io.trino.plugin.doris;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.doris.sdk.thrift.TScanBatchResult;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class DorisPageSource implements ConnectorPageSource
{
    private static final int ROWS_PER_REQUEST = 4096;

    private final DorisClient dorisClient;
    private final DorisTableHandle table;
    private final DorisSplit split;
    private final List<ColumnHandle> columnHandles;
//    private final PageBuilder pageBuilder;
    private final List<Type> types;

    private final List<DorisBeReader> beReaders;
    private boolean closed;
    private List<CompletableFuture<List<Page>>> pageFutures;

    public DorisPageSource(DorisClient dorisClient, DorisTableHandle table, DorisSplit split, List<ColumnHandle> columnHandles) {
        this.dorisClient = dorisClient;
        this.table = table;
        this.split = split;
        this.columnHandles = columnHandles;

        ImmutableList.Builder<Type> types = ImmutableList.builderWithExpectedSize(columnHandles.size());
        columnHandles.stream()
                .map(DorisColumnHandle.class::cast)
                .map(DorisColumnHandle::getColumnType)
                .map(types::add);
        this.types = types.build();
//        this.pageBuilder = new PageBuilder(this.types);

        this.beReaders = dorisClient.getBeClient().executeSplit(table, split);
    }


    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        for (CompletableFuture<List<Page>> pageFuture : pageFutures) {
            if (pageFuture.isDone()) {
                try {
                    boolean hasData = pageFuture.get().size() > 0;
                    if (hasData) {
                        return false;
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return true;
    }

    @Override
    @SuppressWarnings("removal")
    public Page getNextPage()
    {
        // 如果有多个bereaders,这里是否使用并发的形式获取数据
        if (pageFutures == null) {
            pageFutures = beReaders.stream()
                        .filter(beReader -> !beReader.isClosed())
                        .map(beReader -> CompletableFuture.supplyAsync(
                                () -> beReader.getArrowDataToTrinoData(types, columnHandles),
                                dorisClient.getBeClient().getExecutor()))
                        .collect(ImmutableList.toImmutableList());
        }

        for (CompletableFuture<List<Page>> pageFuture : pageFutures) {
            if (pageFuture.isDone()) {
                List<Page> pages = null;
                try {
                    pages = pageFuture.get();
                    if (pages.size() == 0) {
                        continue;
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }

                return pages.removeFirst();
            }
        }

        return null;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        beReaders.forEach(DorisBeReader::close);
    }
}
