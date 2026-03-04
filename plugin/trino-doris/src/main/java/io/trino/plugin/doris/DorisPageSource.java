/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.doris;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import static io.trino.spi.type.BigintType.BIGINT;

public class DorisPageSource implements ConnectorPageSource
{
    private static final Logger log = Logger.get(DorisPageSource.class);
    private static final int ROWS_PER_REQUEST = 4096;

    private final DorisClient dorisClient;
    private final DorisTableHandle table;
    private final DorisSplit split;
    private final List<ColumnHandle> columnHandles;
//    private final PageBuilder pageBuilder;
    private final List<Type> types;
    private final BlockingQueue<Page> pagesQueue = new LinkedBlockingQueue<>();


    private final List<DorisBeReader> beReaders;
    private boolean closed;
    private List<CompletableFuture<List<Page>>> pageFutures;
    private final ExecutorService executor;

    private long start;
    private long end;

    public DorisPageSource(DorisClient dorisClient, DorisTableHandle table, DorisSplit split, List<ColumnHandle> columnHandles) {
        this.dorisClient = dorisClient;
        this.table = table;
        this.split = split;
        this.columnHandles = columnHandles;

        this.executor = dorisClient.getBeClient().getExecutor();

        this.types = columnHandles.stream()
                .map(DorisColumnHandle.class::cast)
                .map(DorisColumnHandle::getColumnType)
                .collect(ImmutableList.toImmutableList());
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
        if (start == 0L){
            start = System.currentTimeMillis();
        }
        for (CompletableFuture<List<Page>> pageFuture : pageFutures) {
            if (!pageFuture.isDone()) {
                return false;
            }
        }

        return pagesQueue.size() == 0;
    }

    @Override
    @SuppressWarnings("removal")
    public Page getNextPage()
    {
        // 如果有多个bereaders,这里是否使用并发的形式获取数据
        if (pageFutures == null) {
            pageFutures = beReaders.stream()
                    .filter(beReader -> !beReader.isClosed())
                    .map(beReader -> {
                        CompletableFuture<List<Page>> future = CompletableFuture.supplyAsync(
                                        () -> beReader.getArrowDataToTrinoData(types, columnHandles),
                                        executor)
                                .thenApplyAsync(pages -> {
                                    pages.forEach(page -> {
                                        try {
                                            pagesQueue.put(page);

                                        }
                                        catch (Exception e) {
                                            log.error("Interrupted while putting page to queue", e);
                                        }
                                    });
                                    return pages;
                                }, executor)
                                .exceptionally(throwable -> {
                                    log.warn(throwable, "get be page failed");
                                    throw new RuntimeException("get be page failed", throwable);
                                });

                        return future;
                    })
                    .collect(ImmutableList.toImmutableList());

        }

        if (pagesQueue.size() > 0) {
            return pagesQueue.poll();
        }

        return null;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (pageFutures != null && isFinished()) {
            return null;
        }
        Page page = getNextPage();

        while (page == null && !isFinished()) {
            page = getNextPage();
        }

        long isDone = pageFutures.stream()
                .map(CompletableFuture::isDone)
                .count();
        long isException = pageFutures.stream()
                .map(CompletableFuture::isCompletedExceptionally)
                .count();
        log.info("isDone pageFuture num is %d, isException pageFuture num is %d", isDone, isException);

        return SourcePage.create(page);
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
        log.debug("from start to close elapse millseconds is %d", System.currentTimeMillis() - start);
        beReaders.forEach(DorisBeReader::close);
    }
}
