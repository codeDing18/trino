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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.trino.metadata.Split;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.split.SplitSource;
import org.eclipse.jetty.http2.api.Session;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Collections.emptyIterator;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DorisSplitSource implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(DorisSplitSource.class);
    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);
    private static final ConnectorSplitBatch NO_MORE_SPLITS_BATCH = new ConnectorSplitBatch(ImmutableList.of(), true);

    private final DorisClient dorisClient;
    private final ConnectorSession session;
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final Stopwatch dynamicFilterWaitStopwatch;
    private final ListeningExecutorService executor;
    private final DorisTableHandle tableHandle;
    private final Constraint constraint;

    private volatile boolean finished;


    @GuardedBy("closer")
    private final Closer closer = Closer.create();
    @GuardedBy("closer")
    private boolean closed;
    @GuardedBy("closer")
    private ListenableFuture<ConnectorSplitBatch> currentBatchFuture;


    public DorisSplitSource(
            DorisClient dorisClient,
            ConnectorSession session,
            DynamicFilter dynamicFilter,
            long dynamicFilteringWaitTimeoutMillis,
            Stopwatch dynamicFilterWaitStopwatch,
            ListeningExecutorService executor,
            DorisTableHandle tableHandle,
            Constraint constraint) {
        this.dorisClient = dorisClient;
        this.session = session;
        this.dynamicFilter = dynamicFilter;
        this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeoutMillis;
        this.dynamicFilterWaitStopwatch = dynamicFilterWaitStopwatch;
        this.executor = executor;
        this.tableHandle = tableHandle;
        this.constraint = constraint;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
        if (dynamicFilter.isAwaitable() && timeLeft > 0) {
            return dynamicFilter.isBlocked()
                    .thenApply(_ -> EMPTY_BATCH)
                    .completeOnTimeout(EMPTY_BATCH, timeLeft, MILLISECONDS);
        }

        ListenableFuture<ConnectorSplitBatch> nextBatchFuture;
        synchronized (closer) {
            checkState(!closed, "already closed");
            checkState(currentBatchFuture == null || currentBatchFuture.isDone(), "previous batch future is not done");

            // Avoids blocking the calling (scheduler) thread when producing splits, allowing other split sources to
            // start loading splits in parallel to each other
            nextBatchFuture = executor.submit(() -> getNextBatchInternal(maxSize));
            currentBatchFuture = nextBatchFuture;
        }

        return toCompletableFuture(nextBatchFuture).exceptionally(t -> {
            throw new RuntimeException(String.format("%s.%s", tableHandle.getSchemaName(), tableHandle.getTableName()), t);
        });
    }

    private synchronized ConnectorSplitBatch getNextBatchInternal(int maxSize) {

        return new ConnectorSplitBatch(
                dorisClient.getFeClient().getSplits(
                        session,
                        tableHandle,
                        dynamicFilter),
                true);
    }


    @Override
    public void close()
    {
        closeInternal(true);
    }

    @Override
    public boolean isFinished()
    {
        if (currentBatchFuture.isDone()) {
            finish();
            return true;
        }
        return false;
    }

    private synchronized void finish()
    {
        closeInternal(false);
        this.finished = true;
    }

    private void closeInternal(boolean interruptIfRunning)
    {
        synchronized (closer) {
            if (!closed) {
                closed = true;
                // don't cancel the current batch future during normal finishing cleanup
                if (interruptIfRunning && currentBatchFuture != null) {
                    currentBatchFuture.cancel(true);
                }
                // release the reference to the current future unconditionally to avoid OOMs
                currentBatchFuture = null;
                try {
                    closer.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }


}
