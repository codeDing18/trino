package io.trino.plugin.doris;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TNetworkAddress;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanCloseResult;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.doris.sdk.thrift.TScanOpenParams;

import org.apache.doris.sdk.thrift.TScanOpenResult;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;

public class DorisBeClient
{
    private static final Logger log = Logger.get(DorisBeClient.class);

    private static final int DEFAULT_BE_PORT = 9060;
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_INTERVAL_MS = 1000;

//    private final Map<String, TDorisExternalService.Client> beClients;
    private final ExecutorService executor = Executors.newFixedThreadPool(3);

    public DorisBeClient()
    {
//        this.executor = requireNonNull(executor, "executor is null");
//        this.beClients = new HashMap<>();
    }

    public List<DorisBeReader> executeSplit(DorisTableHandle tableHandle,DorisSplit split)
    {
        String queryPlan = split.getOpaquedQueryPlan();
        Map<String, Set<Long>> beToTablets = split.getBeToTablets();

        log.debug("Executing split with query plan, BE count: %d", beToTablets.size());

        List<CompletableFuture<DorisBeReader>> futures = new ArrayList<>();

        for (Map.Entry<String, Set<Long>> entry : beToTablets.entrySet()) {
            String beAddress = entry.getKey();
            Set<Long> tabletIds = entry.getValue();

            futures.add(sendQueryToBE(tableHandle, beAddress, queryPlan, tabletIds));
        }

        // join的话会阻塞，应该直接返回future
        List<DorisBeReader> beReaders = futures.stream()
                .map(CompletableFuture::join)
                .collect(java.util.stream.Collectors.toList());

        return beReaders;
    }

    private CompletableFuture<DorisBeReader> sendQueryToBE(
            DorisTableHandle tableHandle,
            String beAddress,
            String queryPlan,
            Set<Long> tabletIds)
    {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String[] split = beAddress.split(":");
                TNetworkAddress networkAddress = new TNetworkAddress(split[0], Integer.valueOf(split[1]));
                TDorisExternalService.Client client = ClientPool.getBePool().borrowObject(networkAddress);

                TScanOpenParams scanParams = new TScanOpenParams();
                scanParams.setCluster("internal");
                scanParams.setDatabase(tableHandle.getSchemaName());
                scanParams.setTable(tableHandle.getTableName());
                scanParams.setTabletIds(new ArrayList<>(tabletIds));
                scanParams.setOpaquedQueryPlan(queryPlan);

                log.debug("Sending query to BE %s for %d tablets", beAddress, tabletIds.size());

                TScanOpenResult openResult = client.openScanner(scanParams);

                String scanId = openResult.getContextId();

                log.debug("Scan opened on BE %s with scan ID: %s", beAddress, scanId);

                return new DorisBeReader(client, scanId, networkAddress);
            }
            catch (Exception e) {
                log.error("Failed to send query to BE %s", beAddress, e);
                throw new RuntimeException("Failed to send query to BE: " + beAddress, e);
            }
        }, executor);
    }

    public void close()
    {
        //
//        beClients.forEach((beAddress, client) -> {
//            try {
//                client.getInputProtocol().getTransport().close();
//                log.debug("Closed Thrift client for BE: %s", beAddress);
//            }
//            catch (Exception e) {
//                log.warn("Failed to close BE client for: %s", beAddress, e);
//            }
//        });
//        beClients.clear();
    }


    public ExecutorService getExecutor()
    {
        return executor;
    }


}
