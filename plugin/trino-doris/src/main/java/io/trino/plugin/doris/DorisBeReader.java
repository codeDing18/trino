package io.trino.plugin.doris;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TNetworkAddress;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.thrift.TException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;

public class DorisBeReader
{
    private static final Logger log = Logger.get(DorisBeReader.class);
    private static final int ROWS_PER_REQUEST = 4096;

    private final TDorisExternalService.Client client;
    private final String scanId;
    private final TNetworkAddress beAddress;
    private final TScanNextBatchParams params;

    private boolean closed;

    private TScanBatchResult batchResult;
    private PageBuilder pageBuilder;
    private List<FieldVector> fieldVectors;


    public DorisBeReader(TDorisExternalService.Client client, String scanId, TNetworkAddress beAddress) {
        this.client = client;
        this.scanId =scanId;
        this.beAddress = beAddress;
        this.params = new TScanNextBatchParams().setContextId(scanId);
    }

    public TScanBatchResult getNextResult() {
        try {
            batchResult = client.getNext(params);
            return batchResult;
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            client.closeScanner(new TScanCloseParams().setContextId(scanId));
            ClientPool.getBePool().returnObject(beAddress, client);

            this.closed = true;
        }
        catch (TException e) {
            log.error("be reader close fail");
            throw new RuntimeException(e);
        }
    }

    public boolean isClosed()
    {
        return closed;
    }

    public List<Page> getArrowDataToTrinoData(List<Type> types, List<ColumnHandle> columnHandles)
    {
        if (pageBuilder == null ) {
            this.pageBuilder = new PageBuilder(types);
        }

        ImmutableList.Builder<Page> builder = ImmutableList.builder();

        TScanBatchResult nextResult = getNextResult();
        int rowCount = 0;

        do {
            byte[] arrowData = nextResult.getRows();

            pageBuilder.declarePosition();
            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                    ByteArrayInputStream bis = new ByteArrayInputStream(arrowData);
                    ArrowStreamReader reader = new ArrowStreamReader(Channels.newChannel(bis), allocator)) {
                // 获取 VectorSchemaRoot，它包含 schema 和所有列向量
                VectorSchemaRoot root = reader.getVectorSchemaRoot();

                if (fieldVectors == null) {
                    fieldVectors = columnHandles.stream()
                            .map(DorisColumnHandle.class::cast)
                            .map(DorisColumnHandle::getColumnName)
                            .map(root::getVector)
                            .collect(ImmutableList.toImmutableList());
                }


                // 循环读取每个批次的数据
                while (reader.loadNextBatch()) {
                    System.out.println("读取到一个批次，包含 " + root.getRowCount() + " 行");

                    // 遍历当前批次的所有行
                    for (int i = 0; i < root.getRowCount(); i++) {
                        rowCount += root.getRowCount();
                        for (int j = 0; j < types.size(); j++) {
                            convertArrowData(pageBuilder, types.get(j), fieldVectors.get(j), j);
                        }
                    }

                    // 如果我判读page是否满了的话，这page return了，后面的数据怎么处理
                    // 所以目前只能获取一批返回一个Page
                    if (pageBuilder.isFull() || rowCount >= ROWS_PER_REQUEST) {
                        builder.add(pageBuilder.build());
                        pageBuilder.reset();
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        } while (!nextResult.isEos());

        close();
        return builder.build();

//        TScanBatchResult nextResult = getNextResult();
//        if (nextResult.isEos()) {
//            close();
//            return null;
//        }

//        byte[] arrowData = nextResult.getRows();
//
//        pageBuilder.declarePosition();
//        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
//                ByteArrayInputStream bis = new ByteArrayInputStream(arrowData);
//                ArrowStreamReader reader = new ArrowStreamReader(Channels.newChannel(bis), allocator)) {
//            // 获取 VectorSchemaRoot，它包含 schema 和所有列向量
//            VectorSchemaRoot root = reader.getVectorSchemaRoot();
//
//            if (fieldVectors == null) {
//                fieldVectors = columnHandles.stream()
//                        .map(DorisColumnHandle.class::cast)
//                        .map(DorisColumnHandle::getColumnName)
//                        .map(root::getVector)
//                        .collect(ImmutableList.toImmutableList());
//            }
//
//
//            // 循环读取每个批次的数据
//            while (reader.loadNextBatch()) {
//                System.out.println("读取到一个批次，包含 " + root.getRowCount() + " 行");
//
//                // 遍历当前批次的所有行
//                for (int i = 0; i < root.getRowCount(); i++) {
//                    for (int j = 0; j < types.size(); j++) {
//                        convertArrowData(pageBuilder, types.get(j), fieldVectors.get(j), j);
//                    }
//                }
//
//                // 如果我判读page是否满了的话，这page return了，后面的数据怎么处理
//                // 所以目前只能获取一批返回一个Page
////                if (pageBuilder.isFull()) {
////                    Page page = pageBuilder.build();
////                    pageBuilder.reset();
////
////                }
//            }
//
//            return pageBuilder.build();
//        }
//        catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

    private void convertArrowData(PageBuilder pageBuilder, Type type, FieldVector valueVector, int index) {
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(index);

        switch (type.getBaseName()) {
            case "integer" :
                IntVector intVector = (IntVector) valueVector;
                if (intVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    type.writeObject(blockBuilder, intVector.get(index));
                }

                break;
            case "varchar" :
                VarCharVector varCharVector = (VarCharVector) valueVector;
                if (varCharVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    type.writeObject(blockBuilder, varCharVector.get(index));
                }

                break;
        }
    }
}
