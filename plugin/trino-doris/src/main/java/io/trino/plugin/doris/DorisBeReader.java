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
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TNetworkAddress;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.lucene.analysis.compound.hyphenation.CharVector;
import org.apache.thrift.TException;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
import static org.apache.arrow.vector.types.Types.MinorType.DECIMAL256;
import java.time.LocalDateTime;

public class DorisBeReader
{
    private static final Logger log = Logger.get(DorisBeReader.class);
    private static final int ROWS_PER_REQUEST = 4096;

    private final TDorisExternalService.Client client;
    private final String scanId;
    private final TNetworkAddress beAddress;
    private final TScanNextBatchParams params;
    private final ClientPool clientPool;

    private boolean closed;

    private TScanBatchResult batchResult;
    private PageBuilder pageBuilder;


    public DorisBeReader(TDorisExternalService.Client client, String scanId, TNetworkAddress beAddress, ClientPool clientPool) {
        this.client = client;
        this.scanId =scanId;
        this.beAddress = beAddress;
        this.params = new TScanNextBatchParams().setContextId(scanId);
        this.clientPool = clientPool;
    }

    public TScanBatchResult getNextResult(long offset) {
        try {
            batchResult = client.getNext(params.setOffset(offset));
            return batchResult;
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            log.debug(" client.closeScanner");
            client.closeScanner(new TScanCloseParams().setContextId(scanId));
            log.debug("clientPool.getBePool().returnObject(beAddress, client)");
            clientPool.getBePool().returnObject(beAddress, client);

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
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(DorisBeReader.class.getClassLoader())) {
            if (pageBuilder == null ) {
                this.pageBuilder = new PageBuilder(types);
            }

            List<Page> pages = Lists.newArrayList();

            TScanBatchResult nextResult;
            long rowCount = 0;

            do {
                nextResult = getNextResult(rowCount);
                if (nextResult.isEos())
                    break;

                byte[] arrowData = nextResult.getRows();
                log.info("look arrow data size is : %s", arrowData.length);

                try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                        ByteArrayInputStream bis = new ByteArrayInputStream(arrowData);
                        ArrowStreamReader reader = new ArrowStreamReader(Channels.newChannel(bis), allocator)) {
                    // 获取 VectorSchemaRoot，它包含 schema 和所有列向量
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();

                    log.debug("VectorSchemaRoot root = reader.getVectorSchemaRoot();");

                    List<FieldVector> fieldVectors = root.getFieldVectors();

                    log.debug("begin to loadNextBatch");

                    // 循环读取每个批次的数据
                    while (reader.loadNextBatch()) {
                        log.info("have read the batch, include " + root.getRowCount() + " row");

                        log.debug("look at be arrow return content is : %s", root.contentToTSVString());
                        // 遍历当前批次的所有行
                        rowCount += root.getRowCount();
                        for (int i = 0; i < root.getRowCount(); i++) {
                            pageBuilder.declarePosition();

                            for (int j = 0; j < types.size(); j++) {
                                convertArrowData(pageBuilder.getBlockBuilder(j), types.get(j), fieldVectors.get(j), i);
                            }
                        }

                        // 如果我判读page是否满了的话，这page return了，后面的数据怎么处理
                        // 所以目前只能获取一批返回一个Page
                        if (pageBuilder.isFull() || rowCount >= ROWS_PER_REQUEST) {
                            pages.add(pageBuilder.build());
                            pageBuilder.reset();
                        }
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } while (!nextResult.isEos());

            log.info("successfully get arrow data to trino data");
            pages.add(pageBuilder.build());
            return pages;
        }
    }

    private void convertArrowData(BlockBuilder blockBuilder, Type type, FieldVector valueVector, int index) {
        switch (type.getBaseName()) {
            case "boolean" :
                BitVector bitVector = (BitVector) valueVector;
                if (bitVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    BooleanType booleanType = (BooleanType) type;
                    booleanType.writeBoolean(blockBuilder, bitVector.getObject(index));
                }

                break;
            case "tinyint" :
                TinyIntVector tinyintVector = (TinyIntVector) valueVector;
                if (tinyintVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    TinyintType tinyintType = (TinyintType) type;
                    tinyintType.writeByte(blockBuilder, tinyintVector.get(index));
                }

                break;
            case "smallint" :
                SmallIntVector smallVector = (SmallIntVector) valueVector;
                if (smallVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    SmallintType smallType = (SmallintType) type;
                    smallType.writeShort(blockBuilder, smallVector.get(index));
                }

                break;
            case "integer" :
                IntVector intVector = (IntVector) valueVector;
                if (intVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    IntegerType integerType = (IntegerType) type;
                    integerType.writeInt(blockBuilder, intVector.get(index));
                }

                break;
            case "bigint" :
                BigIntVector bigintVector = (BigIntVector) valueVector;
                if (bigintVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    BigintType bigintType = (BigintType) type;
                    bigintType.writeLong(blockBuilder, bigintVector.get(index));
                }

                break;
            case "real" :
                Float4Vector floatVector = (Float4Vector) valueVector;
                if (floatVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    RealType floatType = (RealType) type;
                    floatType.writeFloat(blockBuilder, floatVector.get(index));
                }

                break;
            case "double" :
                Float8Vector doubleVector = (Float8Vector) valueVector;
                if (doubleVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    DoubleType doubleTypeType = (DoubleType) type;
                    doubleTypeType.writeDouble(blockBuilder, doubleVector.get(index));
                }

                break;
            case "decimal" :
                if (valueVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    DecimalType decimalType = (DecimalType) type;
                    if (valueVector.getMinorType() == DECIMAL256) {
                        BigDecimal decimal = ((Decimal256Vector) valueVector).getObject(index);
                        type.writeObject(blockBuilder, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
                    } else {
                        BigDecimal decimal = ((DecimalVector) valueVector).getObject(index);
                        decimalType.writeLong(blockBuilder, encodeShortScaledValue(decimal, decimalType.getScale()));
                    }
                }

                break;
            case "varchar" :
                VarCharVector varCharVector = (VarCharVector) valueVector;
                if (varCharVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    VarcharType varcharType = (VarcharType) type;
                    varcharType.writeSlice(blockBuilder, Slices.wrappedBuffer(varCharVector.get(index)));
                }

                break;
            case "char" :
                VarCharVector charVector = (VarCharVector) valueVector;
                if (charVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    CharType charType = (CharType) type;
                    charType.writeSlice(blockBuilder, Slices.wrappedBuffer(charVector.get(index)));
                }

                break;
            // 看下Doris的datetime是否对应这个类型
            case "timestamp" :
                TimeStampSecTZVector timestampVector = (TimeStampSecTZVector) valueVector;
                if (timestampVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    TimestampType timestampType = (TimestampType) type;
                    if (timestampType.isShort()) {

                        long seconds = timestampVector.get(index);
                        log.debug("look at timestampVector.get(index) is " +seconds);
                        String timeZone = timestampVector.getTimeZone();
                        log.debug("look at timeZone is " + timeZone);
                        // 微秒转成秒（需要整数除法），剩余的微秒作为纳秒
//                        long epochSeconds = epochMicros / 1_000_000;  // 取整秒
//                        long nanoAdjustment = (epochMicros % 1_000_000) * 1000;  // 微秒转纳秒

                        ZoneId zoneId = ZoneId.of(timeZone);
                        Instant instant = Instant.ofEpochSecond(seconds);

                        // 转换为本地日期时间（需要指定时区）
                        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zoneId);
                        log.debug("look at dateTime is: " + dateTime.toString());


                        type.writeLong(blockBuilder, TypeConverter.toTrinoTimestamp(dateTime.toString()));
//                        type.writeLong(blockBuilder, (instant.getEpochSecond() * MICROSECONDS_PER_SECOND) + (instant.getNano() / NANOSECONDS_PER_MICROSECOND));



//                        int picosOfMillis = toIntExact(floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND)) * PICOSECONDS_PER_MICROSECOND;
//                        type.writeObject(blockBuilder, fromEpochMillisAndFraction(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND), picosOfMillis, UTC_KEY));
//                        type.writeLong(blockBuilder, timestampVector.get(index));
                    } else {
                        long epochMicros = timestampVector.get(index);
                        int picosOfMillis = toIntExact(floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND)) * PICOSECONDS_PER_MICROSECOND;
                        type.writeObject(blockBuilder, fromEpochMillisAndFraction(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND), picosOfMillis, UTC_KEY));
                    }
                }

                break;
            // time在doris的olap表中不支持
//            case "time" :
//                VarCharVector charVector = (VarCharVector) valueVector;
//                if (charVector.isNull(index)) {
//                    blockBuilder.appendNull();
//                } else {
//                    TimeType timeType = (TimeType) type;
//                    charType.writeSlice(blockBuilder, Slices.wrappedBuffer(charVector.get(index)));
//                }
//
//                break;
            case "date" :
                DateDayVector dateVector = (DateDayVector) valueVector;
                if (dateVector.isNull(index)) {
                    blockBuilder.appendNull();
                } else {
                    int daysSinceEpoch = dateVector.get(index);  // 获取整数天数

                    DateType dateType = (DateType) type;
                    dateType.writeInt(blockBuilder, daysSinceEpoch);
                }

                break;
            default:
                throw new UnsupportedOperationException("unsupported type : " + type.getBaseName());
        }

//        // 基于 FieldVector 类型的判断
//        if (valueVector instanceof BitVector) {
//            BitVector bitVector = (BitVector) valueVector;
//            if (bitVector.isNull(index)) {
//                blockBuilder.appendNull();
//            } else {
//                BooleanType booleanType = (BooleanType) type;
//                booleanType.writeBoolean(blockBuilder, bitVector.getObject(index));
//            }
//        }
//        else if (valueVector instanceof TinyIntVector) {
//            TinyIntVector tinyintVector = (TinyIntVector) valueVector;
//            if (tinyintVector.isNull(index)) {
//                blockBuilder.appendNull();
//            } else {
//                TinyintType tinyintType = (TinyintType) type;
//                tinyintType.writeByte(blockBuilder, tinyintVector.get(index));
//            }
//        }
//        else if (valueVector instanceof SmallIntVector) {
//            SmallIntVector smallVector = (SmallIntVector) valueVector;
//            if (smallVector.isNull(index)) {
//                blockBuilder.appendNull();
//            } else {
//                SmallintType smallType = (SmallintType) type;
//                smallType.writeShort(blockBuilder, smallVector.get(index));
//            }
//        }
//        else if (valueVector instanceof IntVector) {
//            IntVector intVector = (IntVector) valueVector;
//            if (intVector.isNull(index)) {
//                blockBuilder.appendNull();
//            } else {
//                IntegerType integerType = (IntegerType) type;
//                integerType.writeInt(blockBuilder, intVector.get(index));
//            }
//        }
//        else if (valueVector instanceof BigIntVector) {
//            BigIntVector bigintVector = (BigIntVector) valueVector;
//            if (bigintVector.isNull(index)) {
//                blockBuilder.appendNull();
//            } else {
//                BigintType bigintType = (BigintType) type;
//                bigintType.writeLong(blockBuilder, bigintVector.get(index));
//            }
//        }
//        else if (valueVector instanceof Float4Vector) {
//            Float4Vector floatVector = (Float4Vector) valueVector;
//            if (floatVector.isNull(index)) {
//                blockBuilder.appendNull();
//            } else {
//                RealType floatType = (RealType) type;
//                floatType.writeFloat(blockBuilder, floatVector.get(index));
//            }
//        }
//        else if (valueVector instanceof Float8Vector) {
//            Float8Vector doubleVector = (Float8Vector) valueVector;
//            if (doubleVector.isNull(index)) {
//                blockBuilder.appendNull();
//            } else {
//                DoubleType doubleTypeType = (DoubleType) type;
//                doubleTypeType.writeDouble(blockBuilder, doubleVector.get(index));
//            }
//        }
//        else if (valueVector instanceof DecimalVector) {
//            DecimalVector decimalVector = (DecimalVector) valueVector;
//            if (decimalVector.isNull(index)) {
//                blockBuilder.appendNull();
//            } else {
//                DecimalType decimalType = (DecimalType) type;
//                decimalType.writeObject(blockBuilder, decimalVector.getObject(index));
//            }
//        }
//        else if (valueVector instanceof VarCharVector) {
//            VarCharVector varCharVector = (VarCharVector) valueVector;
//            if (varCharVector.isNull(index)) {
//                blockBuilder.appendNull();
//            } else {
//                // 根据实际类型判断是 Varchar 还是 Char
//                if (type instanceof VarcharType) {
//                    VarcharType varcharType = (VarcharType) type;
//                    varcharType.writeSlice(blockBuilder, Slices.wrappedBuffer(varCharVector.get(index)));
//                } else if (type instanceof CharType) {
//                    CharType charType = (CharType) type;
//                    charType.writeSlice(blockBuilder, Slices.wrappedBuffer(varCharVector.get(index)));
//                } else {
//                    throw new UnsupportedOperationException("Unsupported string type: " + type.getClass());
//                }
//            }
//        }
//        else if (valueVector instanceof TimeStampVector) {
//            TimeStampVector timestampVector = (TimeStampVector) valueVector;
//            if (timestampVector.isNull(index)) {
//                blockBuilder.appendNull();
//            } else {
//                TimestampType timestampType = (TimestampType) type;
//                timestampType.writeLong(blockBuilder, timestampVector.get(index));
//            }
//        }
//        else if (valueVector instanceof DateDayVector) {
//            DateDayVector dateVector = (DateDayVector) valueVector;
//            if (dateVector.isNull(index)) {
//                blockBuilder.appendNull();
//            } else {
//                int daysSinceEpoch = dateVector.get(index);
//                DateType dateType = (DateType) type;
//                dateType.writeInt(blockBuilder, daysSinceEpoch);
//            }
//        }
//        else {
//            throw new UnsupportedOperationException("Unsupported FieldVector type: " + valueVector.getClass().getName() + " for Trino type: " + type.getBaseName());
//        }
    }
}
