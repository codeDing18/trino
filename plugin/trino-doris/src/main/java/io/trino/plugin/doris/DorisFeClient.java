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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CachingJdbcClient;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import jakarta.inject.Inject;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashSet;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.mysql.cj.exceptions.MysqlErrorNumbers.ER_NO_SUCH_TABLE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.getDomainCompactionThreshold;
import static io.trino.plugin.jdbc.PredicatePushdownController.CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateReadFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static io.trino.plugin.doris.DorisSessionProperties.getDecimalRounding;
import static io.trino.plugin.doris.DorisSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.doris.DorisSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.doris.DorisConfig.DecimalMapping.ALLOW_OVERFLOW;

public class DorisFeClient
{
    private static final Logger log = Logger.get(DorisFeClient.class);

    private static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 6;
    // MySQL driver returns width of timestamp types instead of precision.
    // 19 characters are used for zero-precision timestamps while others
    // require 19 + precision + 1 characters with the additional character
    // required for the decimal separator.
    private static final int ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE = 19;
    // MySQL driver returns width of time types instead of precision, same as the above timestamp type.
    private static final int ZERO_PRECISION_TIME_COLUMN_SIZE = 8;

    // An empty character means that the table doesn't have a comment in MySQL
    private static final String NO_COMMENT = "";

    private static final PredicatePushdownController MYSQL_CHARACTER_PUSHDOWN = (session, domain) -> {
        if (domain.isNullableSingleValue()) {
            return FULL_PUSHDOWN.apply(session, domain);
        }

        Domain simplifiedDomain = domain.simplify(getDomainCompactionThreshold(session));
        if (!simplifiedDomain.getValues().isDiscreteSet()) {
            // Push down inequality predicate
            ValueSet complement = simplifiedDomain.getValues().complement();
            if (complement.isDiscreteSet()) {
                return FULL_PUSHDOWN.apply(session, simplifiedDomain);
            }
            // Domain#simplify can turn a discrete set into a range predicate
            // Push down of range predicate for varchar/char types could lead to incorrect results
            // when the remote database is case insensitive
            return DISABLE_PUSHDOWN.apply(session, domain);
        }
        return FULL_PUSHDOWN.apply(session, simplifiedDomain);
    };


    protected final Set<String> jdbcTypesMappedToVarchar;


    private final Type jsonType;
    private final OkHttpClient httpClient;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
//    private final DriverConnectionFactory connectionFactory;
//    private final GenericObjectPool<Connection> pool;
    private final DorisConfig dorisConfig;
    private final ClientPool clientPool;

    // for cache
    // specifies whether missing values should be cached
    private final boolean cacheMissing;
    private final Cache<ConnectorSession, List<String>> schemaNamesCache;
    private final Cache<TableListingCacheKey, List<String>> tableNamesCache;
    private final Cache<TableHandlesByNameCacheKey, Optional<DorisTableHandle>> tableHandlesByNameCache;
    private final Cache<TableHandlesByQueryCacheKey, DorisTableHandle> tableHandlesByQueryCache;
    private final Cache<ProcedureHandlesByQueryCacheKey, JdbcProcedureHandle> procedureHandlesByQueryCache;
    private final Cache<ColumnsCacheKey, List<DorisColumnHandle>> columnsCache;
    private final Cache<TableListingCacheKey, List<RelationCommentMetadata>> tableCommentsCache;
    private final Cache<DorisTableHandle, TableStatistics> statisticsCache;
    private final Cache<RemoteTableName, List<DorisColumnHandle>> tablePrimaryKeysCache;
    private final Cache<DorisTableHandle, Map<String, Object>> tablePropertiesCache;

    @Inject
    public DorisFeClient(
            ClientPool clientPool,
            BaseJdbcConfig jdbcConfig,
            DorisConfig config,
            TypeManager typeManager) {
        this.clientPool = clientPool;
        this.jdbcTypesMappedToVarchar = ImmutableSortedSet.orderedBy(CASE_INSENSITIVE_ORDER)
                .addAll(requireNonNull(jdbcConfig.getJdbcTypesMappedToVarchar(), "jdbcTypesMappedToVarchar is null"))
                .build();

        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
        this.dorisConfig = config;

        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
//                    .authenticator(((route, response) -> response.request().newBuilder()
//                            .header("Authorization", Credentials.basic(config.getFeUser(), config.getFePassword().orElse("")))
//                            .build()))
                .build();

        Ticker ticker = Ticker.systemTicker();
        this.cacheMissing = config.isCacheMissing();
        this.schemaNamesCache = buildCache(ticker, config.getCacheMaximumSize(), config.getSchemaNamesCacheTtl());
        this.tableNamesCache = buildCache(ticker, config.getCacheMaximumSize(), config.getTableNamesCacheTtl());
        this.tableHandlesByNameCache = buildCache(ticker, config.getCacheMaximumSize(), config.getMetadataCacheTtl());
        this.tableHandlesByQueryCache = buildCache(ticker, config.getCacheMaximumSize(), config.getMetadataCacheTtl());
        this.procedureHandlesByQueryCache = buildCache(ticker, config.getCacheMaximumSize(), config.getMetadataCacheTtl());
        this.columnsCache = buildCache(ticker, config.getCacheMaximumSize(), config.getMetadataCacheTtl());
        this.tableCommentsCache = buildCache(ticker, config.getCacheMaximumSize(), config.getMetadataCacheTtl());
        this.statisticsCache = buildCache(ticker, config.getCacheMaximumSize(), config.getStatisticsCacheTtl());
        this.tablePrimaryKeysCache = buildCache(ticker, config.getCacheMaximumSize(), config.getStatisticsCacheTtl());
        this.tablePropertiesCache = buildCache(ticker, config.getCacheMaximumSize(), config.getMetadataCacheTtl());

    }

    private static <K, V> Cache<K, V> buildCache(Ticker ticker, long cacheSize, Duration cachingTtl)
    {
        return EvictableCacheBuilder.newBuilder()
                .ticker(ticker)
                .maximumSize(cacheSize)
                .expireAfterWrite(cachingTtl.toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
    }

    private static <K, V> V get(Cache<K, V> cache, K key, Callable<V> loader)
    {
        try {
            return cache.get(key, loader);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new UncheckedExecutionException(e);
        }
    }

    public List<String> getSchemaNames(ConnectorSession session) {
        String sql = "SHOW DATABASES";
        return get(schemaNamesCache, session, () -> getResultSet(sql));

    }

    public List<String> getTableNames(String schemaName) {
        TableListingCacheKey key = new TableListingCacheKey(schemaName);
        String sql = "SHOW TABLES FROM " + schemaName;

        return get(tableNamesCache, key, () -> getResultSet(sql));
    }


    private List<String> getResultSet(String sql) {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        try (ClientPool.PooledConnection pooledConnection = clientPool.borrowPooledConnection(dorisConfig.getJdbcURL());
                Statement statement = pooledConnection.getConnection().createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                builder.add(resultSet.getString(1).toLowerCase());
            }
            return builder.build();
        }
        catch (Exception e) {
            log.error("get resultset failed");
            throw new RuntimeException(e);
        }
    }

//    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
//        DorisTableHandle dorisTableHandle = (DorisTableHandle) table;
//
//        return new ConnectorTableMetadata(
//                new SchemaTableName(dorisTableHandle.getSchemaName(), dorisTableHandle.getTableName()),
//                getColumns(session, dorisTableHandle.getSchemaName(), dorisTableHandle.getTableName()).stream()
//                        .map(DorisColumnHandle::getColumnMetadata)
//                        .collect(ImmutableList.toImmutableList()),
//                getTableProperties(session, dorisTableHandle),
//                dorisTableHandle.getTableComment());
//    }

    public Map<String, Object> getTableProperties(ConnectorSession session, DorisTableHandle tableHandle)
    {
        return get(tablePropertiesCache, tableHandle, () -> getProperties(tableHandle));
    }

    private Map<String, Object> getProperties(DorisTableHandle tableHandle) {
        String sql = format("SELECT PROPERTY_NAME, PROPERTY_VALUE FROM internal.information_schema.table_properties where TABLE_SCHEMA='%s' AND TABLE_NAME='%s'", tableHandle.getSchemaName(), tableHandle.getTableName());

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        try (ClientPool.PooledConnection pooledConnection = clientPool.borrowPooledConnection(dorisConfig.getJdbcURL());
                Statement statement = pooledConnection.getConnection().createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                builder.put(resultSet.getString("PROPERTY_NAME"), resultSet.getString("PROPERTY_VALUE"));
            }

            return builder.build();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public Optional<DorisTableHandle> getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        TableHandlesByNameCacheKey key = new TableHandlesByNameCacheKey(tableName);
        Optional<DorisTableHandle> cachedTableHandle = tableHandlesByNameCache.getIfPresent(key);
        //noinspection OptionalAssignedToNull
        if (cachedTableHandle != null) {
            if (cacheMissing) {
                return cachedTableHandle;
            }
            tableHandlesByNameCache.invalidate(key);
        }
        return get(tableHandlesByNameCache, key, () -> getTable(session, tableName));
    }

    private Optional<DorisTableHandle> getTable(ConnectorSession session, SchemaTableName tableName) {
        //        String sql = format("SHOW PARTITIONS FROM %s.%s.%s", "internal", schemaName, tableName);
//        String commentSql = format("SELECT TABLE_COMMENT FROM internal.information_schema.tables WHERE TABLE_CATALOG='%s' AND TABLE_SCHEMA='%s' AND TABLE_NAME='%s'", "internal", schemaName, tableName);
//
//        log.debug("show partition sql : %s", sql);
//        log.debug("comment sql : %s", commentSql);
//        String[] partitionKey = null;
//        String tableComment = null;
//
//        ResultSet resultSet = null;
//        try (ClientPool.PooledConnection pooledConnection = clientPool.borrowPooledConnection(dorisConfig.getJdbcURL());
//                Statement statement = pooledConnection.getConnection().createStatement()) {
//            resultSet = statement.executeQuery(sql);
//
//            while (resultSet.next()) {
//                partitionKey = resultSet.getString("PartitionKey").split(",");
//            }
//
//            resultSet = statement.executeQuery(commentSql);
//            while (resultSet.next()) {
//                tableComment = resultSet.getString("TABLE_COMMENT");
//            }
//        }
//        catch (Exception e) {
//            log.error("get resultset failed");
//            throw new RuntimeException(e);
//        }
//        finally {
//            if (resultSet != null) {
//                try {
//                    resultSet.close();
//                }
//                catch (SQLException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }

        return Optional.of(new DorisTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
//                ImmutableList.of(),
                getColumns(session, tableName.getSchemaName(), tableName.getTableName()),
                TupleDomain.all(),
//                Optional.of(Arrays.asList(partitionKey)),
                Optional.of(ImmutableList.of()),
                TupleDomain.all(),
//                Optional.of(Strings.nullToEmpty(tableComment)),
                Optional.of(""),
                0,
                Optional.empty(),
                Optional.empty()));
    }

    public List<DorisColumnHandle> getColumns(ConnectorSession session, String schemaName, String tableName)
    {
//        ColumnsCacheKey key = new ColumnsCacheKey(new SchemaTableName(schemaName, tableName));
//        List<DorisColumnHandle> columns = columnsCache.getIfPresent(key);
//        //noinspection OptionalAssignedToNull
//        if (columns != null) {
//            if (cacheMissing) {
//                return columns;
//            }
//            columnsCache.invalidate(key);
//        }
//        return get(columnsCache, key, () -> getDorisColumns(session, schemaName, tableName));

        List<DorisColumnHandle> columns = new ArrayList<>();

        try (ClientPool.PooledConnection pooledConnection = clientPool.borrowPooledConnection(dorisConfig.getJdbcURL());
                Statement statement = pooledConnection.getConnection().createStatement();
                ResultSet resultSet = statement.executeQuery(format("DESC internal.%s.%s", schemaName, tableName))) {

            while (resultSet.next()) {
                columns.add(new DorisColumnHandle(
                        resultSet.getString("Field"),
                        resultSet.getString("NULL").equals("YES") ? true : false,
                        TypeConverter.toTrinoType(resultSet.getString("Type")),
                        Optional.empty()
                ));
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return columns;
    }


    private List<DorisColumnHandle> getDorisColumns(ConnectorSession session, String schemaName, String tableName) {
        try (ClientPool.PooledConnection pooledConnection = clientPool.borrowPooledConnection(dorisConfig.getJdbcURL());
                ResultSet resultSet = pooledConnection.getConnection().getMetaData().getColumns(schemaName, null, tableName, null)) {
            Map<String, CaseSensitivity> caseSensitivityMapping = getCaseSensitivityForColumns(pooledConnection.getConnection(), new SchemaTableName(schemaName, tableName), new RemoteTableName(Optional.of("internal"), Optional.of(schemaName), tableName));
            int allColumns = 0;
            List<DorisColumnHandle> columns = new ArrayList<>();
            while (resultSet.next()) {
                // skip if table doesn't match expected
                if (!Objects.equals(new RemoteTableName(Optional.of(schemaName), Optional.empty(), tableName), getRemoteTable(resultSet))) {
                    continue;
                }
                allColumns++;
                String columnName = resultSet.getString("COLUMN_NAME");
                JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                        getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null")),
                        Optional.ofNullable(resultSet.getString("TYPE_NAME")),
                        getInteger(resultSet, "COLUMN_SIZE"),
                        getInteger(resultSet, "DECIMAL_DIGITS"),
                        Optional.empty(),
                        Optional.ofNullable(caseSensitivityMapping.get(columnName)));
                Optional<ColumnMapping> columnMapping = toColumnMapping(session, pooledConnection.getConnection(), typeHandle);
                log.debug("Mapping data type of '%s' column '%s': %s mapped to %s", schemaName, columnName, typeHandle, columnMapping);
                boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                // Note: some databases (e.g. SQL Server) do not return column remarks/comment here.
                Optional<String> comment = Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
                // skip unsupported column types
                columnMapping.ifPresent(mapping -> columns.add(DorisColumnHandle.builder()
                        .setColumnName(columnName)
//                        .setJdbcTypeHandle(typeHandle)
                        .setColumnType(mapping.getType())
                        .setNullable(nullable)
                        .setComment(comment)
                        .build()));
                if (columnMapping.isEmpty()) {
                    UnsupportedTypeHandling unsupportedTypeHandling = getUnsupportedTypeHandling(session);
                    verify(
                            unsupportedTypeHandling == IGNORE,
                            "Unsupported type handling is set to %s, but toColumnMapping() returned empty for %s",
                            unsupportedTypeHandling,
                            typeHandle);
                }
            }
            if (columns.isEmpty()) {
                // A table may have no supported columns. In rare cases (e.g. PostgreSQL) a table might have no columns at all.
                throw new TableNotFoundException(
                        new SchemaTableName(schemaName, tableName),
                        format("Table '%s' has no supported columns (all %s columns are not supported)", schemaName, allColumns));
            }
            return ImmutableList.copyOf(columns);
        }
        catch (Exception e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }




    public List<ConnectorSplit> getSplits(
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter) {
        DorisTableHandle tableHandle = (DorisTableHandle) connectorTableHandle;
        String sql = buildSql(session, tableHandle, dynamicFilter);

        int maxRetries = 3;
        int retryIntervalMs = 1000;

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                MediaType JSON = MediaType.parse("application/json; charset=utf-8");
                String requestBody = OBJECT_MAPPER.createObjectNode()
                        .put("sql", sql)
                        .toString();
                log.info("doris fe query plan sql is : %s", sql);
                log.info("request query plan url is : %s", format("http://%s/api/%s/%s/_query_plan", dorisConfig.getQueryPlanURL(), tableHandle.getSchemaName(), tableHandle.getTableName()));

                okhttp3.Request request = new okhttp3.Request.Builder()
                        .url(format("http://%s/api/%s/%s/_query_plan", dorisConfig.getQueryPlanURL(), tableHandle.getSchemaName(), tableHandle.getTableName()))
                        .header("Authorization", Credentials.basic(dorisConfig.getFeUser(), dorisConfig.getFePassword().orElse("")))
                        .post(RequestBody.create(requestBody, JSON))
                        .build();


                okhttp3.Response response = httpClient.newCall(request).execute();

                if (response.code() != 200) {
                    log.warn("Query plan request failed with status code: %d, attempt: %d/%d",
                             response.code(), attempt + 1, maxRetries);

                    if (attempt < maxRetries - 1) {
                        continue;
                    }
                    throw new TrinoException(JDBC_ERROR,
                        format("Failed to get query plan after %d attempts. Last status code: %d",
                               maxRetries, response.code()));
                }

                String responseBody = response.body().string();
                JsonNode rootNode = OBJECT_MAPPER.readTree(responseBody);
                JsonNode dataNode = rootNode.path("data");

                QueryPlanData queryPlanData = OBJECT_MAPPER.treeToValue(dataNode, QueryPlanData.class);

                // Convert partitions to Map<String, List<Integer>> (BE IP:PORT -> Tablet IDs) using Stream API
                Map<HostAddress, Set<Long>> beToTablets = buildBeToTabletsMap(queryPlanData.getPartitions());

                return ImmutableList.of(new DorisSplit(queryPlanData.getOpaqued_query_plan(), beToTablets));
            }
            catch (Exception e) {
                log.error("Failed to get query plan, attempt: %d/%d", attempt + 1, maxRetries, e);

                if (attempt < maxRetries - 1) {
                    continue;
                }
                throw new TrinoException(JDBC_ERROR,
                    format("Failed to get query plan after %d attempts: %s", maxRetries, e.getMessage()), e);
            }
        }

        return ImmutableList.of();
    }

    private static String buildSql(
            ConnectorSession session,
            DorisTableHandle tableHandle,
            DynamicFilter dynamicFilter) {
        StringBuilder sql = new StringBuilder("SELECT ");
        // 构建要查找的列
        ImmutableList<String> columnNames = tableHandle.getColumns().stream()
                .map(DorisColumnHandle::getColumnName)
                .collect(toImmutableList());
        sql.append(String.join(", ", columnNames));
        sql.append(format(" FROM internal.%s.%s", tableHandle.getSchemaName(), tableHandle.getTableName()));

        // 构建谓词
        ImmutableList.Builder<String> conjuncts = ImmutableList.builder();
        TupleDomain<ColumnHandle> currentPredicate = dynamicFilter.getCurrentPredicate();

        if (!currentPredicate.isNone() && !currentPredicate.isAll()) {
            Optional<Map<ColumnHandle, Domain>> domains = currentPredicate.getDomains();

            for (Map.Entry<ColumnHandle, Domain> entrySet : domains.get().entrySet()) {
                DorisColumnHandle columnHandle = (DorisColumnHandle) entrySet.getKey();
                ValueSet valueSet = entrySet.getValue().getValues();
                if (valueSet instanceof EquatableValueSet eq) {
                    ImmutableList<String> values = eq.getEntries().stream()
                            .map(entry -> eq.getType().getObjectValue(session, entry.getBlock(), 0).toString())
                            .collect(toImmutableList());
                    if (values.size() == 1) {
                        conjuncts.add(format("%s = %s", columnHandle.getColumnName(), values.get(0)));
                    } else {
                        conjuncts.add(format("%s in (%s)", columnHandle.getColumnName(), Joiner.on(",").join(values)));
                    }
                }

                if (valueSet instanceof SortedRangeSet sortedRangeSet) {
//                    List<String> disjuncts = new ArrayList<>();
//                    List<Object> singleValues = new ArrayList<>();
                    for (Range range : sortedRangeSet.getRanges().getOrderedRanges()) {
                        checkState(!range.isAll()); // Already checked
                        if (range.isSingleValue()) {
                            conjuncts.add(format("%s = %s", columnHandle.getColumnName(), range.getSingleValue()));
                        }
                        else {
                            List<String> rangeConjuncts = new ArrayList<>();
                            if (!range.isLowUnbounded()) {
                                String operator = range.isLowInclusive() ? ">=" : ">";
                                conjuncts.add(format("%s %s %s", columnHandle.getColumnName(), operator, range.getLowBoundedValue()));
                            }
                            if (!range.isHighUnbounded()) {
                                String operator = range.isHighInclusive() ? "<=" : "<";
                                conjuncts.add(format("%s %s %s", columnHandle.getColumnName(), operator, range.getHighBoundedValue()));
                            }
//                            // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
//                            checkState(!rangeConjuncts.isEmpty());
//                            if (rangeConjuncts.size() == 1) {
//                                disjuncts.add(getOnlyElement(rangeConjuncts));
//                            }
//                            else {
//                                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
//                            }
                        }
                    }
                }
            }
        }

        String whereClause = Joiner.on(" AND ").join(conjuncts.build());
        // todo lack of limit
        if (!Strings.isNullOrEmpty(whereClause)) {
            sql.append(format(" WHERE %s", whereClause));
        }

        String ans = sql.toString();
        log.info("look at send to fe sql is : %s", ans);

        return ans;
    }

    private static Optional<Integer> getInteger(ResultSet resultSet, String columnLabel)
            throws SQLException
    {
        int value = resultSet.getInt(columnLabel);
        if (resultSet.wasNull()) {
            return Optional.empty();
        }
        return Optional.of(value);
    }

    Map<String, CaseSensitivity> getCaseSensitivityForColumns(Connection connection, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
    {
        String sql = format("SELECT * FROM %s.%s.%s", "internal", remoteTableName.getSchemaName().get(), remoteTableName.getTableName());
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            ResultSetMetaData metadata = preparedStatement.getMetaData();
            ImmutableMap.Builder<String, CaseSensitivity> columns = ImmutableMap.builder();
            for (int column = 1; column <= metadata.getColumnCount(); column++) {
                String name = metadata.getColumnName(column);
                columns.put(name, metadata.isCaseSensitive(column) ? CASE_SENSITIVE : CASE_INSENSITIVE);
            }
            return columns.buildOrThrow();
        }
        catch (SQLException e) {
            if (e.getErrorCode() == ER_NO_SUCH_TABLE) {
                throw new TableNotFoundException(schemaTableName);
            }
            throw new TrinoException(JDBC_ERROR, "Failed to get case sensitivity for columns. " + firstNonNull(e.getMessage(), e), e);
        }
    }

    private static RemoteTableName getRemoteTable(ResultSet resultSet)
            throws SQLException
    {
        return new RemoteTableName(
                Optional.ofNullable(resultSet.getString("TABLE_CAT")),
                Optional.ofNullable(resultSet.getString("TABLE_SCHEM")),
                resultSet.getString("TABLE_NAME"));
    }

    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (jdbcTypeName.toLowerCase(ENGLISH)) {
            case "tinyint unsigned":
                return Optional.of(smallintColumnMapping());
            case "smallint unsigned":
                return Optional.of(integerColumnMapping());
            case "int unsigned":
                return Optional.of(bigintColumnMapping());
            case "bigint unsigned":
                return Optional.of(decimalColumnMapping(createDecimalType(20)));
            case "json":
                return Optional.of(jsonColumnMapping());
            case "enum":
                return Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), false));
            case "datetime":
                return mysqlDateTimeToTrinoTimestamp(typeHandle);
        }

        switch (typeHandle.jdbcType()) {
            case Types.BIT:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                // Disable pushdown because floating-point values are approximate and not stored as exact values,
                // attempts to treat them as exact in comparisons may lead to problems
                return Optional.of(ColumnMapping.longMapping(
                        REAL,
                        (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                        realWriteFunction(),
                        DISABLE_PUSHDOWN));

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = typeHandle.decimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
                int precision = typeHandle.requiredColumnSize();
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
                // TODO does mysql support negative scale?
                precision = precision + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
                return Optional.of(mySqlDefaultCharColumnMapping(typeHandle.requiredColumnSize(), typeHandle.caseSensitivity()));

            // TODO not all these type constants are necessarily used by the JDBC driver
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(mySqlDefaultVarcharColumnMapping(typeHandle.requiredColumnSize(), typeHandle.caseSensitivity()));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(ColumnMapping.sliceMapping(VARBINARY, varbinaryReadFunction(), varbinaryWriteFunction(), FULL_PUSHDOWN));

            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        dateReadFunctionUsingLocalDate(),
                        mySqlDateWriteFunctionUsingLocalDate()));

            case Types.TIME:
                TimeType timeType = createTimeType(getTimePrecision(typeHandle.requiredColumnSize()));
                requireNonNull(timeType, "timeType is null");
                checkArgument(timeType.getPrecision() <= 9, "Unsupported type precision: %s", timeType);
                return Optional.of(ColumnMapping.longMapping(
                        timeType,
                        mySqlTimeReadFunction(timeType),
                        timeWriteFunction(timeType.getPrecision())));

            case Types.TIMESTAMP:
                return mysqlTimestampToTrinoTimestampWithTz(typeHandle);
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    protected static Optional<ColumnMapping> mapToUnboundedVarchar(JdbcTypeHandle typeHandle)
    {
        VarcharType unboundedVarcharType = createUnboundedVarcharType();
        return Optional.of(ColumnMapping.sliceMapping(
                unboundedVarcharType,
                varcharReadFunction(unboundedVarcharType),
                (statement, index, value) -> {
                    throw new TrinoException(
                            NOT_SUPPORTED,
                            "Underlying type that is mapped to VARCHAR is not supported for INSERT: " + typeHandle.jdbcTypeName().get());
                },
                DISABLE_PUSHDOWN));
    }


    private static Optional<ColumnMapping> mysqlTimestampToTrinoTimestampWithTz(JdbcTypeHandle typeHandle)
    {
        TimestampWithTimeZoneType trinoType = createTimestampWithTimeZoneType(getTimestampPrecision(typeHandle.requiredColumnSize()));
        if (trinoType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return Optional.of(ColumnMapping.longMapping(
                    trinoType,
                    shortTimestampWithTimeZoneReadFunction(),
                    shortTimestampWithTimeZoneWriteFunction()));
        }
        return Optional.of(ColumnMapping.objectMapping(
                trinoType,
                longTimestampWithTimeZoneReadFunction(),
                longTimestampWithTimeZoneWriteFunction()));
    }

    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            Timestamp timestamp = resultSet.getTimestamp(columnIndex);
            long millisUtc = timestamp.getTime();
            return packDateTimeWithZone(millisUtc, UTC_KEY);
        };
    }

    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(
                LongTimestampWithTimeZone.class,
                (resultSet, columnIndex) -> {
                    OffsetDateTime offsetDateTime = resultSet.getObject(columnIndex, OffsetDateTime.class);
                    return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                            offsetDateTime.toEpochSecond(),
                            (long) offsetDateTime.getNano() * PICOSECONDS_PER_NANOSECOND,
                            UTC_KEY);
                });
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            Instant instantValue = Instant.ofEpochMilli(unpackMillisUtc(value));
            statement.setObject(index, instantValue);
        };
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(
                LongTimestampWithTimeZone.class,
                (statement, index, value) -> {
                    long epochSeconds = floorDiv(value.getEpochMillis(), MILLISECONDS_PER_SECOND);
                    long nanosOfSecond = (long) floorMod(value.getEpochMillis(), MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
                    Instant instantValue = Instant.ofEpochSecond(epochSeconds, nanosOfSecond);
                    statement.setObject(index, instantValue);
                });
    }

    private static LongReadFunction mySqlTimeReadFunction(TimeType timeType)
    {
        return new LongReadFunction()
        {
            private final LongReadFunction delegate = timeReadFunction(timeType);

            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                // super calls ResultSet#getObject(), which for TIME type returns java.sql.Time, for which the conversion can fail if the value isn't a valid instant in server's time zone.
                resultSet.getObject(columnIndex, String.class);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                return delegate.readLong(resultSet, columnIndex);
            }
        };
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static int getTimePrecision(int timeColumnSize)
    {
        if (timeColumnSize == ZERO_PRECISION_TIME_COLUMN_SIZE) {
            return 0;
        }
        int timePrecision = timeColumnSize - ZERO_PRECISION_TIME_COLUMN_SIZE - 1;
        verify(1 <= timePrecision && timePrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Unexpected time precision %s calculated from time column size %s", timePrecision, timeColumnSize);
        return timePrecision;
    }

    private LongWriteFunction mySqlDateWriteFunctionUsingLocalDate()
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return "CAST(? AS DATE)";
            }

            @Override
            public void set(PreparedStatement statement, int index, long epochDay)
                    throws SQLException
            {
                statement.setString(index, LocalDate.ofEpochDay(epochDay).format(ISO_DATE));
            }
        };
    }

    private static ColumnMapping mySqlDefaultCharColumnMapping(int columnSize, Optional<CaseSensitivity> caseSensitivity)
    {
        if (columnSize > CharType.MAX_LENGTH) {
            return mySqlDefaultVarcharColumnMapping(columnSize, caseSensitivity);
        }
        return mySqlCharColumnMapping(createCharType(columnSize), caseSensitivity);
    }

    private static ColumnMapping mySqlCharColumnMapping(CharType charType, Optional<CaseSensitivity> caseSensitivity)
    {
        requireNonNull(charType, "charType is null");
        PredicatePushdownController pushdownController = caseSensitivity.orElse(CASE_INSENSITIVE) == CASE_SENSITIVE
                ? MYSQL_CHARACTER_PUSHDOWN
                : CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
        return ColumnMapping.sliceMapping(charType, charReadFunction(charType), charWriteFunction(), pushdownController);
    }

    private static ColumnMapping mySqlDefaultVarcharColumnMapping(int columnSize, Optional<CaseSensitivity> caseSensitivity)
    {
        if (columnSize > VarcharType.MAX_LENGTH) {
            return mySqlVarcharColumnMapping(createUnboundedVarcharType(), caseSensitivity);
        }
        return mySqlVarcharColumnMapping(createVarcharType(columnSize), caseSensitivity);
    }

    private static ColumnMapping mySqlVarcharColumnMapping(VarcharType varcharType, Optional<CaseSensitivity> caseSensitivity)
    {
        PredicatePushdownController pushdownController = caseSensitivity.orElse(CASE_INSENSITIVE) == CASE_SENSITIVE
                ? MYSQL_CHARACTER_PUSHDOWN
                : CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
        return ColumnMapping.sliceMapping(varcharType, varcharReadFunction(varcharType), varcharWriteFunction(), pushdownController);
    }

    private Optional<ColumnMapping> mysqlDateTimeToTrinoTimestamp(JdbcTypeHandle typeHandle)
    {
        TimestampType timestampType = createTimestampType(getTimestampPrecision(typeHandle.requiredColumnSize()));
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return Optional.of(ColumnMapping.longMapping(
                timestampType,
                mySqlTimestampReadFunction(timestampType),
                timestampWriteFunction(timestampType)));
    }

    private static LongReadFunction mySqlTimestampReadFunction(TimestampType timestampType)
    {
        return new LongReadFunction()
        {
            private final LongReadFunction delegate = timestampReadFunction(timestampType);

            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                // super calls ResultSet#getObject(), which for TIMESTAMP type returns java.sql.Timestamp, for which the conversion can fail if the value isn't a valid instant in server's time zone.
                resultSet.getObject(columnIndex, LocalDateTime.class);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                return delegate.readLong(resultSet, columnIndex);
            }
        };
    }

    private static int getTimestampPrecision(int timestampColumnSize)
    {
        if (timestampColumnSize == ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE) {
            return 0;
        }
        int timestampPrecision = timestampColumnSize - ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE - 1;
        verify(1 <= timestampPrecision && timestampPrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Unexpected timestamp precision %s calculated from timestamp column size %s", timestampPrecision, timestampColumnSize);
        return timestampPrecision;
    }


    private Optional<ColumnMapping> getForcedMappingToVarchar(JdbcTypeHandle typeHandle)
    {
        if (typeHandle.jdbcTypeName().isPresent() && jdbcTypesMappedToVarchar.contains(typeHandle.jdbcTypeName().get())) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private Map<HostAddress, Set<Long>> buildBeToTabletsMap(Map<String, PartitionInfo> partitions)
    {
        Map<HostAddress, Set<Long>> builder = Maps.newHashMap();
        Map<String, HostAddress> addressCache = Maps.newHashMap();

        partitions.entrySet().forEach(entry -> {
            String tabletId = entry.getKey();
            entry.getValue().getRoutings().stream()
                    .forEach(routing -> {
                        addressCache.computeIfAbsent(routing, r -> {
                            String[] splits = r.split(":");
//                            HostAddress hostAddress = HostAddress.fromParts(splits[0], Integer.valueOf(splits[1]));
                            HostAddress hostAddress = HostAddress.fromParts("127.0.0.1", Integer.valueOf(splits[1]));
                            return hostAddress;
                        });
                        builder.computeIfAbsent(addressCache.get(routing), k -> new HashSet<>()).add(Long.valueOf(tabletId));
                    });
        });

        return builder;
    }


    private record ColumnsCacheKey( SchemaTableName table)
    {
        private ColumnsCacheKey
        {
//            sessionProperties = ImmutableMap.copyOf(requireNonNull(sessionProperties, "sessionProperties is null"));
            requireNonNull(table, "table is null");
        }
    }

    private record TableHandlesByNameCacheKey(SchemaTableName tableName)
    {
        private TableHandlesByNameCacheKey
        {
            requireNonNull(tableName, "tableName is null");
        }
    }

    private record TableHandlesByQueryCacheKey(IdentityCacheMapping.IdentityCacheKey identity, PreparedQuery preparedQuery)
    {
        private TableHandlesByQueryCacheKey
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(preparedQuery, "preparedQuery is null");
        }
    }

    private record ProcedureHandlesByQueryCacheKey(IdentityCacheMapping.IdentityCacheKey identity, JdbcProcedureHandle.ProcedureQuery procedureQuery)
    {
        private ProcedureHandlesByQueryCacheKey
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(procedureQuery, "procedureQuery is null");
        }
    }

    private record TableListingCacheKey(String schemaName)
    {
        private TableListingCacheKey
        {
            requireNonNull(schemaName, "schemaName is null");
        }
    }

    private record SchemaListingCacheKey(String schemaName)
    {
        private SchemaListingCacheKey
        {
            requireNonNull(schemaName, "schemaName is null");
        }
    }

}
