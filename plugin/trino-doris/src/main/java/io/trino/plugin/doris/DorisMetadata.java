package io.trino.plugin.doris;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.AccessDeniedException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class DorisMetadata implements ConnectorMetadata
{
    private final DorisClient dorisClient;
    private final boolean precalculateStatisticsForPushdown;


    @Inject
    public DorisMetadata(DorisClient DorisClient)
    {
        this.dorisClient = requireNonNull(DorisClient, "DorisClient is null");
        this.precalculateStatisticsForPushdown = true;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(dorisClient.getFeClient().getSchemaNames());
    }

    @Override
    public DorisTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        return dorisClient.getFeClient().getTableHandle(session, tableName.getSchemaName(), tableName.getTableName()).orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return dorisClient.getFeClient().getTableMetadata(session, table);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(dorisClient.getFeClient().getSchemaNames()));

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : dorisClient.getFeClient().getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DorisTableHandle dorisTableHandle = (DorisTableHandle) tableHandle;
        return dorisTableHandle.getColumns().stream()
                .collect(ImmutableMap.toImmutableMap(column -> column.getColumnName(), Function.identity()));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tables = prefix.toOptionalSchemaTableName()
                .<List<SchemaTableName>>map(ImmutableList::of)
                .orElseGet(() -> listTables(session, prefix.getSchema()));
        for (SchemaTableName tableName : tables) {
            try {
                dorisClient.getFeClient().getTableHandle(session, tableName.getSchemaName(), tableName.getTableName())
                        .ifPresent(tableHandle -> columns.put(tableName, getColumnMetadata(session, tableHandle)));
            }
            catch (TableNotFoundException | AccessDeniedException e) {
                // table disappeared during listing operation or user is not allowed to access it
                // these exceptions are ignored because listTableColumns is used for metadata queries (SELECT FROM information_schema)
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isNone()) {
            return Optional.empty();
        }

        DorisTableHandle dorisTableHandle = (DorisTableHandle) handle;

        TupleDomain<ColumnHandle> oldDomain = dorisTableHandle.getConstraint();
        // 这种情况没必要下推谓词了，因为dorisTableHandle中已经有了
        if (oldDomain.contains(summary) && !oldDomain.isAll()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> newDoamin = oldDomain.intersect(summary);

        return Optional.of(new ConstraintApplicationResult<>(
                new DorisTableHandle(
                        dorisTableHandle.getSchemaName(),
                        dorisTableHandle.getTableName(),
                        dorisTableHandle.getColumns(),
                        newDoamin,
                        dorisTableHandle.getPartitionNames(),
                        dorisTableHandle.getCompactEffectivePredicate(),
                        dorisTableHandle.getTableComment(),
                        dorisTableHandle.getNextSyntheticColumnId(),
                        dorisTableHandle.getAllReferencedTables(),
                        dorisTableHandle.getAuthorization()),
                summary,
                constraint.getExpression(),
                precalculateStatisticsForPushdown));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        // todo push down projections
        DorisTableHandle tableHandle = (DorisTableHandle) handle;
        ImmutableList<DorisColumnHandle> newColumns = assignments.values().stream()
                .map(DorisColumnHandle.class::cast)
                .collect(toImmutableList());

        return Optional.of(new ProjectionApplicationResult<>(
                new DorisTableHandle(
                        tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        newColumns,
                        tableHandle.getConstraint(),
                        tableHandle.getPartitionNames(),
                        tableHandle.getCompactEffectivePredicate(),
                        tableHandle.getTableComment(),
                        tableHandle.getNextSyntheticColumnId(),
                        tableHandle.getAllReferencedTables(),
                        tableHandle.getAuthorization()),
                projections,
                assignments.entrySet().stream()
                        .map(entry -> new Assignment(
                                entry.getKey(),
                                entry.getValue(),
                                ((DorisColumnHandle) entry.getValue()).getColumnType()
                        ))
                        .collect(toImmutableList()),
                precalculateStatisticsForPushdown));
    }

    public List<ColumnMetadata> getColumnMetadata(ConnectorSession session, DorisTableHandle tableHandle)
    {
        return getColumnHandles(session, tableHandle).values()
                .stream()
                .map(JdbcColumnHandle.class::cast)
                .map(JdbcColumnHandle::getColumnMetadata)
                .collect(toImmutableList());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((DorisColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        // For now, we'll implement a simplified version that leverages BaseJdbcClient
        // This will be enhanced later to support Doris-specific features
        
        // Validate inputs
        if (aggregates == null || aggregates.isEmpty()) {
            // No aggregate functions, no need to push down
            return Optional.empty();
        }
        
        if (groupingSets == null || groupingSets.isEmpty()) {
            // No grouping sets provided, which is invalid for aggregation
            return Optional.empty();
        }
        
        // Global aggregation (with no grouping sets) is represented by [[]]
        if (groupingSets.equals(List.of(List.of())) && aggregates.isEmpty()) {
            // Global aggregation with no aggregate functions is not sensible
            return Optional.empty();
        }
        
        // Check if Doris FE client supports aggregation pushdown
        // Note: DorisFeClient extends BaseJdbcClient which has default implementation
        // We'll rely on that for now
        
        // Create a simplified result that indicates aggregation could be pushed down
        // This is a placeholder until we fully integrate with BaseJdbcClient
        
        return Optional.empty();
    }

    @Override
    public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(
            ConnectorSession session,
            JoinType joinType,
            ConnectorTableHandle left,
            ConnectorTableHandle right,
            ConnectorExpression joinCondition,
            Map<String, ColumnHandle> leftAssignments,
            Map<String, ColumnHandle> rightAssignments,
            JoinStatistics statistics)
    {
        // For now, we'll implement a simplified version
        // This will be enhanced later to support Doris-specific features
        
        // Validate inputs
        if (joinCondition == null) {
            // No join condition, cannot push down
            return Optional.empty();
        }
        
        // Check if the join type is supported
        // Doris supports INNER, LEFT, RIGHT, and FULL joins
        // (similar to MySQL)
        
        // Create a simplified result that indicates join could be pushed down
        // This is a placeholder until we fully integrate with BaseJdbcClient
        
        return Optional.empty();
    }
}
