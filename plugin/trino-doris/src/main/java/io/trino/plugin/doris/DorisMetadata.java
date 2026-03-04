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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
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
import io.trino.spi.connector.ConnectorTableSchema;
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class DorisMetadata implements ConnectorMetadata
{
    private final DorisFeClient feClient;
    private final boolean precalculateStatisticsForPushdown;

    public DorisMetadata(DorisFeClient feClient)
    {
        this.feClient = requireNonNull(feClient, "feClient is null");
        this.precalculateStatisticsForPushdown = true;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(feClient.getSchemaNames(session));
    }

    @Override
    public DorisTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        return feClient.getTableHandle(session, tableName).orElse(null);
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        DorisTableHandle dorisTableHandle = (DorisTableHandle) table;
         return new SchemaTableName(dorisTableHandle.getSchemaName(), dorisTableHandle.getTableName());
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle table)
    {
        DorisTableHandle handle = (DorisTableHandle) table;
        return new ConnectorTableSchema(
                new SchemaTableName(handle.getSchemaName(), handle.getTableName()),
                handle.getColumns().stream().map(DorisColumnHandle::getColumnSchema).collect(toImmutableList()));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        DorisTableHandle dorisTableHandle = (DorisTableHandle) table;

        return new ConnectorTableMetadata(
                new SchemaTableName(dorisTableHandle.getSchemaName(), dorisTableHandle.getTableName()),
                dorisTableHandle.getColumns().stream().map(DorisColumnHandle::getColumnMetadata).collect(toImmutableList()),
                feClient.getTableProperties(session, dorisTableHandle),
                dorisTableHandle.getTableComment());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (!schemaName.isPresent()) {
            return Collections.emptyList();
        }

        return feClient.getTableNames(schemaName.get()).stream()
                .map(tableName -> new SchemaTableName(schemaName.get(), tableName))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DorisTableHandle dorisTableHandle = (DorisTableHandle) tableHandle;

        return dorisTableHandle.getColumns().stream()
                .collect(ImmutableMap.toImmutableMap(column -> column.getColumnName(), Function.identity()));

//        return feClient.getColumns(session, dorisTableHandle.getSchemaName(), dorisTableHandle.getTableName()).stream()
//                .collect(ImmutableMap.toImmutableMap(column -> column.getColumnName(), Function.identity()));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        prefix.toOptionalSchemaTableName().ifPresent(tableName -> feClient.getTableHandle(session, tableName)
                    .ifPresent(tableHandle -> columns.put(tableName, tableHandle.getColumns().stream().map(DorisColumnHandle::getColumnMetadata).toList())));

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
