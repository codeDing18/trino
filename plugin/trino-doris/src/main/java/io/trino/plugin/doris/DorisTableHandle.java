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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DorisTableHandle implements ConnectorTableHandle {
    private final String schemaName;
    private final String tableName;
    private final List<DorisColumnHandle> columns;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<List<String>> partitionNames;
    private final TupleDomain<DorisColumnHandle> compactEffectivePredicate;
    private final Optional<String> tableComment;
    private final int nextSyntheticColumnId;
    private final Optional<Set<SchemaTableName>> allReferencedTables;
    private final Optional<String> authorization;

    @JsonCreator
    public DorisTableHandle(
            @JsonProperty String schemaName,
            @JsonProperty String tableName,
            @JsonProperty List<DorisColumnHandle> columns,
            @JsonProperty TupleDomain<ColumnHandle> constraint,
            @JsonProperty Optional<List<String>> partitionNames,
            @JsonProperty TupleDomain<DorisColumnHandle> compactEffectivePredicate,
            @JsonProperty Optional<String> tableComment,
            @JsonProperty(defaultValue = "0") int nextSyntheticColumnId,
            @JsonProperty Optional<Set<SchemaTableName>> allReferencedTables,
            @JsonProperty Optional<String> authorization) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = columns;
        this.constraint = constraint;
        this.partitionNames = partitionNames;
        this.compactEffectivePredicate = compactEffectivePredicate;
        this.tableComment = tableComment;
        this.nextSyntheticColumnId = nextSyntheticColumnId;
        this.allReferencedTables = allReferencedTables;
        this.authorization = authorization;
    }

    @JsonProperty
    public Optional<String> getTableComment()
    {
        return tableComment;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<DorisColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<List<String>> getPartitionNames()
    {
        return partitionNames;
    }

    @JsonProperty
    public TupleDomain<DorisColumnHandle> getCompactEffectivePredicate()
    {
        return compactEffectivePredicate;
    }

    // do not serialize constraint columns as they are not needed on workers
    @JsonIgnore
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonIgnore
    public int getNextSyntheticColumnId()
    {
        return nextSyntheticColumnId;
    }

    @JsonIgnore
    public Optional<Set<SchemaTableName>> getAllReferencedTables()
    {
        return allReferencedTables;
    }

    @JsonIgnore
    public Optional<String> getAuthorization()
    {
        return authorization;
    }
}
