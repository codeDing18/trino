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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class QueryPlanData {
    // tablet ID -> PartitionInfo
    private final Map<String, PartitionInfo> partitions;
    private final String opaqued_query_plan;
    private final int status;

    @JsonCreator
    public QueryPlanData(
            @JsonProperty("partitions") Map<String, PartitionInfo> partitions,
            @JsonProperty("opaqued_query_plan") String opaqued_query_plan,
            @JsonProperty("status") int status)
    {
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.opaqued_query_plan = requireNonNull(opaqued_query_plan, "opaqued_query_plan is null");
        this.status = status;
    }

    @JsonProperty
    public Map<String, PartitionInfo> getPartitions()
    {
        return partitions;
    }

    @JsonProperty
    public String getOpaqued_query_plan()
    {
        return opaqued_query_plan;
    }

    @JsonProperty
    public int getStatus()
    {
        return status;
    }
}
