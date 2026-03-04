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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class DorisSplit implements ConnectorSplit {
    private static final int INSTANCE_SIZE = instanceSize(DorisSplit.class);

//    private final String uri;
    private final boolean remotelyAccessible;
//    private final List<HostAddress> addresses;
    private final String opaquedQueryPlan;
    private final Map<HostAddress, Set<Long>> beToTablets;

    @JsonCreator
    public DorisSplit(
            String opaquedQueryPlan,
            Map<HostAddress, Set<Long>> beToTablets)
    {
        this.opaquedQueryPlan = requireNonNull(opaquedQueryPlan, "opaquedQueryPlan is null");
        this.beToTablets = requireNonNull(beToTablets, "beToTablets is null");
//        this.uri = "";
        this.remotelyAccessible = true;
//        this.addresses = ImmutableList.of();
    }

//    @JsonProperty
//    public String getUri()
//    {
//        return uri;
//    }

    @JsonProperty
    public String getOpaquedQueryPlan()
    {
        return opaquedQueryPlan;
    }

    @JsonProperty
    public Map<HostAddress, Set<Long>> getBeToTablets()
    {
        return beToTablets;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        // only http or https is remotely accessible
        return remotelyAccessible;
    }

//    @Override
//    public List<HostAddress> getAddresses()
//    {
//        return addresses;
//    }

//    @Override
//    public Map<String, String> getSplitInfo()
//    {
//        return ImmutableMap.of("addresses", addresses.stream().map(HostAddress::toString).collect(joining(",")), "remotelyAccessible", String.valueOf(remotelyAccessible));
//    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long beToTabletsSize = beToTablets.entrySet().stream()
                .mapToLong(entry -> sizeOf(entry.getKey().getRetainedSizeInBytes()) + estimatedSizeOf(entry.getValue(), i -> Integer.BYTES))
                .sum();
        return INSTANCE_SIZE
//                + estimatedSizeOf(uri)
                + estimatedSizeOf(opaquedQueryPlan)
//                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes)
                + beToTabletsSize;
    }
}
