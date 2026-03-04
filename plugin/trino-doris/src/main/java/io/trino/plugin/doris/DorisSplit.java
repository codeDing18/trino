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

    private final String uri;
    private final boolean remotelyAccessible;
    private final List<HostAddress> addresses;
    private final String opaquedQueryPlan;
    private final Map<String, Set<Long>> beToTablets;

    @JsonCreator
    public DorisSplit(@JsonProperty("uri") String uri)
    {
        this.uri = requireNonNull(uri, "uri is null");

        remotelyAccessible = true;
        addresses = ImmutableList.of(HostAddress.fromUri(URI.create(uri)));
        this.opaquedQueryPlan = "";
        this.beToTablets = ImmutableMap.of();
    }

    @JsonCreator
    public DorisSplit(
            String opaquedQueryPlan,
            Map<String, Set<Long>> beToTablets)
    {
        this.opaquedQueryPlan = requireNonNull(opaquedQueryPlan, "opaquedQueryPlan is null");
        this.beToTablets = requireNonNull(beToTablets, "beToTablets is null");
        this.uri = "";
        this.remotelyAccessible = true;
        this.addresses = ImmutableList.of();
    }

    @JsonProperty
    public String getUri()
    {
        return uri;
    }

    @JsonProperty
    public String getOpaquedQueryPlan()
    {
        return opaquedQueryPlan;
    }

    @JsonProperty
    public Map<String, Set<Long>> getBeToTablets()
    {
        return beToTablets;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        // only http or https is remotely accessible
        return remotelyAccessible;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return ImmutableMap.of("addresses", addresses.stream().map(HostAddress::toString).collect(joining(",")), "remotelyAccessible", String.valueOf(remotelyAccessible));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long beToTabletsSize = beToTablets.entrySet().stream()
                .mapToLong(entry -> sizeOf(entry.getKey().getBytes()) + estimatedSizeOf(entry.getValue(), i -> Integer.BYTES))
                .sum();
        return INSTANCE_SIZE
                + estimatedSizeOf(uri)
                + estimatedSizeOf(opaquedQueryPlan)
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes)
                + beToTabletsSize;
    }
}
