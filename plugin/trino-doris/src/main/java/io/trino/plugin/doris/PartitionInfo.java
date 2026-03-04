package io.trino.plugin.doris;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PartitionInfo {
    private final List<String> routings;
    private final int version;
    private final long schemaHash;

    @JsonCreator
    public PartitionInfo(
            @JsonProperty("routings") List<String> routings,
            @JsonProperty("version") int version,
            @JsonProperty("schemaHash") long schemaHash)
    {
        this.routings = requireNonNull(routings, "routings is null");
        this.version = version;
        this.schemaHash = schemaHash;
    }

    @JsonProperty
    public List<String> getRoutings()
    {
        return routings;
    }

    @JsonProperty
    public int getVersion()
    {
        return version;
    }

    @JsonProperty
    public long getSchemaHash()
    {
        return schemaHash;
    }
}
