package io.trino.plugin.doris;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class DorisTransactionHandle
        implements ConnectorTransactionHandle
{
    private final boolean autoCommit;
    private final UUID uuid;

    public DorisTransactionHandle(boolean autoCommit)
    {
        this(autoCommit, UUID.randomUUID());
    }

    @JsonCreator
    public DorisTransactionHandle(@JsonProperty("autoCommit") boolean autoCommit, @JsonProperty("uuid") UUID uuid)
    {
        this.autoCommit = autoCommit;
        this.uuid = requireNonNull(uuid, "uuid is null");
    }

    @JsonProperty
    public boolean isAutoCommit()
    {
        return autoCommit;
    }

    @JsonProperty
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisTransactionHandle that = (DorisTransactionHandle) o;
        return autoCommit == that.autoCommit && Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(autoCommit, uuid);
    }

    @Override
    public String toString()
    {
        return uuid.toString();
    }
}
