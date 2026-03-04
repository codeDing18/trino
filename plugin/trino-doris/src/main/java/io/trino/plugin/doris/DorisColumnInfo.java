package io.trino.plugin.doris;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DorisColumnInfo
{
    private final List<DorisNativeColumn> properties;
    private final int status;

    @JsonCreator
    public DorisColumnInfo(
            @JsonProperty("properties") List<DorisNativeColumn> properties,
            @JsonProperty("status") int status)
    {
        this.properties = requireNonNull(properties, "partitions is null");
        this.status = status;
    }

    @JsonProperty
    public List<DorisNativeColumn> getProperties()
    {
        return properties;
    }

    @JsonProperty
    public int getStatus()
    {
        return status;
    }


    public class DorisNativeColumn {
        private final String columnName;
        private final boolean nullable;
        private final String columnType;
        private final Optional<String> comment;
        private final Optional<Integer> decimalPrecision;
        private final Optional<Integer> decimalScale;

        @JsonCreator
        public DorisNativeColumn(
                @JsonProperty("name") String columnName,
                @JsonProperty("is_nullable") boolean nullable,
                @JsonProperty("type") String columnType,
                @JsonProperty("comment") Optional<String> comment,
                @JsonProperty("precision") Optional<Integer> decimalPrecision,
                @JsonProperty("scale") Optional<Integer> decimalScale)
        {
            this.columnName = columnName;
            this.columnType = columnType;
            this.nullable = nullable;
            this.comment = comment;
            this.decimalPrecision = decimalPrecision == null ? Optional.empty() : decimalPrecision;
            this.decimalScale = decimalScale == null ? Optional.empty() : decimalScale;
        }


        @JsonProperty
        public String getColumnName()
        {
            return columnName;
        }

        @JsonProperty
        public boolean isNullable()
        {
            return nullable;
        }

        @JsonProperty
        public String getColumnType()
        {
            return columnType;
        }

        @JsonProperty
        public Optional<String> getComment()
        {
            return comment;
        }

        @JsonProperty
        public Optional<Integer> getDecimalPrecision()
        {
            return decimalPrecision;
        }

        @JsonProperty
        public Optional<Integer> getDecimalScale()
        {
            return decimalScale;
        }

    }


}
