package io.trino.plugin.doris;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Optional;

public class DorisColumnHandle  implements ColumnHandle {
    private final String columnName;
    private final boolean nullable;
    private final Type columnType;
    private final Optional<String> comment;

    @JsonCreator
    public DorisColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("nullable") boolean nullable,
            @JsonProperty("column_type") Type columnType,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.columnName = columnName;
        this.columnType = columnType;
        this.nullable = nullable;
        this.comment = comment;
    }

    public DorisColumnHandle(Builder builder) {
        this.columnName = builder.columnName;
        this.columnType = builder.columnType;
        this.nullable = builder.nullable;
        this.comment = builder.comment;
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
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(columnType)
                .setNullable(nullable)
                .setComment(comment)
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String columnName;
        private boolean nullable;
        private Type columnType;
        private Optional<String> comment;

        public Builder() {}

        public Builder(DorisColumnHandle dorisColumnHandle) {
            this.columnName = dorisColumnHandle.getColumnName();
            this.nullable = dorisColumnHandle.isNullable();
            this.columnType = dorisColumnHandle.getColumnType();
            this.comment = dorisColumnHandle.getComment();
        }

        public Builder setComment(Optional<String> comment)
        {
            this.comment = comment;
            return this;
        }

        public Builder setColumnType(Type columnType)
        {
            this.columnType = columnType;
            return this;
        }

        public Builder setNullable(boolean nullable)
        {
            this.nullable = nullable;
            return this;
        }

        public Builder setColumnName(String columnName)
        {
            this.columnName = columnName;
            return this;
        }

        public DorisColumnHandle build() {
            return new DorisColumnHandle(this);
        }
    }
}
