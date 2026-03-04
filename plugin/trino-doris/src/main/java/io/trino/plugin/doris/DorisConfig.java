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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.trino.plugin.jdbc.DecimalConfig;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.math.RoundingMode;
import java.util.Optional;

import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_MAPPING;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DorisConfig {
    private static final String METADATA_CACHE_TTL = "metadata.cache-ttl";
    private static final String METADATA_SCHEMAS_CACHE_TTL = "metadata.schemas.cache-ttl";
    private static final String METADATA_TABLES_CACHE_TTL = "metadata.tables.cache-ttl";
    private static final String METADATA_STATISTICS_CACHE_TTL = "metadata.statistics.cache-ttl";
    private static final String METADATA_CACHE_MAXIMUM_SIZE = "metadata.cache-maximum-size";
    private static final long DEFAULT_METADATA_CACHE_SIZE = 10000;

    private String jdbcURL;
    private String queryPlanURL;
    private String feUser;
    private Optional<String> fePassword;
    private Duration dynamicFilteringWaitTimeout = new Duration(1, SECONDS);


    private DecimalMapping decimalMapping = DecimalMapping.STRICT;
    private int decimalDefaultScale;
    private RoundingMode decimalRoundingMode = UNNECESSARY;


    private Duration metadataCacheTtl = new Duration(0, SECONDS);
    private Optional<Duration> schemaNamesCacheTtl = Optional.empty();
    private Optional<Duration> tableNamesCacheTtl = Optional.empty();
    private Optional<Duration> statisticsCacheTtl = Optional.empty();
    private boolean cacheMissing;
    private Optional<Long> cacheMaximumSize = Optional.empty();

    public enum DecimalMapping
    {
        STRICT,
        ALLOW_OVERFLOW,
        /**/;
    }

    @Config("doris.jdbc-url")
    @ConfigDescription("jdbc-url=jdbc:mysql://fe:9030")
    public DorisConfig setJdbcURL(String jdbcURL)
    {
        this.jdbcURL = jdbcURL;
        return this;
    }

    @Config("doris.queryPlan-url")
    @ConfigDescription("fe:8030")
    public DorisConfig setQueryPlanURL(String queryPlanURL)
    {
        this.queryPlanURL = queryPlanURL;
        return this;
    }

    @Config("doris.user")
    @ConfigDescription("fe user")
    public DorisConfig setFeUser(String feUser)
    {
        this.feUser = feUser;
        return this;
    }

    @Config("doris.password")
    @ConfigDescription("fe password")
    public DorisConfig setFePassword(String fePassword)
    {
        this.fePassword = Optional.of(fePassword);
        return this;
    }

    @Config("doris.dynamic-filtering.wait-timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters during split generation")
    public DorisConfig setDynamicFilteringWaitTimeout(Duration dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
    }

    @NotNull
    public Duration getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    @NotNull
    public DecimalMapping getDecimalMapping()
    {
        return decimalMapping;
    }

    @Config("decimal-mapping")
    @ConfigDescription("Decimal mapping for unspecified and exceeding precision decimals. STRICT skips them. ALLOW_OVERFLOW requires setting proper decimal scale and rounding mode")
    public DorisConfig setDecimalMapping(DecimalMapping decimalMapping)
    {
        this.decimalMapping = decimalMapping;
        return this;
    }

    @Min(0)
    @Max(38)
    public int getDecimalDefaultScale()
    {
        return decimalDefaultScale;
    }

    @Config("decimal-default-scale")
    @ConfigDescription("Default decimal scale for mapping unspecified and exceeding precision decimals. Not used when " + DECIMAL_MAPPING + " is set to STRICT")
    public DorisConfig setDecimalDefaultScale(Integer decimalDefaultScale)
    {
        this.decimalDefaultScale = decimalDefaultScale;
        return this;
    }

    @NotNull
    public RoundingMode getDecimalRoundingMode()
    {
        return decimalRoundingMode;
    }

    @Config("decimal-rounding-mode")
    @ConfigDescription("Rounding mode for mapping unspecified and exceeding precision decimals. Not used when" + DECIMAL_MAPPING + "is set to STRICT")
    public DorisConfig setDecimalRoundingMode(RoundingMode decimalRoundingMode)
    {
        this.decimalRoundingMode = decimalRoundingMode;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getMetadataCacheTtl()
    {
        return metadataCacheTtl;
    }

    @Config(METADATA_CACHE_TTL)
    @ConfigDescription("Determines how long meta information will be cached")
    public DorisConfig setMetadataCacheTtl(Duration metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }

    @NotNull
    public Duration getSchemaNamesCacheTtl()
    {
        return schemaNamesCacheTtl.orElse(metadataCacheTtl);
    }

    @Config(METADATA_SCHEMAS_CACHE_TTL)
    @ConfigDescription("Determines how long schema names list information will be cached")
    public DorisConfig setSchemaNamesCacheTtl(Duration schemaNamesCacheTtl)
    {
        this.schemaNamesCacheTtl = Optional.ofNullable(schemaNamesCacheTtl);
        return this;
    }

    @NotNull
    public Duration getTableNamesCacheTtl()
    {
        return tableNamesCacheTtl.orElse(metadataCacheTtl);
    }

    @Config(METADATA_TABLES_CACHE_TTL)
    @ConfigDescription("Determines how long table names list information will be cached")
    public DorisConfig setTableNamesCacheTtl(Duration tableNamesCacheTtl)
    {
        this.tableNamesCacheTtl = Optional.ofNullable(tableNamesCacheTtl);
        return this;
    }

    @NotNull
    public Duration getStatisticsCacheTtl()
    {
        return statisticsCacheTtl.orElse(metadataCacheTtl);
    }

    @Config(METADATA_STATISTICS_CACHE_TTL)
    @ConfigDescription("Determines how long table statistics information will be cached")
    public DorisConfig setStatisticsCacheTtl(Duration statisticsCacheTtl)
    {
        this.statisticsCacheTtl = Optional.ofNullable(statisticsCacheTtl);
        return this;
    }

    public boolean isCacheMissing()
    {
        return cacheMissing;
    }

    @Config("metadata.cache-missing")
    @ConfigDescription("Determines if missing information will be cached")
    public DorisConfig setCacheMissing(boolean cacheMissing)
    {
        this.cacheMissing = cacheMissing;
        return this;
    }

    @Min(1)
    public long getCacheMaximumSize()
    {
        return cacheMaximumSize.orElse(DEFAULT_METADATA_CACHE_SIZE);
    }

    @Config(METADATA_CACHE_MAXIMUM_SIZE)
    @ConfigDescription("Maximum number of objects stored in the metadata cache")
    public DorisConfig setCacheMaximumSize(long cacheMaximumSize)
    {
        this.cacheMaximumSize = Optional.of(cacheMaximumSize);
        return this;
    }



    public String getJdbcURL()
    {
        return jdbcURL;
    }

    public String getQueryPlanURL()
    {
        return queryPlanURL;
    }

    public String getFeUser()
    {
        return feUser;
    }

    public Optional<String> getFePassword()
    {
        return fePassword;
    }
}
