package io.trino.plugin.doris;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;

public class DorisConfig {
    private String jdbcURL;
    private String queryPlanURL;
    private String feUser;
    private Optional<String> fePassword;
    private Duration dynamicFilteringWaitTimeout = new Duration(1, SECONDS);


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
