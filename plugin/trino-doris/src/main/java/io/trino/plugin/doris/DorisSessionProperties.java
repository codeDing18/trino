package io.trino.plugin.doris;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;

public class DorisSessionProperties implements SessionPropertiesProvider
{
    public static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";


    private final List<PropertyMetadata<?>> sessionProperties;


    @Inject
    public DorisSessionProperties(DorisConfig dorisConfig) {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters during split generation",
                        dorisConfig.getDynamicFilteringWaitTimeout(),
                        false))
                .build();
    }


    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Duration getDynamicFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Duration.class);
    }
}
