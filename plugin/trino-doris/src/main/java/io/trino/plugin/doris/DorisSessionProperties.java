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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.DecimalConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.math.RoundingMode;
import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public class DorisSessionProperties implements SessionPropertiesProvider
{
    public static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";
    public static final String DECIMAL_MAPPING = "decimal_mapping";
    public static final String DECIMAL_DEFAULT_SCALE = "decimal_default_scale";
    public static final String DECIMAL_ROUNDING_MODE = "decimal_rounding_mode";


    private final List<PropertyMetadata<?>> sessionProperties;


    @Inject
    public DorisSessionProperties(DorisConfig dorisConfig) {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters during split generation",
                        dorisConfig.getDynamicFilteringWaitTimeout(),
                        false))
                .add(enumProperty(
                        DECIMAL_MAPPING,
                        "Decimal mapping for unspecified and exceeding precision decimals. STRICT skips them. ALLOW_OVERFLOW requires setting proper decimal scale and rounding mode",
                        DorisConfig.DecimalMapping.class,
                        dorisConfig.getDecimalMapping(),
                        false))
                .add(integerProperty(
                        DECIMAL_DEFAULT_SCALE,
                        "Default decimal scale for mapping unspecified and exceeding precision decimals. Not used when " + DECIMAL_MAPPING + " is set to STRICT",
                        dorisConfig.getDecimalDefaultScale(),
                        false))
                .add(enumProperty(
                        DECIMAL_ROUNDING_MODE,
                        "Rounding mode for mapping unspecified and exceeding precision decimals. Not used when " + DECIMAL_MAPPING + " is set to STRICT",
                        RoundingMode.class,
                        dorisConfig.getDecimalRoundingMode(),
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

    public static DorisConfig.DecimalMapping getDecimalRounding(ConnectorSession session)
    {
        return session.getProperty(DECIMAL_MAPPING, DorisConfig.DecimalMapping.class);
    }

    public static int getDecimalDefaultScale(ConnectorSession session)
    {
        return session.getProperty(DECIMAL_DEFAULT_SCALE, Integer.class);
    }

    public static RoundingMode getDecimalRoundingMode(ConnectorSession session)
    {
        return session.getProperty(DECIMAL_ROUNDING_MODE, RoundingMode.class);
    }
}
