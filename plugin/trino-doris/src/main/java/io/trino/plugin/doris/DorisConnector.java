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

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.JdbcTransactionManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class DorisConnector implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final DorisSplitManager splitManager;
    private final DorisPageSourceProvider pageSourceProvider;
    private final DorisTransactionManager transactionManager;
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public DorisConnector(
            LifeCycleManager lifeCycleManager,
            DorisSplitManager splitManager,
            DorisPageSourceProvider pageSourceProvider,
            DorisTransactionManager transactionManager,
            Set<SessionPropertiesProvider> sessionProperties)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "recordSetProvider is null");
        this.transactionManager = requireNonNull(transactionManager, "recordSetProvider is null");
        this.sessionProperties = sessionProperties.stream()
                .flatMap(sessionPropertiesProvider -> sessionPropertiesProvider.getSessionProperties().stream())
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return transactionManager.beginTransaction(isolationLevel, readOnly, autoCommit);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return new ClassLoaderSafeConnectorMetadata(transactionManager.getMetadata(transactionHandle), getClass().getClassLoader());
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public DorisPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
