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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.FeaturesConfig;
import io.trino.metadata.TypeRegistry;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.type.InternalTypeManager;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class DorisModule implements Module {
    @Override
    public void configure(Binder binder)
    {
        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(DorisSessionProperties.class).in(Scopes.SINGLETON);

        binder.bind(DorisConnectorFactory.class).in(Scopes.SINGLETON);
        binder.bind(DorisConnector.class).in(Scopes.SINGLETON);

        binder.bind(DorisMetadataFactory.class).to(DefaultDorisMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(DorisTransactionManager.class).in(Scopes.SINGLETON);

        binder.bind(DorisClient.class).in(Scopes.SINGLETON);
        binder.bind(DorisFeClient.class).in(Scopes.SINGLETON);
        binder.bind(DorisBeClient.class).in(Scopes.SINGLETON);
        binder.bind(DorisSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DorisPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ClientPool.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSourceProvider.class).setDefault().to(DorisPageSourceProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(DorisConfig.class);
        configBinder(binder).bindConfig(FeaturesConfig.class);


        binder.bind(BaseJdbcConfig.class).in(Scopes.SINGLETON);

//        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(DorisTable.class));
    }

    @Provides
    @Singleton
    @ForDorisSplitManager
    public ListeningExecutorService createSplitSourceExecutor()
    {
        return listeningDecorator(newCachedThreadPool(daemonThreadsNamed("doris-split-source")));
    }

}
