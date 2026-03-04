package io.trino.plugin.doris;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.plugin.jdbc.JdbcPageSourceProvider;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorPageSourceProvider;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class DorisModule implements Module {
    @Override
    public void configure(Binder binder)
    {
        binder.bind(DorisConnector.class).in(Scopes.SINGLETON);
        binder.bind(DorisMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DorisClient.class).in(Scopes.SINGLETON);
        binder.bind(DorisSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DorisPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ClientPool.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSourceProvider.class).setDefault().to(DorisPageSourceProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(DorisConfig.class);

//        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(DorisTable.class));
    }

    @Provides
    @Singleton
    @ForDorisSplitManager
    public ListeningExecutorService createSplitSourceExecutor(CatalogName catalogName)
    {
        return listeningDecorator(newCachedThreadPool(daemonThreadsNamed("doris-split-source-" + catalogName + "-%s")));
    }

}
