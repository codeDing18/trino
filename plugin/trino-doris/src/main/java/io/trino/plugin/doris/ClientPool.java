package io.trino.plugin.doris;

import jakarta.inject.Inject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TNetworkAddress;

import java.sql.Connection;

import static java.util.Objects.requireNonNull;

public class ClientPool
{
    private final DorisConfig dorisConfig;
    private static GenericKeyedObjectPool<String, Connection> fePool;
    private static GenericKeyedObjectPool<TNetworkAddress, TDorisExternalService.Client> bePool;

    @Inject
    public ClientPool(DorisConfig dorisConfig) {
        this.dorisConfig = requireNonNull(dorisConfig, "dorisConfig is null");

        GenericKeyedObjectPoolConfig feConfig = new GenericKeyedObjectPoolConfig();
        feConfig.setMaxTotal(20);
        feConfig.setMaxIdlePerKey(5);
        feConfig.setMinIdlePerKey(2);
        feConfig.setMaxWaitMillis(3000);
        feConfig.setTestOnBorrow(true);
        feConfig.setTestWhileIdle(true);

        GenericKeyedObjectPoolConfig beConfig = new GenericKeyedObjectPoolConfig();
        beConfig.setMaxTotal(20);
        beConfig.setMaxIdlePerKey(5);
        beConfig.setMinIdlePerKey(2);
        beConfig.setMaxWaitMillis(3000);
        beConfig.setTestOnBorrow(true);
        beConfig.setTestWhileIdle(true);

        FeConnectionFactory feFactory = new FeConnectionFactory(
                dorisConfig.getJdbcURL(),
                dorisConfig.getFeUser(),
                dorisConfig.getFePassword().orElse(""));

        ThriftClientFactory beFactory = new ThriftClientFactory();

        this.fePool = new GenericKeyedObjectPool<>(feFactory, feConfig);
        this.bePool = new GenericKeyedObjectPool<>(beFactory, beConfig);
    }

    public static GenericKeyedObjectPool<String, Connection> getFePool() {
        return fePool;
    }

    public static GenericKeyedObjectPool<TNetworkAddress, TDorisExternalService.Client> getBePool() {
        return bePool;
    }
}
