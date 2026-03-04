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
    private final GenericKeyedObjectPool<String, Connection> fePool;
    private final GenericKeyedObjectPool<TNetworkAddress, TDorisExternalService.Client> bePool;

    @Inject
    public ClientPool(DorisConfig dorisConfig) {
        this.dorisConfig = requireNonNull(dorisConfig, "dorisConfig is null");

        GenericKeyedObjectPoolConfig feConfig = new GenericKeyedObjectPoolConfig();
        feConfig.setMaxTotal(20);
        feConfig.setMaxIdlePerKey(5);
        feConfig.setMinIdlePerKey(2);
        feConfig.setMaxWaitMillis(3000);
//        feConfig.setTestOnBorrow(true);
        feConfig.setTestWhileIdle(true);
        // 3600秒检查一次
        feConfig.setTimeBetweenEvictionRunsMillis(3600000);

        GenericKeyedObjectPoolConfig beConfig = new GenericKeyedObjectPoolConfig();
        beConfig.setMaxTotal(20);
        beConfig.setMaxIdlePerKey(5);
        beConfig.setMinIdlePerKey(2);
        beConfig.setMaxWaitMillis(3000);
//        beConfig.setTestOnBorrow(true);
        beConfig.setTestWhileIdle(true);

        FeConnectionFactory feFactory = new FeConnectionFactory(
                dorisConfig.getJdbcURL(),
                dorisConfig.getFeUser(),
                dorisConfig.getFePassword().orElse(""));

        ThriftClientFactory beFactory = new ThriftClientFactory();

        this.fePool = new GenericKeyedObjectPool<>(feFactory, feConfig);
        this.bePool = new GenericKeyedObjectPool<>(beFactory, beConfig);
    }

    public GenericKeyedObjectPool<String, Connection> getFePool() {
        return fePool;
    }

    public GenericKeyedObjectPool<TNetworkAddress, TDorisExternalService.Client> getBePool() {
        return bePool;
    }


    public PooledConnection borrowPooledConnection(String url) {
        try {
            Connection conn = fePool.borrowObject(url);
            return new PooledConnection(url, conn, fePool);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



    public class PooledConnection implements AutoCloseable {
        private final Connection connection;
        private final String key;
        private final GenericKeyedObjectPool<String, Connection> pool;
        private boolean closed = false;

        public PooledConnection(String key, Connection connection,
                GenericKeyedObjectPool<String, Connection> pool) {
            this.key = key;
            this.connection = connection;
            this.pool = pool;
        }

        public Connection getConnection() {
            return connection;
        }

        @Override
        public void close() {
            if (!closed) {
                try {
                    pool.returnObject(key, connection);
                } catch (Exception e) {
                    // 归还失败，尝试使无效
                    try {
                        pool.invalidateObject(key, connection);
                    } catch (Exception ex) {
                        // ignore
                    }
                }
                closed = true;
            }
        }

        public void invalidate() {
            if (!closed) {
                try {
                    pool.invalidateObject(key, connection);
                } catch (Exception e) {
                    // ignore
                }
                closed = true;
            }
        }
    }
}
