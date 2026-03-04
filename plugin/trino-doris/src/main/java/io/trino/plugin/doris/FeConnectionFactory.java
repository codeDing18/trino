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

import io.trino.spi.classloader.ThreadContextClassLoader;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class FeConnectionFactory
        extends BaseKeyedPooledObjectFactory<String, Connection>
{
    private final String url;
    private final Properties properties;

    public FeConnectionFactory(String url, String username, String password) {
        this.url = url;

        this.properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        properties.setProperty("useInformationSchema", "true");
        properties.setProperty("useUnicode", "true");
        properties.setProperty("characterEncoding", "utf8");
        properties.setProperty("tinyInt1isBit", "false");
        properties.setProperty("rewriteBatchedStatements", "true");
    }

    @Override
    public Connection create(String s)
            throws Exception
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(FeConnectionFactory.class.getClassLoader())) {
            return DriverManager.getConnection(url, properties);
        }
    }

    @Override
    public boolean validateObject(String key, PooledObject<Connection> p) {
        Connection conn = p.getObject();
        try {
            // 方式1：使用 JDBC 4.0 的 isValid 方法（推荐）
            return conn.isValid(3);  // 5秒超时

            // 方式2：使用传统的验证查询
            // try (Statement stmt = conn.createStatement()) {
            //     stmt.execute("SELECT 1");
            //     return true;
            // }
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public PooledObject<Connection> wrap(Connection connection)
    {
        return new DefaultPooledObject<>(connection);
    }
}
