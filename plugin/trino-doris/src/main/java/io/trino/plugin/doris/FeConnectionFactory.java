package io.trino.plugin.doris;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.sql.Connection;
import java.sql.DriverManager;

public class FeConnectionFactory
        extends BaseKeyedPooledObjectFactory<String, Connection>
{
    private final String url;
    private final String username;
    private final String password;

    public FeConnectionFactory(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    @Override
    public Connection create(String s)
            throws Exception
    {
        return DriverManager.getConnection(url, username, password);
    }

    @Override
    public PooledObject<Connection> wrap(Connection connection)
    {
        return new DefaultPooledObject<>(connection);
    }
}
