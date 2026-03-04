package io.trino.plugin.doris;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TNetworkAddress;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ThriftClientFactory extends BaseKeyedPooledObjectFactory<TNetworkAddress, TDorisExternalService.Client>
{
    @Override
    public TDorisExternalService.Client create(TNetworkAddress address)
            throws Exception
    {
        TTransport transport = new TSocket(address.getHostname(), address.getPort());
        TProtocol protocol = new TBinaryProtocol(transport);
        return new TDorisExternalService.Client(protocol);
    }

    @Override
    public PooledObject<TDorisExternalService.Client> wrap(TDorisExternalService.Client client)
    {
        return new DefaultPooledObject<TDorisExternalService.Client>(client);
    }
}
