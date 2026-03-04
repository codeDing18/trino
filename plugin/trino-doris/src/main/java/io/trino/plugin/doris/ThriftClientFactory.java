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
        TTransport transport = new TSocket(address.getHostname(), address.getPort(), 60);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new TDorisExternalService.Client(protocol);
    }

    @Override
    public PooledObject<TDorisExternalService.Client> wrap(TDorisExternalService.Client client)
    {
        return new DefaultPooledObject<TDorisExternalService.Client>(client);
    }
}
