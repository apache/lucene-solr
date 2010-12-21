package org.apache.lucene.search;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;

/**
 * Base class for remote tests.
 * <p>
 * Call {@link #startServer(Searchable)} in a {@link #BeforeClass} annotated method
 * to start the server.
 * Call {@link #lookupRemote} to get a RemoteSearchable.
 */
public abstract class RemoteTestCase extends LuceneTestCase {
  private static int port;

  public static void startServer(Searchable searchable) throws Exception {
    // publish it
    // use our own factories for testing, so we can bind to an ephemeral port.
    RMIClientSocketFactory clientFactory = new RMIClientSocketFactory() {
      public Socket createSocket(String host, int port) throws IOException {
        return new Socket(host, port);
      }};

    class TestRMIServerSocketFactory implements RMIServerSocketFactory {
      ServerSocket socket;
      public ServerSocket createServerSocket(int port) throws IOException {
        return (socket = new ServerSocket(port));
      }
    };
    TestRMIServerSocketFactory serverFactory = new TestRMIServerSocketFactory();
    
    LocateRegistry.createRegistry(0, clientFactory, serverFactory);
    RemoteSearchable impl = new RemoteSearchable(searchable);
    port = serverFactory.socket.getLocalPort();
    Naming.rebind("//localhost:" + port + "/Searchable", impl);
  }
  
  @AfterClass
  public static void stopServer() {
    try {
      Naming.unbind("//localhost:" + port + "/Searchable");
    } catch (RemoteException e) {
    } catch (MalformedURLException e) {
    } catch (NotBoundException e) {
    }
  }
  
  public static Searchable lookupRemote() throws Exception {
    return (Searchable)Naming.lookup("//localhost:" + port + "/Searchable");
  }
}
