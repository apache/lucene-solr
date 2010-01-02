package org.apache.solr.cloud;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;

public class ZkTestServer {
  private static final int PORT = 2181;

  protected ZKServerMain zkServer = new ZKServerMain();

  private String zkDir;

  class ZKServerMain extends ZooKeeperServerMain {
    public void shutdown() {
      super.shutdown();
    }
  }

  public ZkTestServer(String zkDir) {
    this.zkDir = zkDir;
  }

  public void run() throws InterruptedException {
    // we don't call super.setUp
    Thread zooThread = new Thread() {
      @Override
      public void run() {
        ServerConfig config = new ServerConfig() {
          {
            this.clientPort = PORT;
            this.dataDir = zkDir;;
            this.dataLogDir = zkDir;
          }
        };

        try {
          zkServer.runFromConfig(config);
        } catch (Throwable e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    };

    zooThread.setDaemon(true);
    zooThread.start();
    Thread.sleep(500); // pause for ZooKeeper to start
  }

  public void shutdown() {
    zkServer.shutdown();
  }
}
