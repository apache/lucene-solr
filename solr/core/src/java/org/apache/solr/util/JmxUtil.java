/*
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
package org.apache.solr.util;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;

/**
 * Utility methods to find a MBeanServer.
 */
public final class JmxUtil {

  /**
   * Retrieve the first MBeanServer found and if not found return the platform mbean server
   *
   * @return the first MBeanServer found
   */
  public static MBeanServer findFirstMBeanServer() {
    MBeanServer mBeanServer = findMBeanServerForAgentId(null);
    if (mBeanServer == null)  {
      return ManagementFactory.getPlatformMBeanServer();
    }
    return mBeanServer;
  }

  /**
   * Find a MBeanServer given a service url.
   *
   * @param serviceUrl the service url
   * @return a MBeanServer
   */
  public static MBeanServer findMBeanServerForServiceUrl(String serviceUrl) throws IOException {
    if (serviceUrl == null) {
      return null;
    }

    MBeanServer server = MBeanServerFactory.newMBeanServer();
    JMXConnectorServer connector = JMXConnectorServerFactory
        .newJMXConnectorServer(new JMXServiceURL(serviceUrl), null, server);
    connector.start();

    return server;
  }

  /**
   * Find a MBeanServer given an agent id.
   *
   * @param agentId the agent id
   * @return a MBeanServer
   */
  public static MBeanServer findMBeanServerForAgentId(String agentId) {
    List<MBeanServer> servers = MBeanServerFactory.findMBeanServer(agentId);
    if (servers == null || servers.isEmpty()) {
      return null;
    }

    return servers.get(0);
  }
}
