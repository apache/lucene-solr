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
package org.apache.solr.client.solrj;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * @since solr 1.3
 */
public class StartSolrJetty
{
  public static void main( String[] args )
  {
    //System.setProperty("solr.solr.home", "../../../example/solr");

    Server server = new Server();
    ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory());
    // Set some timeout options to make debugging easier.
    connector.setIdleTimeout(1000 * 60 * 60);
    connector.setPort(8983);
    server.setConnectors(new Connector[] { connector });

    WebAppContext bb = new WebAppContext();
    bb.setServer(server);
    bb.setContextPath("/solr");
    bb.setWar("webapp/web");

//    // START JMX SERVER
//    if( true ) {
//      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
//      MBeanContainer mBeanContainer = new MBeanContainer(mBeanServer);
//      server.getContainer().addEventListener(mBeanContainer);
//      mBeanContainer.start();
//    }

    server.setHandler(bb);

    try {
      System.out.println(">>> STARTING EMBEDDED JETTY SERVER, PRESS ANY KEY TO STOP");
      server.start();
      while (System.in.available() == 0) {
        Thread.sleep(5000);
      }
      server.stop();
      server.join();
    }
    catch (Exception e) {
      e.printStackTrace();
      System.exit(100);
    }
  }
}
