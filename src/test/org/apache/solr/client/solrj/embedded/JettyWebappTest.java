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

package org.apache.solr.client.solrj.embedded;

import java.net.URL;

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.webapp.WebAppContext;

/**
 * @version $Id$
 * @since solr 1.3
 */
public class JettyWebappTest extends TestCase 
{
  int port = 0;
  static final String context = "/test";
  
  Server server;
  
  @Override
  public void setUp() throws Exception 
  {
    System.setPrope

    rty("solr.solr.home", "../../../example/solr");
    String path = "../../webapp/web";

    server = new Server(port);
    new WebAppContext(server, path, context );

    SocketConnector connector = new SocketConnector();
    connector.setMaxIdleTime(1000 * 60 * 60);
    connector.setSoLingerTime(-1);
    connector.setPort(0);
    server.setConnectors(new Connector[]{connector});
    server.setStopAtShutdown( true );
    
    server.start();
    port = connector.getLocalPort();
  }

  @Override
  public void tearDown() throws Exception 
  {
    try {
      server.stop();
    } catch( Exception ex ) {}
  }
  
  public void testJSP() throws Exception
  {
    // Currently not an extensive test, but it does fire up the JSP pages and make 
    // sure they compile ok
    
    String adminPath = "http://localhost:"+port+context+"/";
    String html = IOUtils.toString( new URL(adminPath).openStream() );
    assertNotNull( html ); // real error will be an exception

    adminPath += "admin/";
    html = IOUtils.toString( new URL(adminPath).openStream() );
    assertNotNull( html ); // real error will be an exception

    // analysis
    html = IOUtils.toString( new URL(adminPath+"analysis.jsp").openStream() );
    assertNotNull( html ); // real error will be an exception

    // schema browser
    html = IOUtils.toString( new URL(adminPath+"schema.jsp").openStream() );
    assertNotNull( html ); // real error will be an exception

    // schema browser
    html = IOUtils.toString( new URL(adminPath+"threaddump.jsp").openStream() );
    assertNotNull( html ); // real error will be an exception
  }
}
