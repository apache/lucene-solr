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

package org.apache.solr.client.solrj.embedded;

import java.io.File;
import java.net.URL;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.util.ExternalPaths;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.session.HashSessionIdManager;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

/**
 *
 * @since solr 1.3
 */
public class JettyWebappTest extends LuceneTestCase 
{
  int port = 0;
  static final String context = "/test";
 
  @Rule
  public TestRule solrTestRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule());

  Server server;
  
  @Override
  public void setUp() throws Exception 
  {
    super.setUp();
    System.setProperty("solr.solr.home", ExternalPaths.EXAMPLE_HOME);
    
    File dataDir = new File(LuceneTestCase.TEMP_DIR,
        getClass().getName() + "-" + System.currentTimeMillis());
    dataDir.mkdirs();
    System.setProperty("solr.data.dir", dataDir.getCanonicalPath());
    String path = ExternalPaths.WEBAPP_HOME;

    server = new Server(port);
    // insecure: only use for tests!!!!
    server.setSessionIdManager(new HashSessionIdManager(new Random(random().nextLong())));
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
    super.tearDown();
  }
  
  public void testAdminUI() throws Exception
  {
    // Currently not an extensive test, but it does fire up the JSP pages and make 
    // sure they compile ok
    
    String adminPath = "http://localhost:"+port+context+"/";
    byte[] bytes = IOUtils.toByteArray( new URL(adminPath).openStream() );
    assertNotNull( bytes ); // real error will be an exception
  }
}
