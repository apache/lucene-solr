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

import org.apache.solr.client.solrj.SolrExampleTests;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.util.ExternalPaths;

import java.io.File;
import java.util.Map;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO? perhaps use:
 *  http://docs.codehaus.org/display/JETTY/ServletTester
 * rather then open a real connection?
 * 
 */
public class SolrExampleJettyTest extends SolrExampleTests {

  private static Logger log = LoggerFactory.getLogger(SolrExampleJettyTest.class);

  private static boolean manageSslProps = true;
  
  private static final File TEST_KEYSTORE = new File(ExternalPaths.SOURCE_HOME, 
                                                     "example/etc/solrtest.keystore");
  
  private static final Map<String,String> SSL_PROPS = new HashMap<String,String>();
  static {
    SSL_PROPS.put("tests.jettySsl","false");
    SSL_PROPS.put("tests.jettySsl.clientAuth","false");
    SSL_PROPS.put("javax.net.ssl.keyStore", TEST_KEYSTORE.getAbsolutePath());
    SSL_PROPS.put("javax.net.ssl.keyStorePassword","secret");
    SSL_PROPS.put("javax.net.ssl.trustStore", TEST_KEYSTORE.getAbsolutePath());
    SSL_PROPS.put("javax.net.ssl.trustStorePassword","secret");
  }

  @BeforeClass
  public static void beforeTest() throws Exception {

    // // //

    // :TODO: SOLR-4394 promote SSL up to SolrJettyTestBase?

    // consume the same amount of random no matter what
    final boolean trySsl = random().nextBoolean();
    final boolean trySslClientAuth = random().nextBoolean();
    
    // only randomize SSL if none of the SSL_PROPS are already set
    final Map<Object,Object> sysprops = System.getProperties();
    for (String prop : SSL_PROPS.keySet()) {
      if (sysprops.containsKey(prop)) {
        log.info("System property explicitly set, so skipping randomized ssl properties: " + prop);
        manageSslProps = false;
        break;
      }
    }

    assertTrue("test keystore does not exist, can't be used for randomized " +
               "ssl testing: " + TEST_KEYSTORE.getAbsolutePath(), 
               TEST_KEYSTORE.exists() );

    if (manageSslProps) {
      log.info("Randomized ssl ({}) and clientAuth ({})", trySsl, trySslClientAuth);
      for (String prop : SSL_PROPS.keySet()) {
        System.setProperty(prop, SSL_PROPS.get(prop));
      }
      // now explicitly re-set the two random values
      System.setProperty("tests.jettySsl", String.valueOf(trySsl));
      System.setProperty("tests.jettySsl.clientAuth", String.valueOf(trySslClientAuth));
    }
    // // //


    createJetty(ExternalPaths.EXAMPLE_HOME, null, null);
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (manageSslProps) {
      for (String prop : SSL_PROPS.keySet()) {
        System.clearProperty(prop);
      }
    }
  }


  @Test
  public void testBadSetup()
  {
    try {
      // setup the server...
      String url = "http://127.0.0.1/?core=xxx";
      HttpSolrServer s = new HttpSolrServer( url );
      Assert.fail( "CommonsHttpSolrServer should not allow a path with a parameter: "+s.getBaseURL() );
    }
    catch( Exception ex ) {
      // expected
    }
  }
}
