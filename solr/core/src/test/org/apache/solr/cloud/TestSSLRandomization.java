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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.SSLTestConfig;
import org.apache.solr.util.RandomizeSSL;
import org.apache.solr.util.RandomizeSSL.SSLRandomizer;

import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A "test the test" method that verifies the SSL options randomized by {@link SolrTestCaseJ4} are 
 * correctly used in the various helper methods available from the test framework and
 * {@link MiniSolrCloudCluster}.
 *
 * @see TestMiniSolrCloudClusterSSL
 */
@RandomizeSSL(ssl=0.5,reason="frequent SSL usage to make test worth while")
public class TestSSLRandomization extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void createMiniSolrCloudCluster() throws Exception {
    configureCluster(TestMiniSolrCloudClusterSSL.NUM_SERVERS).configure();
  }
  
  public void testRandomizedSslAndClientAuth() throws Exception {
    TestMiniSolrCloudClusterSSL.checkClusterWithCollectionCreations(cluster,sslConfig);
  }
  
  public void testBaseUrl() throws Exception {
    String url = buildUrl(6666, "/foo");
    assertEquals(sslConfig.isSSLMode() ? "https://127.0.0.1:6666/foo" : "http://127.0.0.1:6666/foo", url);
  }
  
  /** Used by {@link #testSSLRandomizer} */
  @RandomizeSSL(ssl=0.42,clientAuth=0.33,reason="foo")
  public class FullyAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  public class InheritedFullyAnnotated extends FullyAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  public class NotAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  public class InheritedNotAnnotated extends NotAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  @SuppressSSL(bugUrl="fakeBugUrl")
  public class Suppressed { };
  
  /** Used by {@link #testSSLRandomizer} */
  public class InheritedSuppressed extends Suppressed { };
  
  /** Used by {@link #testSSLRandomizer} */
  @SuppressSSL(bugUrl="fakeBugUrl")
  public class InheritedAnnotationButSuppressed extends FullyAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  @RandomizeSSL(ssl=0.42,clientAuth=0.33,reason="foo")
  public class InheritedSuppressedWithIgnoredAnnotation extends Suppressed {
    // Even with direct annotation, supression at superclass overrules us.
    //
    // (If it didn't work this way, it would be a pain in the ass to quickly disable SSL for a
    // broad hierarchy of tests)
  };
  
  /** Used by {@link #testSSLRandomizer} */
  @RandomizeSSL()
  public class EmptyAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  public class InheritedEmptyAnnotated extends EmptyAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  @RandomizeSSL(0.5)
  public class InheritedEmptyAnnotatationWithOverride extends EmptyAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  @RandomizeSSL(ssl=0.42,clientAuth=0.33,reason="foo")
  public class GrandchildInheritedEmptyAnnotatationWithOverride extends InheritedEmptyAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  @RandomizeSSL(0.5)
  public class SimplyAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  @RandomizeSSL(0.0)
  public class MinAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  @RandomizeSSL(1)
  public class MaxAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  @RandomizeSSL(ssl=0.42)
  public class SSlButNoClientAuthAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  @RandomizeSSL(clientAuth=0.42)
  public class ClientAuthButNoSSLAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  @RandomizeSSL(ssl=42.0)
  public class SSLOutOfRangeAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  @RandomizeSSL(clientAuth=42.0)
  public class ClientAuthOutOfRangeAnnotated { };
  
  /** Used by {@link #testSSLRandomizer} */
  public class InheritedOutOfRangeAnnotated extends ClientAuthOutOfRangeAnnotated { };
  
  public void testSSLRandomizer() {
    SSLRandomizer r;
    // for some cases, we know exactly what the config should be regardless of randomization factors
    SSLTestConfig conf;

    for (Class c : Arrays.asList(FullyAnnotated.class, InheritedFullyAnnotated.class,
                                 GrandchildInheritedEmptyAnnotatationWithOverride.class )) {
      r = SSLRandomizer.getSSLRandomizerForClass(c);
      assertEquals(c.toString(), 0.42D, r.ssl, 0.0D);
      assertEquals(c.toString(), 0.33D, r.clientAuth, 0.0D);
      assertTrue(c.toString(), r.debug.contains("foo"));
    }

    for (Class c : Arrays.asList(NotAnnotated.class, InheritedNotAnnotated.class)) { 
      r = SSLRandomizer.getSSLRandomizerForClass(c);
      assertEquals(c.toString(), 0.0D, r.ssl, 0.0D);
      assertEquals(c.toString(), 0.0D, r.clientAuth, 0.0D);
      assertTrue(c.toString(), r.debug.contains("not specified"));
      conf = r.createSSLTestConfig();
      assertEquals(c.toString(), false, conf.isSSLMode());
      assertEquals(c.toString(), false, conf.isClientAuthMode());
    }

    for (Class c : Arrays.asList(Suppressed.class,
                                 InheritedSuppressed.class,
                                 InheritedAnnotationButSuppressed.class,
                                 InheritedSuppressedWithIgnoredAnnotation.class)) {
      r = SSLRandomizer.getSSLRandomizerForClass(Suppressed.class);
      assertEquals(c.toString(), 0.0D, r.ssl, 0.0D);
      assertEquals(c.toString(), 0.0D, r.clientAuth, 0.0D);
      assertTrue(c.toString(), r.debug.contains("SuppressSSL"));
      assertTrue(c.toString(), r.debug.contains("fakeBugUrl"));
      conf = r.createSSLTestConfig();
      assertEquals(c.toString(), false, conf.isSSLMode());
      assertEquals(c.toString(), false, conf.isClientAuthMode());
    }

    for (Class c : Arrays.asList(EmptyAnnotated.class, InheritedEmptyAnnotated.class)) {
      r = SSLRandomizer.getSSLRandomizerForClass(c);
      assertEquals(c.toString(), RandomizeSSL.DEFAULT_ODDS, r.ssl, 0.0D);
      assertEquals(c.toString(), RandomizeSSL.DEFAULT_ODDS, r.clientAuth, 0.0D);
    }

    for (Class c : Arrays.asList(SimplyAnnotated.class, InheritedEmptyAnnotatationWithOverride.class)) {
      r = SSLRandomizer.getSSLRandomizerForClass(c);
      assertEquals(c.toString(), 0.5D, r.ssl, 0.0D);
      assertEquals(c.toString(), 0.5D, r.clientAuth, 0.0D);
    }
    
    r = SSLRandomizer.getSSLRandomizerForClass(MinAnnotated.class);
    assertEquals(0.0D, r.ssl, 0.0D);
    assertEquals(0.0D, r.clientAuth, 0.0D);
    conf = r.createSSLTestConfig();
    assertEquals(false, conf.isSSLMode());
    assertEquals(false, conf.isClientAuthMode());
    
    r = SSLRandomizer.getSSLRandomizerForClass(MaxAnnotated.class);
    assertEquals(1.0D, r.ssl, 0.0D);
    assertEquals(1.0D, r.clientAuth, 0.0D);
    conf = r.createSSLTestConfig();
    assertEquals(true, conf.isSSLMode());
    assertEquals(true, conf.isClientAuthMode());

    r = SSLRandomizer.getSSLRandomizerForClass(SSlButNoClientAuthAnnotated.class);
    assertEquals(0.42D, r.ssl, 0.0D);
    assertEquals(0.42D, r.clientAuth, 0.0D);

    r = SSLRandomizer.getSSLRandomizerForClass(ClientAuthButNoSSLAnnotated.class);
    assertEquals(RandomizeSSL.DEFAULT_ODDS, r.ssl, 0.0D);
    assertEquals(0.42D, r.clientAuth, 0.0D);

    for (Class c : Arrays.asList(SSLOutOfRangeAnnotated.class,
                                 ClientAuthOutOfRangeAnnotated.class,
                                 InheritedOutOfRangeAnnotated.class)) {
      expectThrows(IllegalArgumentException.class, () -> {
          Object trash = SSLRandomizer.getSSLRandomizerForClass(c);
        });
    }
    
  }
  public void testSSLRandomizerEffectiveOdds() {
    assertEquals(RandomizeSSL.DEFAULT_ODDS,
                 SSLRandomizer.getEffectiveOdds(RandomizeSSL.DEFAULT_ODDS, false, 1), 0.0005D);
    assertEquals(0.2727D,
                 SSLRandomizer.getEffectiveOdds(RandomizeSSL.DEFAULT_ODDS, true, 1), 0.0005D);
    
    assertEquals(0.0100D, SSLRandomizer.getEffectiveOdds(0.01D, false, 1), 0.0005D);
    assertEquals(0.1000D, SSLRandomizer.getEffectiveOdds(0.01D, true, 1), 0.0005D);
    assertEquals(0.6206D, SSLRandomizer.getEffectiveOdds(0.01D, false, 5), 0.0005D);
    
    assertEquals(0.5000D, SSLRandomizer.getEffectiveOdds(0.5D, false, 1), 0.0005D);
    assertEquals(0.5454D, SSLRandomizer.getEffectiveOdds(0.5D, true, 1), 0.0005D);
    assertEquals(0.8083D, SSLRandomizer.getEffectiveOdds(0.5D, false, 5), 0.0005D);
    
    assertEquals(0.8000D, SSLRandomizer.getEffectiveOdds(0.8D, false, 1), 0.0005D);
    assertEquals(0.8181D, SSLRandomizer.getEffectiveOdds(0.8D, true, 1), 0.0005D);
    assertEquals(0.9233D, SSLRandomizer.getEffectiveOdds(0.8D, false, 5), 0.0005D);

    // never ever
    assertEquals(0.0D, SSLRandomizer.getEffectiveOdds(0.0D, false, 1), 0.0D);
    assertEquals(0.0D, SSLRandomizer.getEffectiveOdds(0.0D, true, 100), 0.0D);
    assertEquals(0.0D, SSLRandomizer.getEffectiveOdds(0.0D, false, 100), 0.0D);
    assertEquals(0.0D, SSLRandomizer.getEffectiveOdds(0.0D, true, 10000), 0.0D);
    assertEquals(0.0D, SSLRandomizer.getEffectiveOdds(0.0D, false, 10000), 0.0D);
    assertEquals(0.0D, SSLRandomizer.getEffectiveOdds(0.0D, random().nextBoolean(), random().nextInt()), 0.0D);
    
    // always
    assertEquals(1.0D, SSLRandomizer.getEffectiveOdds(1.0D, false, 1), 0.0D);
    assertEquals(1.0D, SSLRandomizer.getEffectiveOdds(1.0D, true, 100), 0.0D);
    assertEquals(1.0D, SSLRandomizer.getEffectiveOdds(1.0D, false, 100), 0.0D);
    assertEquals(1.0D, SSLRandomizer.getEffectiveOdds(1.0D, true, 10000), 0.0D);
    assertEquals(1.0D, SSLRandomizer.getEffectiveOdds(1.0D, false, 10000), 0.0D);
    assertEquals(1.0D, SSLRandomizer.getEffectiveOdds(1.0D, random().nextBoolean(), random().nextInt()), 0.0D);
    
  }

  
}
