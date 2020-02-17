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

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

/**
 * This test class does a few very basic things, but it does them at several scales. Improvements to this test will
 * impact nearly everything else in the test suite. Everything that happens here already has test coverage via other
 * tests, but this class is still useful for several scenarios:
 * <ul>
 *     <li>Isolating the startup and teardown costs for a cluster</li>
 *     <li>Capturing the startup time in a unit test rather than <code>@Before</code> methods</li>
 *     <li>Profiling basic setup operations like cluster start or collection creation</li>
 *     <li>Easily adjusting the scale of the tests - we rarely touch 100+ nodes in our test suite otherwise</li>
 * </ul>
 *
 * <p>In an effort to minimize overhead and other operations, we disable most logging. When troubleshooting specific
 * areas you will likely find it useful to tweak these settings.
 *
 * <p>The commented run times for each method are rough measures from a single desktop computer. They will vary between
 * environments, but the intent is to have some baseline for spotting regressions.
 */
@LogLevel("org=OFF")
public class MinimalSolrCloudTest extends SolrCloudTestCase {
    private void testNnode(int n) throws Exception {
        // We are explicitly interested in measuring shutdown time as well
        configureCluster(n).build().shutdown();
    }

    @Test
    public void test1node() throws Exception {
        testNnode(1); // 1s
    }

    @Test
    public void test10node() throws Exception {
        testNnode(10); // 3s
    }

    @Test
    public void test50node() throws Exception {
        testNnode(50); // 15s
    }

    @Test
    @Ignore("Too slow")
    public void test100node() throws Exception {
        testNnode(100); // 45s
    }

    @Test
    @Ignore("OOM with -Xmx=512G")
    public void test250node() throws Exception {
        testNnode(250);
    }

    private void testNshards(int n) throws Exception {
      configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
      CollectionAdminRequest.createCollection("collection", "conf", 1, n).setMaxShardsPerNode(n).process(cluster.getSolrClient());
      cluster.waitForActiveCollection("collection", 1, n);
    }

    @Test
    public void test1shard() throws Exception {
        testNshards(1); // 1s
    }

    @Test
    public void test10shard() throws Exception {
        testNshards(10); // 4s
    }

    @Test
    public void test50shard() throws Exception {
        testNshards(50); // 5s
    }

    @After
    public void teardownCluster() throws Exception {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
}
