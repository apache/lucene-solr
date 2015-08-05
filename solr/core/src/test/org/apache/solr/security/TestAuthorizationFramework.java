package org.apache.solr.security;

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

import org.apache.commons.io.Charsets;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.Slow
public class TestAuthorizationFramework extends AbstractFullDistribZkTestBase {
  final private Logger log = LoggerFactory.getLogger(TestAuthorizationFramework.class);

  static final int TIMEOUT = 10000;

  public void distribSetUp() throws Exception {
    super.distribSetUp();
    try (ZkStateReader zkStateReader = new ZkStateReader(zkServer.getZkAddress(),
        TIMEOUT, TIMEOUT)) {
      zkStateReader.getZkClient().create(ZkStateReader.SOLR_SECURITY_CONF_PATH,
          "{\"authorization\":{\"class\":\"org.apache.solr.security.MockAuthorizationPlugin\"}}".getBytes(Charsets.UTF_8),
          CreateMode.PERSISTENT, true);
    }
  }

  @Test
  public void authorizationFrameworkTest() throws Exception {
    MockAuthorizationPlugin.denyUsers.add("user1");
    MockAuthorizationPlugin.denyUsers.add("user1");
    waitForThingsToLevelOut(10);
    log.info("Starting test");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    // This should work fine.
    cloudClient.query(params);
    
    // This user is blacklisted in the mock. The request should return a 403.
    params.add("uname", "user1");
    try {
      cloudClient.query(params);
      fail("This should have failed");
    } catch (Exception e) {}
    log.info("Ending test");
  }

  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    MockAuthorizationPlugin.denyUsers.clear();

  }
}
