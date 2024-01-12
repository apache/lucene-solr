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
package org.apache.solr.client.solrj.io;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider;
import org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrClientCacheTest extends SolrCloudTestCase {

  private static final Map<String, String> sysProps =
      ImmutableMap.of(
          SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
              VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName(),
          SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
              VMParamsAllAndReadonlyDigestZkACLProvider.class.getName(),
          VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "admin-user",
          VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, "pass",
          VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME, "read-user",
          VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME, "pass");

  @BeforeClass
  public static void before() throws Exception {
    sysProps.forEach(System::setProperty);
    configureCluster(1)
        .addConfig("config", getFile("solrj/solr/configsets/streaming/conf").toPath())
        .configure();
  }

  @AfterClass
  public static void after() {
    sysProps.keySet().forEach(System::clearProperty);
  }

  @Test
  public void testZkACLsNotUsedWithDifferentZkHost() {
    SolrClientCache cache = new SolrClientCache();
    try {
      // This ZK Host is fake, thus the ZK ACLs should not be used
      cache.setDefaultZKHost("test:2181");
      expectThrows(
          SolrException.class, () -> cache.getCloudSolrClient(zkClient().getZkServerAddress()).close());
    } finally {
      cache.close();
    }
  }

  @Test
  public void testZkACLsUsedWithDifferentChroot() throws IOException {
    SolrClientCache cache = new SolrClientCache();
    try {
      // The same ZK Host is used, so the ZK ACLs should still be applied
      cache.setDefaultZKHost(zkClient().getZkServerAddress() + "/random/chroot");
      cache.getCloudSolrClient(zkClient().getZkServerAddress()).close();
    } finally {
      cache.close();
    }
  }
}
