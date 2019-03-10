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
package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class CloudHttp2SolrClientMultiConstructorTest extends SolrTestCase {
  
  /*
   * NOTE: If you only include one String argument, it will NOT use the
   * constructor with the variable argument list, which is the one that
   * we are testing here.
   */
  Collection<String> hosts;

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testZkConnectionStringConstructorWithValidChroot() throws IOException {
    boolean setOrList = random().nextBoolean();
    int numOfZKServers = TestUtil.nextInt(random(), 1, 5);
    boolean withChroot = random().nextBoolean();

    final String chroot = "/mychroot";

    StringBuilder sb = new StringBuilder();

    if(setOrList) {
      /*
        A LinkedHashSet is required here for testing, or we can't guarantee
        the order of entries in the final string.
       */
      hosts = new LinkedHashSet<>();
    } else {
      hosts = new ArrayList<>();
    }

    for(int i=0; i<numOfZKServers; i++) {
      String ZKString = "host" + i + ":2181";
      hosts.add(ZKString);
      sb.append(ZKString);
      if(i<numOfZKServers -1) sb.append(",");
    }

    String clientChroot = null;
    if (withChroot) {
      sb.append(chroot);
      clientChroot = "/mychroot";
    }

    try (CloudHttp2SolrClient client = new CloudHttp2SolrClient.Builder(new ArrayList<>(hosts), Optional.ofNullable(clientChroot)).build()) {
      assertEquals(sb.toString(), client.getZkHost());
    }
  }
  
  @Test(expected = IllegalArgumentException.class)
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testBadChroot() {
    final List<String> zkHosts = new ArrayList<>();
    zkHosts.add("host1:2181");
    new CloudHttp2SolrClient.Builder(zkHosts, Optional.of("foo")).build();
  }
}
