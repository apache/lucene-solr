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

package org.apache.solr.cloud.rule;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Sets;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.cloud.rule.SnitchContext;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

@TestRuleLimitSysouts.Limit(bytes=32000)
public class ImplicitSnitchTest extends SolrTestCaseJ4 {

  private ImplicitSnitch snitch;
  private SnitchContext context;

  private static final String IP_1 = "ip_1";
  private static final String IP_2 = "ip_2";
  private static final String IP_3 = "ip_3";
  private static final String IP_4 = "ip_4";

  @Before
  public void beforeImplicitSnitchTest() {
    snitch = new ImplicitSnitch();
    context = new ServerSnitchContext(null, null, new HashMap<>(),null);
  }


  @Test
  public void testGetTags_withAllIPv4RequestedTags_with_omitted_zeros_returns_four_tags() throws Exception {
    String node = "5:8983_solr";

    snitch.getTags(node, Sets.newHashSet(IP_1, IP_2, IP_3, IP_4), context);

    Map<String, Object> tags = context.getTags();
    assertThat(tags.entrySet().size(), is(4));
    assertThat(tags.get(IP_1), is("5"));
    assertThat(tags.get(IP_2), is("0"));
    assertThat(tags.get(IP_3), is("0"));
    assertThat(tags.get(IP_4), is("0"));
  }


  @Test
  public void testGetTags_withAllIPv4RequestedTags_returns_four_tags() throws Exception {
    String node = "192.168.1.2:8983_solr";

    snitch.getTags(node, Sets.newHashSet(IP_1, IP_2, IP_3, IP_4), context);

    Map<String, Object> tags = context.getTags();
    assertThat(tags.entrySet().size(), is(4));
    assertThat(tags.get(IP_1), is("2"));
    assertThat(tags.get(IP_2), is("1"));
    assertThat(tags.get(IP_3), is("168"));
    assertThat(tags.get(IP_4), is("192"));
  }

  @Test
  public void testGetTags_withIPv4RequestedTags_ip2_and_ip4_returns_two_tags() throws Exception {
    String node = "192.168.1.2:8983_solr";

    SnitchContext context = new ServerSnitchContext(null, node, new HashMap<>(),null);
    snitch.getTags(node, Sets.newHashSet(IP_2, IP_4), context);

    Map<String, Object> tags = context.getTags();
    assertThat(tags.entrySet().size(), is(2));
    assertThat(tags.get(IP_2), is("1"));
    assertThat(tags.get(IP_4), is("192"));
  }

  @Test
  @Ignore("SOLR-12360: local DNS resolver may return some address for a host named '192.168.1.2.1'")
  public void testGetTags_with_wrong_ipv4_format_ip_returns_nothing() throws Exception {
    String node = "192.168.1.2.1:8983_solr";

    SnitchContext context = new ServerSnitchContext(null, node, new HashMap<>(),null);
    snitch.getTags(node, Sets.newHashSet(IP_1), context);

    Map<String, Object> tags = context.getTags();
    assertThat(tags.entrySet().size(), is(0));
  }


  @Test
  public void testGetTags_with_correct_ipv6_format_ip_returns_nothing() throws Exception {
    String node = "[0:0:0:0:0:0:0:1]:8983_solr";

    SnitchContext context = new ServerSnitchContext(null, node, new HashMap<>(),null);
    snitch.getTags(node, Sets.newHashSet(IP_1), context);

    Map<String, Object> tags = context.getTags();
    assertThat(tags.entrySet().size(), is(0)); //This will fail when IPv6 is implemented
  }


  @Test
  public void testGetTags_withEmptyRequestedTag_returns_nothing() throws Exception {
    String node = "192.168.1.2:8983_solr";

    snitch.getTags(node, Sets.newHashSet(), context);

    Map<String, Object> tags = context.getTags();
    assertThat(tags.entrySet().size(), is(0));
  }


  @Test
  public void testGetTags_withAllHostNameRequestedTags_returns_all_Tags() throws Exception {
    SolrTestCaseJ4.assumeWorkingMockito();
    
    String node = "serv01.dc01.london.uk.apache.org:8983_solr";

    SnitchContext context = new ServerSnitchContext(null, node, new HashMap<>(),null);
    //We need mocking here otherwise, we would need proper DNS entry for this test to pass
    ImplicitSnitch mockedSnitch = Mockito.spy(snitch);
    when(mockedSnitch.getHostIp(anyString())).thenReturn("10.11.12.13");

    mockedSnitch.getTags(node, Sets.newHashSet(IP_1, IP_2, IP_3, IP_4), context);

    Map<String, Object> tags = context.getTags();
    assertThat(tags.entrySet().size(), is(4));
    assertThat(tags.get(IP_1), is("13"));
    assertThat(tags.get(IP_2), is("12"));
    assertThat(tags.get(IP_3), is("11"));
    assertThat(tags.get(IP_4), is("10"));
  }

  @Test
  public void testGetTags_withHostNameRequestedTag_ip3_returns_1_tag() throws Exception {
    SolrTestCaseJ4.assumeWorkingMockito();
    
    String node = "serv01.dc01.london.uk.apache.org:8983_solr";

    SnitchContext context = new ServerSnitchContext(null, node, new HashMap<>(),null);
    //We need mocking here otherwise, we would need proper DNS entry for this test to pass
    ImplicitSnitch mockedSnitch = Mockito.spy(snitch);
    when(mockedSnitch.getHostIp(anyString())).thenReturn("10.11.12.13");
    mockedSnitch.getTags(node, Sets.newHashSet(IP_3), context);

    Map<String, Object> tags = context.getTags();
    assertThat(tags.entrySet().size(), is(1));
    assertThat(tags.get(IP_3), is("11"));
  }

  @Test
  public void testGetTags_withHostNameRequestedTag_ip99999_returns_nothing() throws Exception {
    SolrTestCaseJ4.assumeWorkingMockito();
    
    String node = "serv01.dc01.london.uk.apache.org:8983_solr";

    SnitchContext context = new ServerSnitchContext(null, node, new HashMap<>(),null);
    //We need mocking here otherwise, we would need proper DNS entry for this test to pass
    ImplicitSnitch mockedSnitch = Mockito.spy(snitch);
    when(mockedSnitch.getHostIp(anyString())).thenReturn("10.11.12.13");
    mockedSnitch.getTags(node, Sets.newHashSet("ip_99999"), context);

    Map<String, Object> tags = context.getTags();
    assertThat(tags.entrySet().size(), is(0));
  }

  @Test
  public void testIsKnownTag_ip1() throws Exception {
    assertFalse(snitch.isKnownTag("ip_0"));
    assertTrue(snitch.isKnownTag(IP_1));
    assertTrue(snitch.isKnownTag(IP_2));
    assertTrue(snitch.isKnownTag(IP_3));
    assertTrue(snitch.isKnownTag(IP_4));
    assertFalse(snitch.isKnownTag("ip_5"));
  }

  @Test
  public void testExceptions() throws Exception {
    ImplicitSnitch implicitSnitch = new ImplicitSnitch();
    ServerSnitchContext noNodeExceptionSnitch = new ServerSnitchContext(null, null, new HashMap<>(), null)  {
      @Override
      @SuppressWarnings({"rawtypes"})
      public Map getZkJson(String path) throws KeeperException, InterruptedException {
        throw new KeeperException.NoNodeException();
      }
    };
    implicitSnitch.getTags("", Collections.singleton(ImplicitSnitch.ROLE), noNodeExceptionSnitch);
    @SuppressWarnings({"rawtypes"})
    Map map = (Map) noNodeExceptionSnitch.retrieve(ZkStateReader.ROLES); // todo it the key really supposed to /roles.json?
    assertNotNull(map);
    assertEquals(0, map.size());

    implicitSnitch.getTags("", Collections.singleton(ImplicitSnitch.NODEROLE), noNodeExceptionSnitch);
    map = (Map) noNodeExceptionSnitch.retrieve(ZkStateReader.ROLES); // todo it the key really supposed to /roles.json?
    assertNotNull(map);
    assertEquals(0, map.size());

    ServerSnitchContext keeperExceptionSnitch = new ServerSnitchContext(null, null, new HashMap<>(), null)  {
      @Override
      @SuppressWarnings({"rawtypes"})
      public Map getZkJson(String path) throws KeeperException, InterruptedException {
        throw new KeeperException.ConnectionLossException();
      }
    };
    expectThrows(SolrException.class, KeeperException.ConnectionLossException.class, () -> implicitSnitch.getTags("", Collections.singleton(ImplicitSnitch.ROLE), keeperExceptionSnitch));
    expectThrows(SolrException.class, KeeperException.ConnectionLossException.class, () -> implicitSnitch.getTags("", Collections.singleton(ImplicitSnitch.NODEROLE), keeperExceptionSnitch));

    ServerSnitchContext remoteExceptionSnitch = new ServerSnitchContext(null, null, new HashMap<>(), null)  {

      @Override
      public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
        throw new RuntimeException();
      }
    };
    expectThrows(SolrException.class, RuntimeException.class, () -> implicitSnitch.getTags("", Collections.singleton(ImplicitSnitch.CORES), remoteExceptionSnitch));
    expectThrows(SolrException.class, RuntimeException.class, () -> implicitSnitch.getTags("", Collections.singleton(ImplicitSnitch.DISK), remoteExceptionSnitch));
    expectThrows(SolrException.class, RuntimeException.class, () -> implicitSnitch.getTags("", Collections.singleton(ImplicitSnitch.SYSPROP + "xyz"), remoteExceptionSnitch));
  }
}
