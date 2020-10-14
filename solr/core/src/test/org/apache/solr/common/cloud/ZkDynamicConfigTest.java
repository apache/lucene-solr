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

package org.apache.solr.common.cloud;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.Test;

public class ZkDynamicConfigTest extends SolrTestCaseJ4 {
  @Test
  public void parseLines() {
    ZkDynamicConfig parsed = ZkDynamicConfig.parseLines(
        "ignored-line\n" +
            "server.1=zoo1:2780:2783:participant;0.0.0.0:2181\n" +
            "server.2=zoo2:2781:2784:participant|zoo3:2783;2181\n" +
            "server.3=zoo3:2782:2785;zoo3-client:2181\n" +
            "server.4=zoo4:2783:2786:participant\n" + // this assumes clientPort specified in static config
            "version=400000003");
    assertEquals(4, parsed.size());

    assertEquals("zoo1", parsed.getServers().get(0).address);
    assertEquals(Integer.valueOf(2780), parsed.getServers().get(0).leaderPort);
    assertEquals(Integer.valueOf(2783), parsed.getServers().get(0).leaderElectionPort);
    assertEquals("participant", parsed.getServers().get(0).role);
    assertEquals("0.0.0.0", parsed.getServers().get(0).clientPortAddress);
    assertEquals(Integer.valueOf(2181), parsed.getServers().get(0).clientPort);
    assertEquals("zoo1", parsed.getServers().get(0).resolveClientPortAddress());

    // |<host2> is ignored
    assertEquals("participant", parsed.getServers().get(1).role);
    assertNull(parsed.getServers().get(1).clientPortAddress);
    assertEquals("zoo2", parsed.getServers().get(1).resolveClientPortAddress());
    assertEquals(Integer.valueOf(2181), parsed.getServers().get(1).clientPort);

    // role optional
    assertNull(parsed.getServers().get(2).role);
    assertEquals("zoo3-client", parsed.getServers().get(2).clientPortAddress);
    assertEquals("zoo3-client", parsed.getServers().get(2).resolveClientPortAddress());

    // client address/port optional if clientPort specified in static config file (back-compat mode)
    assertEquals("participant", parsed.getServers().get(3).role);
    assertEquals(null, parsed.getServers().get(3).clientPortAddress);
    assertEquals("zoo4", parsed.getServers().get(3).resolveClientPortAddress());
    assertEquals(null, parsed.getServers().get(3).clientPort);
  }

  @Test(expected = SolrException.class)
  public void parseLinesInvalid() {
    ZkDynamicConfig.parseLines(
        "server.1=zoo2:2781:2784:participant|zoo3:2783;0.0.0.0:2181\n" +
            "server.2=zoo3:2782\n" + // This line fails as it lacks mandatory parts
            "version=400000003");
  }
}