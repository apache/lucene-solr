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

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;

/**
 * Class holding the dynamic config of a Zookeeper ensemble as fetched from znode <code>/zookeeper/config</code>.
 */
public class ZkDynamicConfig {
  // server.<positive id> = <address1>:<port1>:<port2>[:role][|<address2>:<port2>...];[<client port address>:]<client port>
  // TODO: Add support for handling multiple address specs per server line, how we simply ignore all but the first
  public static final Pattern linePattern = Pattern.compile("server\\.(?<serverId>\\d+) ?= ?(?<address>[^:]+):(?<leaderPort>\\d+):(?<leaderElectionPort>\\d+)(:(?<role>.*?))?(\\|.*?)?(;((?<clientPortAddress>.*?):)?(?<clientPort>\\d+))?");

  private List<Server> servers = new ArrayList<>();
  private String version = "";

  private ZkDynamicConfig() { /* Use static factory methods */ }

  /**
   * Parse a raw multi line config string with the full content of znode /zookeeper/config.
   * @param lines the multi line config string. If empty or null, this will return an empty list
   * @return an instance of ZkDynamicConfig
   */
  public static ZkDynamicConfig parseLines(String lines) {
    ZkDynamicConfig zkDynamicConfig = new ZkDynamicConfig();
    if (!StringUtils.isEmpty(lines)) {
      new BufferedReader(new StringReader(lines)).lines().forEach(l -> {
        if (l.startsWith("version=")) {
          zkDynamicConfig.version = l.split("=")[1];
        }
        if (l.startsWith("server.")) {
          zkDynamicConfig.servers.add(Server.parseLine(l));
        }
      });
    }
    return zkDynamicConfig;
  }

  /**
   * Creates an instance based on a zookeeper connect string on format <code>host:port,host:port[/chroot]</code>
   * @param zkHost zk connect string
   * @return instance of ZkDynamicConfig
   */
  public static ZkDynamicConfig fromZkConnectString(String zkHost) {
    ZkDynamicConfig zkDynamicConfig = new ZkDynamicConfig();
    zkDynamicConfig.servers = Arrays.stream(zkHost.split("/")[0].split(","))
        .map(h -> new ZkDynamicConfig.Server(
            null,
            null,
            null,
            null,
            null,
            h.split(":")[0],
            h.contains(":") ? Integer.parseInt(h.split(":")[1]) : 2181)
        ).collect(Collectors.toList());
    return zkDynamicConfig;
  }

  public List<Server> getServers() {
    return servers;
  }

  public String getVersion() {
    return version;
  }

  public int size() {
    return servers.size();
  }

  /**
   * Object representing one line in Zk dynamic config
   */
  public static class Server {
    public final Integer serverId;
    public final String address;
    public final Integer leaderPort;
    public final Integer leaderElectionPort;
    public final String role;
    public final String clientPortAddress;
    public final Integer clientPort;

    Server(Integer serverId, String address, Integer leaderPort, Integer leaderElectionPort, String role, String clientPortAddress, Integer clientPort) {
      this.serverId = serverId;
      this.address = address;
      this.leaderPort = leaderPort;
      this.leaderElectionPort = leaderElectionPort;
      this.role = role;
      this.clientPortAddress = clientPortAddress;
      this.clientPort = clientPort;
    }

    /**
     * Resolve the most likely address, first trying 'clientPortAddress', falling back to 'address'
     * @return a string with client address, without port
     */
    public String resolveClientPortAddress() {
      return ("0.0.0.0".equals(clientPortAddress) || clientPortAddress == null ? address : clientPortAddress);
    }

    /**
     * Parse a single zk config server line
     */
    public static Server parseLine(String line) {
      Matcher m = linePattern.matcher(line);
      if (!m.matches()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Could not parse dynamic zk config line: " + line);
      }
      String clientPortStr = m.group("clientPort");
      return new Server(
          Integer.parseInt(m.group("serverId")),
          m.group("address"),
          Integer.parseInt(m.group("leaderPort")),
          Integer.parseInt(m.group("leaderElectionPort")),
          m.group("role"),
          m.group("clientPortAddress"),
          clientPortStr != null ? Integer.parseInt(clientPortStr) : null
      );
    }
  }
}
