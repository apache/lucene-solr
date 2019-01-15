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
package org.apache.solr.handler.admin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Zookeeper Status handler, talks to ZK using sockets and four-letter words
 *
 * @since solr 7.5
 */
public final class ZookeeperStatusHandler extends RequestHandlerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int ZOOKEEPER_DEFAULT_PORT = 2181;
  private static final String STATUS_RED = "red";
  private static final String STATUS_GREEN = "green";
  private static final String STATUS_YELLOW = "yellow";
  private static final String STATUS_NA = "N/A";
  private CoreContainer cores;

  public ZookeeperStatusHandler(CoreContainer cc) {
    this.cores = cc;
  }
  
  @Override
  public String getDescription() {
    return "Fetch Zookeeper status";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    NamedList values = rsp.getValues();
    if (cores.isZooKeeperAware()) {
      values.add("zkStatus", getZkStatus(cores.getZkController().getZkServerAddress()));
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The Zookeeper status API is only available in Cloud mode");
    }
  }

  /*
   Gets all info from ZK API and returns as a map
   */
  protected Map<String, Object> getZkStatus(String zkHost) {
    Map<String, Object> zkStatus = new HashMap<>();
    List<String> zookeepers = Arrays.asList(zkHost.split("/")[0].split(","));
    List<Object> details = new ArrayList<>();
    int numOk = 0;
    String status = STATUS_NA;
    int standalone = 0;
    int followers = 0;
    int reportedFollowers = 0;
    int leaders = 0;
    List<String> errors = new ArrayList<>();
    for (String zk : zookeepers) {
      try {
        Map<String, Object> stat = monitorZookeeper(zk);
        details.add(stat);
        if ("true".equals(String.valueOf(stat.get("ok")))) {
          numOk++;
        }
        String state = String.valueOf(stat.get("zk_server_state"));
        if ("follower".equals(state)) {
          followers++;
        } else if ("leader".equals(state)) {
          leaders++;
          reportedFollowers = Integer.parseInt(String.valueOf(stat.get("zk_followers")));
        } else if ("standalone".equals(state)) {
          standalone++;
        }
      } catch (SolrException se) {
        log.warn("Failed talking to zookeeper" + zk, se);
        errors.add(se.getMessage());
        Map<String, Object> stat = new HashMap<>();
        stat.put("host", zk);
        stat.put("ok", false);
        details.add(stat);
      }       
    }
    zkStatus.put("ensembleSize", zookeepers.size());
    zkStatus.put("zkHost", zkHost);
    zkStatus.put("details", details);
    if (followers+leaders > 0 && standalone > 0) {
      status = STATUS_RED;
      errors.add("The zk nodes do not agree on their mode, check details");
    }
    if (standalone > 1) {
      status = STATUS_RED;
      errors.add("Only one zk allowed in standalone mode");
    }
    if (leaders > 1) {
      zkStatus.put("mode", "ensemble");
      status = STATUS_RED;
      errors.add("Only one leader allowed, got " + leaders);
    }
    if (followers > 0 && leaders == 0) {
      zkStatus.put("mode", "ensemble");
      status = STATUS_RED;
      errors.add("We do not have a leader");
    }
    if (leaders > 0 && followers != reportedFollowers) {
      zkStatus.put("mode", "ensemble");
      status = STATUS_RED;
      errors.add("Leader reports " + reportedFollowers + " followers, but we only found " + followers + 
        ". Please check zkHost configuration");
    }
    if (followers+leaders == 0 && standalone == 1) {
      zkStatus.put("mode", "standalone");
    }
    if (followers+leaders > 0 && (zookeepers.size())%2 == 0) {
      if (!STATUS_RED.equals(status)) {
        status = STATUS_YELLOW;
      }
      errors.add("We have an even number of zookeepers which is not recommended");
    }
    if (followers+leaders > 0 && standalone == 0) {
      zkStatus.put("mode", "ensemble");
    }
    if (status.equals(STATUS_NA)) {
      if (numOk == zookeepers.size()) {
        status = STATUS_GREEN;
      } else if (numOk < zookeepers.size() && numOk > zookeepers.size() / 2) {
        status = STATUS_YELLOW;
        errors.add("Some zookeepers are down: " + numOk + "/" + zookeepers.size());
      } else {
        status = STATUS_RED;
        errors.add("Mismatch in number of zookeeper nodes live. numOK=" + numOk + ", expected " + zookeepers.size());
      }
    }
    zkStatus.put("status", status);
    if (!errors.isEmpty()) {
      zkStatus.put("errors", errors);
    }
    return zkStatus;
  }

  private Map<String, Object> monitorZookeeper(String zkHostPort) {
    List<String> lines = getZkRawResponse(zkHostPort, "mntr");
    Map<String, Object> obj = new HashMap<>();
    obj.put("host", zkHostPort);
    obj.put("ok", "imok".equals(getZkRawResponse(zkHostPort, "ruok").get(0)));
    for (String line : lines) {
      obj.put(line.split("\t")[0], line.split("\t")[1]);
    }
    lines = getZkRawResponse(zkHostPort, "conf");
    for (String line : lines) {
      obj.put(line.split("=")[0], line.split("=")[1]);
    }
    return obj;
  }
  
  /**
   * Sends a four-letter-word command to one particular Zookeeper server and returns the response as list of strings
   * @param zkHostPort the host:port for one zookeeper server to access
   * @param fourLetterWordCommand the custom 4-letter command to send to Zookeeper
   * @return a list of lines returned from Zookeeper
   */
  private List<String> getZkRawResponse(String zkHostPort, String fourLetterWordCommand) {
    String[] hostPort = zkHostPort.split(":");
    String host = hostPort[0];
    int port = ZOOKEEPER_DEFAULT_PORT;
    if (hostPort.length > 1) {
      port = Integer.parseInt(hostPort[1]);
    }
    try (
        Socket socket = new Socket(host, port);
        Writer writer = new OutputStreamWriter(socket.getOutputStream(), "utf-8");
        PrintWriter out = new PrintWriter(writer, true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), "utf-8"));) {
      out.println(fourLetterWordCommand);
      List<String> response = in.lines().collect(Collectors.toList());
      log.debug("Got response from ZK on host {} and port {}: {}", host, port, response);
      if (response == null || response.isEmpty()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Empty response from Zookeeper " + zkHostPort);
      }
      return response;
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed talking to Zookeeper " + zkHostPort, e);
    }
  }
}
