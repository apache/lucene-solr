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
package org.apache.solr.common.cloud.rule;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//This is the client-side component of the snitch
public class ImplicitSnitch extends Snitch {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final Pattern hostAndPortPattern = Pattern.compile("(?:https?://)?([^:]+):(\\d+)");

  //well known tags
  public static final String NODE = "node";
  public static final String PORT = "port";
  public static final String HOST = "host";
  public static final String CORES = "cores";
  public static final String DISK = "freedisk";
  public static final String ROLE = "role";
  public static final String NODEROLE = "nodeRole";
  public static final String SYSPROP = "sysprop.";
  public static final String SYSLOADAVG = "sysLoadAvg";
  public static final String HEAPUSAGE = "heapUsage";
  public static final String DISKTYPE = "diskType";
  public static final List<String> IP_SNITCHES = Collections.unmodifiableList(Arrays.asList("ip_1", "ip_2", "ip_3", "ip_4"));
  public static final Set<String> tags = Set.of(NODE, PORT, HOST, CORES, DISK, ROLE, "ip_1", "ip_2", "ip_3", "ip_4");

  @Override
  public void getTags(String solrNode, Set<String> requestedTags, SnitchContext ctx) {
    try {
      if (requestedTags.contains(NODE)) ctx.getTags().put(NODE, solrNode);
      if (requestedTags.contains(HOST)) {
        Matcher hostAndPortMatcher = hostAndPortPattern.matcher(solrNode);
        if (hostAndPortMatcher.find()) ctx.getTags().put(HOST, hostAndPortMatcher.group(1));
      }
      if (requestedTags.contains(PORT)) {
        Matcher hostAndPortMatcher = hostAndPortPattern.matcher(solrNode);
        if (hostAndPortMatcher.find()) ctx.getTags().put(PORT, hostAndPortMatcher.group(2));
      }
      if (requestedTags.contains(ROLE)) fillRole(solrNode, ctx, ROLE);
      if (requestedTags.contains(NODEROLE)) fillRole(solrNode, ctx, NODEROLE);// for new policy framework

      addIpTags(solrNode, requestedTags, ctx);

      getRemoteInfo(solrNode, requestedTags, ctx);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  protected void getRemoteInfo(String solrNode, Set<String> requestedTags, SnitchContext ctx) {
    HashMap<String, Object> params = new HashMap<>();
    if (requestedTags.contains(CORES)) params.put(CORES, "1");
    if (requestedTags.contains(DISK)) params.put(DISK, "1");
    for (String tag : requestedTags) {
      if (tag.startsWith(SYSPROP)) params.put(tag, tag.substring(SYSPROP.length()));
    }

    if (params.size() > 0) {
      Map<String, Object> vals = ctx.getNodeValues(solrNode, params.keySet());
      for (Map.Entry<String, Object> e : vals.entrySet()) {
        if(e.getValue() != null) params.put(e.getKey(), e.getValue());
      }
    }
    ctx.getTags().putAll(params);
  }

  private void fillRole(String solrNode, SnitchContext ctx, String key) throws KeeperException, InterruptedException {
    Map roles = (Map) ctx.retrieve(ZkStateReader.ROLES); // we don't want to hit the ZK for each node
    // so cache and reuse
    try {
      if (roles == null) roles = ctx.getZkJson(ZkStateReader.ROLES);
      cacheRoles(solrNode, ctx, key, roles);
    } catch (KeeperException.NoNodeException e) {
      cacheRoles(solrNode, ctx, key, Collections.emptyMap());
    }
  }

  private void cacheRoles(String solrNode, SnitchContext ctx, String key, Map roles) {
    ctx.store(ZkStateReader.ROLES, roles);
    if (roles != null) {
      for (Object o : roles.entrySet()) {
        Map.Entry e = (Map.Entry) o;
        if (e.getValue() instanceof List) {
          if (((List) e.getValue()).contains(solrNode)) {
            ctx.getTags().put(key, e.getKey());
            break;
          }
        }
      }
    }
  }

  private static final String HOST_FRAG_SEPARATOR_REGEX = "\\.";

  @Override
  public boolean isKnownTag(String tag) {
    return tags.contains(tag) ||
        tag.startsWith(SYSPROP);
  }

  private void addIpTags(String solrNode, Set<String> requestedTags, SnitchContext context) {

    List<String> requestedHostTags = new ArrayList<>();
    for (String tag : requestedTags) {
      if (IP_SNITCHES.contains(tag)) {
        requestedHostTags.add(tag);
      }
    }

    if (requestedHostTags.isEmpty()) {
      return;
    }

    String[] ipFragments = getIpFragments(solrNode);

    if (ipFragments == null) {
      return;
    }

    int ipSnitchCount = IP_SNITCHES.size();
    for (int i = 0; i < ipSnitchCount; i++) {
      String currentTagValue = ipFragments[i];
      String currentTagKey = IP_SNITCHES.get(ipSnitchCount - i - 1);

      if (requestedHostTags.contains(currentTagKey)) {
        context.getTags().put(currentTagKey, currentTagValue);
      }

    }

  }

  private String[] getIpFragments(String solrNode) {
    Matcher hostAndPortMatcher = hostAndPortPattern.matcher(solrNode);
    if (hostAndPortMatcher.find()) {
      String host = hostAndPortMatcher.group(1);
      if (host != null) {
        String ip = getHostIp(host);
        if (ip != null) {
          return ip.split(HOST_FRAG_SEPARATOR_REGEX); //IPv6 support will be provided by SOLR-8523
        }
      }
    }

    log.warn("Failed to match host IP address from node URL [{}] using regex [{}]", solrNode, hostAndPortPattern.pattern());
    return null;
  }

  public String getHostIp(String host) {
    try {
      InetAddress address = InetAddress.getByName(host);
      return address.getHostAddress();
    } catch (Exception e) {
      log.warn("Failed to get IP address from host [{}], with exception [{}] ", host, e);
      return null;
    }
  }

}
