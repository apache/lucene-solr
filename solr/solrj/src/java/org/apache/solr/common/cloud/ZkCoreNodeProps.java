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

import java.util.Objects;

public class ZkCoreNodeProps {
  private final ZkNodeProps nodeProps;
  
  public ZkCoreNodeProps(ZkNodeProps nodeProps) {
    this.nodeProps = nodeProps;
  }
  
  public String getCoreUrl() {
    return getCoreUrl(this.nodeProps);
  }
  
  public String getNodeName() {
    return nodeProps.getStr(ZkStateReader.NODE_NAME_PROP);
  }

  public String getState() {
    return nodeProps.getStr(ZkStateReader.STATE_PROP);
  }

  public String getBaseUrl() {
    return getBaseUrl(this.nodeProps);
  }
  
  public String getCoreName() {
    return nodeProps.getStr(ZkStateReader.CORE_NAME_PROP);
  }

  private static String getBaseUrl(ZkNodeProps nodeProps) {
    String baseUrl = null;
    final String nodeName = nodeProps.getStr(ZkStateReader.NODE_NAME_PROP);
    if (nodeName != null) {
      baseUrl = UrlScheme.INSTANCE.getBaseUrlForNodeName(nodeName);
    } else if (nodeProps.containsKey(ZkStateReader.BASE_URL_PROP)) {
      baseUrl = UrlScheme.INSTANCE.applyUrlScheme(nodeProps.getStr(ZkStateReader.BASE_URL_PROP));
    }
    return baseUrl;
  }
  
  public static String getCoreUrl(ZkNodeProps nodeProps) {
    String baseUrl = getBaseUrl(nodeProps);
    return baseUrl != null ? getCoreUrl(baseUrl, nodeProps.getStr(ZkStateReader.CORE_NAME_PROP)) : null;
  }
  
  public static String getCoreUrl(String baseUrl, String coreName) {
    Objects.requireNonNull(baseUrl,"baseUrl must not be null");
    StringBuilder sb = new StringBuilder();
    sb.append(baseUrl);
    if (!baseUrl.endsWith("/")) sb.append("/");
    sb.append(coreName != null ? coreName : "");
    if (!(sb.substring(sb.length() - 1).equals("/"))) sb.append("/");
    return sb.toString();
  }

  @Override
  public String toString() {
    return nodeProps.toString();
  }

  public ZkNodeProps getNodeProps() {
    return nodeProps;
  }

  public boolean isLeader() {
    return nodeProps.containsKey(ZkStateReader.LEADER_PROP);
  }
}
