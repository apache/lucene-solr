package org.apache.solr.common.cloud;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class ZkCoreNodeProps {
  private ZkNodeProps nodeProps;
  
  public ZkCoreNodeProps(ZkNodeProps nodeProps) {
    this.nodeProps = nodeProps;
  }
  
  // may return null
  public String getCoreUrl() {
    return getCoreUrl(nodeProps.get(ZkStateReader.BASE_URL_PROP), nodeProps.get(ZkStateReader.CORE_NAME_PROP));
  }
  
  public String getNodeName() {
    return nodeProps.get(ZkStateReader.NODE_NAME_PROP);
  }

  public String getState() {
    return nodeProps.get(ZkStateReader.STATE_PROP);
  }

  public String getBaseUrl() {
    return nodeProps.get(ZkStateReader.BASE_URL_PROP);
  }
  
  public String getCoreName() {
    return nodeProps.get(ZkStateReader.CORE_NAME_PROP);
  }
  
  public static String getCoreUrl(String baseUrl, String coreName) {
    StringBuilder sb = new StringBuilder();
    if (baseUrl == null) return null;
    if (coreName == null) return null;
    sb.append(baseUrl);
    if (!baseUrl.endsWith("/")) sb.append("/");
    sb.append(coreName == null ? "" : coreName);
    if (!(sb.substring(sb.length() - 1).equals("/"))) sb.append("/");
    
    return sb.toString();
  }

  @Override
  public String toString() {
    return nodeProps.toString();
  }

  public String getCoreNodeName() {
    return getNodeName() + "_" + getCoreName();
  }

  public ZkNodeProps getNodeProps() {
    return nodeProps;
  }


}
