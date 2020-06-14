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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the context provided to the snitches to interact with the system. This is a per-node-per-snitch
 * instance.
 */
public abstract class SnitchContext implements RemoteCallback {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Map<String, Object> tags = new HashMap<>();
  private String node;
  private Map<String, Object> session;
  public final SnitchInfo snitchInfo;
  public Exception exception;


  public SnitchContext(SnitchInfo perSnitch, String node, Map<String, Object> session) {
    this.snitchInfo = perSnitch;
    this.node = node;
    this.session = session;
  }

  public Map<String, Object> getTags() {
    return tags;
  }

  public void store(String s, Object val) {
    if (session != null) session.put(s, val);

  }

  public Object retrieve(String s) {
    return session != null ? session.get(s) : null;

  }
  public Map<String, Object> getNodeValues(String node, Collection<String> tags){
    return Collections.emptyMap();
  }

  @SuppressWarnings({"rawtypes"})
  public abstract Map getZkJson(String path) throws KeeperException, InterruptedException;

  public String getNode() {
    return node;
  }

  /**
   * make a call to solrnode/admin/cores with the given params and give a callback. This is designed to be
   * asynchronous because the system would want to batch the calls made to any given node
   *
   * @param node     The node for which this call is made
   * @param params   The params to be passed to the Snitch counterpart
   * @param klas     The  name of the class to be invoked in the remote node
   * @param callback The callback to be called when the response is obtained from remote node.
   *                 If this is passed as null the entire response map will be added as tags
   */
  @Deprecated
  public void invokeRemote(String node, ModifiableSolrParams params, String klas, RemoteCallback callback) {};


  @Override
  public void remoteCallback(SnitchContext ctx, Map<String, Object> returnedVal) {
    tags.putAll(returnedVal);
  }

  public String getErrMsg() {
    return exception == null ? null : exception.getMessage();
  }

  public static abstract class SnitchInfo {
    private final Map<String, Object> conf;

    protected SnitchInfo(Map<String, Object> conf) {
      this.conf = conf;
    }

    public abstract Set<String> getTagNames();

  }
}
