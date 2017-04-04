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

package org.apache.solr.cloud.autoscaling;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.cloud.rule.ServerSnitchContext;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.cloud.rule.SnitchContext;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

//uses metrics API to get node information
public class AutoScalingSnitch extends ImplicitSnitch {


  @Override
  protected void getRemoteInfo(String solrNode, Set<String> requestedTags, SnitchContext ctx) {
    ServerSnitchContext snitchContext = (ServerSnitchContext) ctx;
    List<String> groups = new ArrayList<>();
    List<String> prefixes = new ArrayList<>();
    if (requestedTags.contains(DISK)) {
      groups.add("solr.node");
      prefixes.add("CONTAINER.fs.usableSpace");
    }
    if (requestedTags.contains(CORES)) {
      groups.add("solr.core");
      prefixes.add("CORE.coreName");
    }
    if(groups.isEmpty() || prefixes.isEmpty()) return;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("group", StrUtils.join(groups, ','));
    params.add("prefix", StrUtils.join(prefixes,','));

    try {
      SimpleSolrResponse rsp = snitchContext.invoke(solrNode, CommonParams.METRICS_PATH, params);
      Map m = rsp.nl.asMap(4);
      if(requestedTags.contains(DISK)){
        Number n = (Number) Utils.getObjectByPath(m,true, "metrics/solr.node/CONTAINER.fs.usableSpace");
        if(n != null) ctx.getTags().put(DISK, n.longValue());
      }
      if(requestedTags.contains(CORES)){
        int count = 0;
        Map cores  = (Map) m.get("metrics");
        for (Object o : cores.keySet()) {
          if(o.toString().startsWith("solr.core.")) count++;
        }
        ctx.getTags().put(CORES, count);
      }

    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }

  }
}
