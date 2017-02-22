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

package org.apache.solr.handler;


import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.common.params.CommonParams.JSON;

public class ExportHandler extends SearchHandler {
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    try {
      super.handleRequestBody(req, rsp);
    } catch (Exception e) {
      rsp.setException(e);
    }
    String wt = req.getParams().get(CommonParams.WT, JSON);
    if("xsort".equals(wt)) wt = JSON;
    Map<String, String> map = new HashMap<>(1);
    map.put(CommonParams.WT, ReplicationHandler.FILE_STREAM);
    req.setParams(SolrParams.wrapDefaults(new MapSolrParams(map),req.getParams()));
    rsp.add(ReplicationHandler.FILE_STREAM, new ExportWriter(req, rsp, wt));
  }
}
