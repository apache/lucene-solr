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
package org.apache.solr.core;

import java.io.IOException;

import org.apache.solr.handler.DumpRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

public class BlobStoreTestRequestHandler extends DumpRequestHandler implements Runnable, SolrCoreAware {

  private SolrCore core;

  private long version = 0;
  private String watchedVal = null;

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    super.handleRequestBody(req, rsp);
    rsp.add("class", this.getClass().getName());
    rsp.add("x", watchedVal);
  }

  @Override
  public void run() {
    RequestParams p = core.getSolrConfig().getRequestParams();
    RequestParams.ParamSet v = p.getParams("watched");
    if (v == null) {
      watchedVal = null;
      version = -1;
      return;
    }
    if (v.getVersion() != version) {
      watchedVal = v.getParams(PluginInfo.DEFAULTS).get("x");
    }
  }

  @Override
  public void inform(SolrCore core) {
    this.core = core;
    core.addConfListener(this);
    run();

  }

}
