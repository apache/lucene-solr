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

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InvokeOp implements CoreAdminHandler.CoreAdminOp {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static Map<String, Object> invokeAClass(SolrQueryRequest req, String c) {
    SolrResourceLoader loader = null;
    if (req.getCore() != null) loader = req.getCore().getResourceLoader();
    else if (req.getContext().get(CoreContainer.class.getName()) != null) {
      CoreContainer cc = (CoreContainer) req.getContext().get(CoreContainer.class.getName());
      loader = cc.getResourceLoader();
    }

    CoreAdminHandler.Invocable invokable = loader.newInstance(c, CoreAdminHandler.Invocable.class);
    Map<String, Object> result = invokable.invoke(req);
    log.info("Invocable_invoked {}", result);
    return result;
  }

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    String[] klas = it.req.getParams().getParams("class");
    if (klas == null || klas.length == 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "class is a required param");
    }
    for (String c : klas) {
      Map<String, Object> result = invokeAClass(it.req, c);
      it.rsp.add(c, result);
    }
  }
}
