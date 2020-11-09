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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;


class StatusOp implements CoreAdminHandler.CoreAdminOp {
  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    SolrParams params = it.req.getParams();

    String cname = params.get(CoreAdminParams.CORE);
    String indexInfo = params.get(CoreAdminParams.INDEX_INFO);
    boolean isIndexInfoNeeded = Boolean.parseBoolean(null == indexInfo ? "true" : indexInfo);
    NamedList<Object> status = new SimpleOrderedMap<>();
    Map<String, Exception> failures = new HashMap<>();
    for (Map.Entry<String, CoreContainer.CoreLoadFailure> failure : it.handler.coreContainer.getCoreInitFailures().entrySet()) {
      failures.put(failure.getKey(), failure.getValue().exception);
    }
    if (cname == null) {
      List<String> nameList = it.handler.coreContainer.getAllCoreNames();
      nameList.sort(null);
      for (String name : nameList) {
        status.add(name, CoreAdminOperation.getCoreStatus(it.handler.coreContainer, name, isIndexInfoNeeded));
      }
      it.rsp.add("initFailures", failures);
    } else {
      failures = failures.containsKey(cname)
          ? Collections.singletonMap(cname, failures.get(cname))
              : Collections.<String, Exception>emptyMap();
          it.rsp.add("initFailures", failures);
          status.add(cname, CoreAdminOperation.getCoreStatus(it.handler.coreContainer, cname, isIndexInfoNeeded));
    }
    it.rsp.add("status", status);
  }
}
