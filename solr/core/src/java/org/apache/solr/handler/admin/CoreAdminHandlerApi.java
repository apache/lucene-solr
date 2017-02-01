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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.handler.admin.CoreAdminHandlerApi.EndPoint.CORES_COMMANDS;
import static org.apache.solr.handler.admin.CoreAdminHandlerApi.EndPoint.CORES_STATUS;
import static org.apache.solr.handler.admin.CoreAdminHandlerApi.EndPoint.NODEAPIS;
import static org.apache.solr.handler.admin.CoreAdminHandlerApi.EndPoint.NODEINVOKE;
import static org.apache.solr.handler.admin.CoreAdminHandlerApi.EndPoint.PER_CORE_COMMANDS;
import static org.apache.solr.handler.admin.CoreAdminOperation.CREATE_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.FORCEPREPAREFORLEADERSHIP_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.INVOKE_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.MERGEINDEXES_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.OVERSEEROP_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.PREPRECOVERY_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.REJOINLEADERELECTION_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.RELOAD_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.RENAME_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.REQUESTAPPLYUPDATES_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.REQUESTBUFFERUPDATES_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.REQUESTRECOVERY_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.REQUESTSTATUS_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.REQUESTSYNCSHARD_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.SPLIT_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.STATUS_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.SWAP_OP;
import static org.apache.solr.handler.admin.CoreAdminOperation.UNLOAD_OP;

public class CoreAdminHandlerApi extends BaseHandlerApiSupport {
  private final CoreAdminHandler handler;

  public CoreAdminHandlerApi(CoreAdminHandler handler) {
    this.handler = handler;
  }

  enum Cmd implements ApiCommand {
    CREATE(CORES_COMMANDS, POST, CREATE_OP, null, ImmutableMap.of("config", "configSet")),
    UNLOAD(PER_CORE_COMMANDS, POST, UNLOAD_OP, null, null),
    RELOAD(PER_CORE_COMMANDS, POST, RELOAD_OP, null, null),
    STATUS(CORES_STATUS, GET, STATUS_OP),
    SWAP(PER_CORE_COMMANDS, POST, SWAP_OP, null, ImmutableMap.of("other", "with")),
    RENAME(PER_CORE_COMMANDS, POST, RENAME_OP, null, null),
    MERGEINDEXES(PER_CORE_COMMANDS, POST, MERGEINDEXES_OP, "merge-indexes", null),
    SPLIT(PER_CORE_COMMANDS, POST, SPLIT_OP, null, ImmutableMap.of("split.key", "splitKey")),
    PREPRECOVERY(PER_CORE_COMMANDS, POST, PREPRECOVERY_OP, "prep-recovery", null),
    REQUESTRECOVERY(PER_CORE_COMMANDS, POST, REQUESTRECOVERY_OP, null, null),
    REQUESTSYNCSHARD(PER_CORE_COMMANDS, POST, REQUESTSYNCSHARD_OP, "request-sync-shard", null),
    REQUESTBUFFERUPDATES(PER_CORE_COMMANDS, POST, REQUESTBUFFERUPDATES_OP, "request-buffer-updates", null),
    REQUESTAPPLYUPDATES(PER_CORE_COMMANDS, POST, REQUESTAPPLYUPDATES_OP, "request-apply-updates", null),
    REQUESTSTATUS(PER_CORE_COMMANDS, POST, REQUESTSTATUS_OP, null, null),
    OVERSEEROP(NODEAPIS, POST, OVERSEEROP_OP, "overseer-op", null),
    REJOINLEADERELECTION(NODEAPIS, POST, REJOINLEADERELECTION_OP, "rejoin-leader-election", null),
    INVOKE(NODEINVOKE, GET, INVOKE_OP, null, null),
    FORCEPREPAREFORLEADERSHIP(PER_CORE_COMMANDS, POST, FORCEPREPAREFORLEADERSHIP_OP, "force-prepare-for-leadership", null);

    public final String commandName;
    public final BaseHandlerApiSupport.V2EndPoint endPoint;
    public final SolrRequest.METHOD method;
    public final Map<String, String> paramstoAttr;
    final CoreAdminOperation target;


    Cmd(EndPoint endPoint, SolrRequest.METHOD method, CoreAdminOperation target) {
      this.endPoint = endPoint;
      this.method = method;
      this.target = target;
      commandName = null;
      paramstoAttr = Collections.EMPTY_MAP;

    }


    Cmd(EndPoint endPoint, SolrRequest.METHOD method, CoreAdminOperation target, String commandName,
        Map<String, String> paramstoAttr) {
      this.commandName = commandName == null ? target.action.toString().toLowerCase(Locale.ROOT) : commandName;
      this.endPoint = endPoint;
      this.method = method;
      this.target = target;
      this.paramstoAttr = paramstoAttr == null ? Collections.EMPTY_MAP : paramstoAttr;
    }

    @Override
    public String getName() {
      return commandName;
    }

    @Override
    public SolrRequest.METHOD getHttpMethod() {
      return method;
    }

    @Override
    public V2EndPoint getEndPoint() {
      return endPoint;
    }

    @Override
    public String getParamSubstitute(String param) {
      return paramstoAttr.containsKey(param) ? paramstoAttr.get(param) : param;
    }

    @Override
    public void invoke(SolrQueryRequest req, SolrQueryResponse rsp, BaseHandlerApiSupport apiHandler) throws Exception {
      target.execute(new CoreAdminHandler.CallInfo(((CoreAdminHandlerApi) apiHandler).handler,
          req,
          rsp,
          target));

    }

  }



  enum EndPoint implements BaseHandlerApiSupport.V2EndPoint {
    CORES_STATUS("cores.Status"),
    CORES_COMMANDS("cores.Commands"),
    PER_CORE_COMMANDS("cores.core.Commands"),
    NODEINVOKE("node.invoke"),
    NODEAPIS("node.Commands")
    ;

    final String specName;

    EndPoint(String specName) {
      this.specName = specName;
    }

    @Override
    public String getSpecName() {
      return specName;
    }
  }


  @Override
  protected List<ApiCommand> getCommands() {
    return Arrays.asList(Cmd.values());
  }

  @Override
  protected List<V2EndPoint> getEndPoints() {
    return Arrays.asList(EndPoint.values());
  }


}
