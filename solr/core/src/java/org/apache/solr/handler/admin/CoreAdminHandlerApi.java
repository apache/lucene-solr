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
import java.util.List;

import org.apache.solr.client.solrj.request.CollectionApiMapping;
import org.apache.solr.client.solrj.request.CollectionApiMapping.V2EndPoint;
import org.apache.solr.client.solrj.request.CoreApiMapping;
import org.apache.solr.client.solrj.request.CoreApiMapping.Meta;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

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
    CREATE(Meta.CREATE, CREATE_OP),
    UNLOAD(Meta.UNLOAD, UNLOAD_OP),
    RELOAD(Meta.RELOAD, RELOAD_OP),
    STATUS(Meta.STATUS, STATUS_OP),
    SWAP(Meta.SWAP, SWAP_OP),
    RENAME(Meta.RENAME, RENAME_OP),
    MERGEINDEXES(Meta.MERGEINDEXES, MERGEINDEXES_OP),
    SPLIT(Meta.SPLIT, SPLIT_OP),
    PREPRECOVERY(Meta.PREPRECOVERY, PREPRECOVERY_OP),
    REQUESTRECOVERY(Meta.REQUESTRECOVERY, REQUESTRECOVERY_OP),
    REQUESTSYNCSHARD(Meta.REQUESTSYNCSHARD, REQUESTSYNCSHARD_OP),
    REQUESTBUFFERUPDATES(Meta.REQUESTBUFFERUPDATES, REQUESTBUFFERUPDATES_OP),
    REQUESTAPPLYUPDATES(Meta.REQUESTAPPLYUPDATES, REQUESTAPPLYUPDATES_OP),
    REQUESTSTATUS(Meta.REQUESTSTATUS, REQUESTSTATUS_OP),
    OVERSEEROP(Meta.OVERSEEROP, OVERSEEROP_OP),
    REJOINLEADERELECTION(Meta.REJOINLEADERELECTION, REJOINLEADERELECTION_OP),
    INVOKE(Meta.INVOKE, INVOKE_OP),
    FORCEPREPAREFORLEADERSHIP(Meta.FORCEPREPAREFORLEADERSHIP, FORCEPREPAREFORLEADERSHIP_OP);

    public final Meta meta;
    public final CoreAdminOperation target;


    Cmd(Meta meta, CoreAdminOperation target) {
      this.meta = meta;
      this.target = target;
    }


    @Override
    public CollectionApiMapping.CommandMeta meta() {
      return meta;
    }

    @Override
    public void invoke(SolrQueryRequest req, SolrQueryResponse rsp, BaseHandlerApiSupport apiHandler) throws Exception {
      target.execute(new CoreAdminHandler.CallInfo(((CoreAdminHandlerApi) apiHandler).handler,
          req,
          rsp,
          target));

    }
  }


  @Override
  protected List<ApiCommand> getCommands() {
    return Arrays.asList(Cmd.values());
  }

  @Override
  protected List<V2EndPoint> getEndPoints() {
    return Arrays.asList(CoreApiMapping.EndPoint.values());
  }


}
