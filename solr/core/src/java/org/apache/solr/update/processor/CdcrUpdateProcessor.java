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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Extends {@link org.apache.solr.update.processor.DistributedUpdateProcessor} to force peer sync logic
 * for every updates. This ensures that the version parameter sent by the source cluster is kept
 * by the target cluster.
 * </p>
 * @deprecated since 8.6
 */
@Deprecated
public class CdcrUpdateProcessor extends DistributedZkUpdateProcessor {

  public static final String CDCR_UPDATE = "cdcr.update";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public CdcrUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(req, rsp, next);
  }

  @Override
  protected boolean versionAdd(AddUpdateCommand cmd) throws IOException {
    /*
    temporarily set the PEER_SYNC flag so that DistributedUpdateProcessor.versionAdd doesn't execute leader logic
    but the else part of that if. That way version remains preserved.

    we cannot set the flag for the whole processAdd method because DistributedUpdateProcessor.setupRequest() would set
    isLeader to false which wouldn't work
     */
    if (cmd.getReq().getParams().get(CDCR_UPDATE) != null) {
      cmd.setFlags(cmd.getFlags() | UpdateCommand.PEER_SYNC); // we need super.versionAdd() to set leaderLogic to false
    }

    boolean result = super.versionAdd(cmd);

    // unset the flag to avoid unintended consequences down the chain
    if (cmd.getReq().getParams().get(CDCR_UPDATE) != null) {
      cmd.setFlags(cmd.getFlags() & ~UpdateCommand.PEER_SYNC);
    }

    return result;
  }

  @Override
  protected boolean versionDelete(DeleteUpdateCommand cmd) throws IOException {
    /*
    temporarily set the PEER_SYNC flag so that DistributedUpdateProcessor.deleteAdd doesn't execute leader logic
    but the else part of that if. That way version remains preserved.

    we cannot set the flag for the whole processDelete method because DistributedUpdateProcessor.setupRequest() would set
    isLeader to false which wouldn't work
     */
    if (cmd.getReq().getParams().get(CDCR_UPDATE) != null) {
      cmd.setFlags(cmd.getFlags() | UpdateCommand.PEER_SYNC); // we need super.versionAdd() to set leaderLogic to false
    }

    boolean result = super.versionDelete(cmd);

    // unset the flag to avoid unintended consequences down the chain
    if (cmd.getReq().getParams().get(CDCR_UPDATE) != null) {
      cmd.setFlags(cmd.getFlags() & ~UpdateCommand.PEER_SYNC);
    }

    return result;
  }

  protected ModifiableSolrParams filterParams(SolrParams params) {
    ModifiableSolrParams result = super.filterParams(params);
    if (params.get(CDCR_UPDATE) != null) {
      result.set(CDCR_UPDATE, "");
//      if (params.get(DistributedUpdateProcessor.VERSION_FIELD) == null) {
//        log.warn("+++ cdcr.update but no version field, params are: " + params);
//      } else {
//        log.info("+++ cdcr.update version present, params are: " + params);
//      }
      result.set(CommonParams.VERSION_FIELD, params.get(CommonParams.VERSION_FIELD));
    }

    return result;
  }

  @Override
  protected void versionDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    /*
    temporarily set the PEER_SYNC flag so that DistributedUpdateProcessor.versionDeleteByQuery doesn't execute leader logic
    That way version remains preserved.

     */
    if (cmd.getReq().getParams().get(CDCR_UPDATE) != null) {
      cmd.setFlags(cmd.getFlags() | UpdateCommand.PEER_SYNC); // we need super.versionDeleteByQuery() to set leaderLogic to false
    }

    super.versionDeleteByQuery(cmd);

    // unset the flag to avoid unintended consequences down the chain
    if (cmd.getReq().getParams().get(CDCR_UPDATE) != null) {
      cmd.setFlags(cmd.getFlags() & ~UpdateCommand.PEER_SYNC);
    }
  }
}

