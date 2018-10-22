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
package org.apache.solr.client.solrj.request;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;


/**
 *
 *
 **/
public abstract class AbstractUpdateRequest extends SolrRequest<UpdateResponse> implements IsUpdateRequest {
  protected ModifiableSolrParams params;
  protected int commitWithin = -1;

  public enum ACTION {
    COMMIT,
    OPTIMIZE
  }

  public AbstractUpdateRequest(METHOD m, String path) {
    super(m, path);
  }

  /** Sets appropriate parameters for the given ACTION */
  public AbstractUpdateRequest setAction(ACTION action, boolean waitFlush, boolean waitSearcher ) {
    return setAction(action, waitFlush, waitSearcher, 1);
  }

  public AbstractUpdateRequest setAction(ACTION action, boolean waitFlush, boolean waitSearcher, boolean softCommit ) {
    return setAction(action, waitFlush, waitSearcher, softCommit, 1);
  }

  public AbstractUpdateRequest setAction(ACTION action, boolean waitFlush, boolean waitSearcher, int maxSegments ) {
    return setAction(action, waitFlush, waitSearcher, false, maxSegments);
  }

  public AbstractUpdateRequest setAction(ACTION action, boolean waitFlush, boolean waitSearcher, boolean softCommit, int maxSegments ) {
    if (params == null)
      params = new ModifiableSolrParams();

    if( action == ACTION.OPTIMIZE ) {
      params.set( UpdateParams.OPTIMIZE, "true" );
      params.set(UpdateParams.MAX_OPTIMIZE_SEGMENTS, maxSegments);
    }
    else if( action == ACTION.COMMIT ) {
      params.set( UpdateParams.COMMIT, "true" );
      params.set( UpdateParams.SOFT_COMMIT, String.valueOf(softCommit));
    }
    params.set( UpdateParams.WAIT_SEARCHER, String.valueOf(waitSearcher));
    return this;
  }

  public AbstractUpdateRequest setAction(ACTION action, boolean waitFlush, boolean waitSearcher, int maxSegments , boolean softCommit, boolean expungeDeletes) {
    setAction(action, waitFlush, waitSearcher,softCommit,maxSegments) ;
    params.set(UpdateParams.EXPUNGE_DELETES, String.valueOf(expungeDeletes));
    return this;
  }

  public AbstractUpdateRequest setAction(ACTION action, boolean waitFlush, boolean waitSearcher, int maxSegments , boolean expungeDeletes) {
    return setAction(action, waitFlush, waitSearcher,maxSegments,false,expungeDeletes);
  }

  public AbstractUpdateRequest setAction(ACTION action, boolean waitFlush, boolean waitSearcher, int maxSegments, boolean softCommit, boolean expungeDeletes, boolean openSearcher) {
    setAction(action, waitFlush, waitSearcher, maxSegments, softCommit, expungeDeletes);
    params.set(UpdateParams.OPEN_SEARCHER, String.valueOf(openSearcher));
    return this;
  }

  /**
   * @since Solr 1.4
   */
  public AbstractUpdateRequest rollback() {
    if (params == null)
      params = new ModifiableSolrParams();

    params.set( UpdateParams.ROLLBACK, "true" );
    return this;
  }

  public void setParam(String param, String value) {
    if (params == null)
      params = new ModifiableSolrParams();
    params.set(param, value);
  }

  /** Sets the parameters for this update request, overwriting any previous */
  public void setParams(ModifiableSolrParams params) {
    this.params = params;
  }

  @Override
  public ModifiableSolrParams getParams() {
    return params;
  }

  @Override
  protected UpdateResponse createResponse(SolrClient client) {
    return new UpdateResponse();
  }

  public boolean isWaitSearcher() {
    return params != null && params.getBool(UpdateParams.WAIT_SEARCHER, false);
  }

  public ACTION getAction() {
    if (params==null) return null;
    if (params.getBool(UpdateParams.COMMIT, false)) return ACTION.COMMIT;
    if (params.getBool(UpdateParams.OPTIMIZE, false)) return ACTION.OPTIMIZE;
    return null;
  }

  public void setWaitSearcher(boolean waitSearcher) {
    setParam( UpdateParams.WAIT_SEARCHER, waitSearcher+"" );
  }

  public int getCommitWithin() {
    return commitWithin;
  }

  public AbstractUpdateRequest setCommitWithin(int commitWithin) {
    this.commitWithin = commitWithin;
    return this;
  }


}
