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

import java.io.IOException;
import java.util.*;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;

/**
 * Common helper functions for RequestHandlers
 * 
 *
 * @since solr 1.2
 */
public class RequestHandlerUtils
{
  /**
   * A common way to mark the response format as experimental
   */
  public static void addExperimentalFormatWarning( SolrQueryResponse rsp )
  {
    rsp.add( "WARNING", "This response format is experimental.  It is likely to change in the future." ); 
  }


  /**
   * Check the request parameters and decide if it should commit or optimize.
   * If it does, it will check other related parameters such as "waitFlush" and "waitSearcher"
   */
  public static boolean handleCommit(SolrQueryRequest req, UpdateRequestProcessor processor, SolrParams params, boolean force ) throws IOException
  {
    if( params == null) {
      params = new MapSolrParams( new HashMap<String, String>() ); 
    }
    
    boolean optimize = params.getBool( UpdateParams.OPTIMIZE, false );
    boolean commit   = params.getBool( UpdateParams.COMMIT,   false );
    boolean softCommit = params.getBool( UpdateParams.SOFT_COMMIT,   false );
    boolean prepareCommit = params.getBool( UpdateParams.PREPARE_COMMIT,   false );


    if( optimize || commit || softCommit || prepareCommit || force ) {
      CommitUpdateCommand cmd = new CommitUpdateCommand(req, optimize );
      updateCommit(cmd, params);
      processor.processCommit( cmd );
      return true;
    }
    
    
    return false;
  }

  
  private static Set<String> commitParams = new HashSet<>(Arrays.asList(new String[]{UpdateParams.OPEN_SEARCHER, UpdateParams.WAIT_SEARCHER, UpdateParams.SOFT_COMMIT, UpdateParams.EXPUNGE_DELETES, UpdateParams.MAX_OPTIMIZE_SEGMENTS, UpdateParams.PREPARE_COMMIT}));

  public static void validateCommitParams(SolrParams params) {
    Iterator<String> i = params.getParameterNamesIterator();
    while (i.hasNext()) {
      String key = i.next();
      if (!commitParams.contains(key)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown commit parameter '" + key + "'");
      }
    }
  }
  
  /**
   * Modify UpdateCommand based on request parameters
   */
  public static void updateCommit(CommitUpdateCommand cmd, SolrParams params) {
    if( params == null ) return;

    cmd.openSearcher = params.getBool( UpdateParams.OPEN_SEARCHER, cmd.openSearcher );
    cmd.waitSearcher = params.getBool( UpdateParams.WAIT_SEARCHER, cmd.waitSearcher );
    cmd.softCommit = params.getBool( UpdateParams.SOFT_COMMIT, cmd.softCommit );
    cmd.expungeDeletes = params.getBool( UpdateParams.EXPUNGE_DELETES, cmd.expungeDeletes );
    cmd.maxOptimizeSegments = params.getInt( UpdateParams.MAX_OPTIMIZE_SEGMENTS, cmd.maxOptimizeSegments );
    cmd.prepareCommit = params.getBool( UpdateParams.PREPARE_COMMIT,   cmd.prepareCommit );
  }


  /**
   * @since Solr 1.4
   */
  public static boolean handleRollback(SolrQueryRequest req, UpdateRequestProcessor processor, SolrParams params, boolean force ) throws IOException
  {
    if( params == null ) {
      params = new MapSolrParams( new HashMap<String, String>() ); 
    }
    
    boolean rollback = params.getBool( UpdateParams.ROLLBACK, false );
    
    if( rollback || force ) {
      RollbackUpdateCommand cmd = new RollbackUpdateCommand(req);
      processor.processRollback( cmd );
      return true;
    }
    return false;
  }

  /**
   * @since 6.7
   */
  public static void setWt(SolrQueryRequest req, String wt) {
    SolrParams params = req.getParams();
    if (params.get(CommonParams.WT) != null) return;//wt is set by user
    Map<String, String> map = new HashMap<>(1);
    map.put(CommonParams.WT, wt);
    map.put("indent", "true");
    req.setParams(SolrParams.wrapDefaults(params, new MapSolrParams(map)));
  }
}
