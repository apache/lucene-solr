/**
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
import java.util.HashMap;

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
   * If it does, it will check parameters for "waitFlush" and "waitSearcher"
   * 
   * @deprecated Use {@link #handleCommit(SolrQueryRequest,UpdateRequestProcessor,SolrParams,boolean)}
   *
   * @since solr 1.2
   */
  @Deprecated
  public static boolean handleCommit( SolrQueryRequest req, SolrQueryResponse rsp, boolean force ) throws IOException
  {
    SolrParams params = req.getParams();
    if( params == null ) {
      params = new MapSolrParams( new HashMap<String, String>() ); 
    }
    
    boolean optimize = params.getBool( UpdateParams.OPTIMIZE, false );
    boolean commit   = params.getBool( UpdateParams.COMMIT,   false );
    
    if( optimize || commit || force ) {
      CommitUpdateCommand cmd = new CommitUpdateCommand(req, optimize );
      cmd.waitSearcher = params.getBool( UpdateParams.WAIT_SEARCHER, cmd.waitSearcher );
      cmd.softCommit = params.getBool( UpdateParams.SOFT_COMMIT, cmd.softCommit );
      cmd.expungeDeletes = params.getBool( UpdateParams.EXPUNGE_DELETES, cmd.expungeDeletes);
      cmd.maxOptimizeSegments = params.getInt(UpdateParams.MAX_OPTIMIZE_SEGMENTS, cmd.maxOptimizeSegments);
      req.getCore().getUpdateHandler().commit( cmd );
      
      // Lets wait till after solr1.2 to define consistent output format
      //if( optimize ) {
      //  rsp.add( "optimize", true );
      //}
      //else {
      //  rsp.add( "commit", true );
      //}
      return true;
    }
    return false;
  }
  

  /**
   * Check the request parameters and decide if it should commit or optimize.
   * If it does, it will check parameters for "waitFlush" and "waitSearcher"
   */
  public static boolean handleCommit(SolrQueryRequest req, UpdateRequestProcessor processor, SolrParams params, boolean force ) throws IOException
  {
    if( params == null ) {
      params = new MapSolrParams( new HashMap<String, String>() ); 
    }
    
    boolean optimize = params.getBool( UpdateParams.OPTIMIZE, false );
    boolean commit   = params.getBool( UpdateParams.COMMIT,   false );
    
    if( optimize || commit || force ) {
      CommitUpdateCommand cmd = new CommitUpdateCommand(req, optimize );
      cmd.waitSearcher = params.getBool( UpdateParams.WAIT_SEARCHER, cmd.waitSearcher );
      cmd.softCommit = params.getBool( UpdateParams.SOFT_COMMIT, cmd.softCommit );
      cmd.expungeDeletes = params.getBool( UpdateParams.EXPUNGE_DELETES, cmd.expungeDeletes);      
      cmd.maxOptimizeSegments = params.getInt(UpdateParams.MAX_OPTIMIZE_SEGMENTS, cmd.maxOptimizeSegments);
      processor.processCommit( cmd );
      return true;
    }
    return false;
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
}
