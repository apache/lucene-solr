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

package org.apache.solr.handler.component;

import org.apache.lucene.queryParser.ParseException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.RTimer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *
 * Refer SOLR-281
 *
 */
public class SearchHandler extends RequestHandlerBase implements SolrCoreAware
{
  static final String INIT_COMPONENTS = "components";
  static final String INIT_FIRST_COMPONENTS = "first-components";
  static final String INIT_LAST_COMPONENTS = "last-components";


  

  
  protected static Logger log = LoggerFactory.getLogger(SearchHandler.class);

  protected List<SearchComponent> components = null;
  private ShardHandlerFactory shardHandlerFactory ;
  private PluginInfo shfInfo;

  protected List<String> getDefaultComponents()
  {
    ArrayList<String> names = new ArrayList<String>(6);
    names.add( QueryComponent.COMPONENT_NAME );
    names.add( FacetComponent.COMPONENT_NAME );
    names.add( MoreLikeThisComponent.COMPONENT_NAME );
    names.add( HighlightComponent.COMPONENT_NAME );
    names.add( StatsComponent.COMPONENT_NAME );
    names.add( DebugComponent.COMPONENT_NAME );
    return names;
  }

  public void init(PluginInfo info) {
    init(info.initArgs);
    for (PluginInfo child : info.children) {
      if("shardHandlerFactory".equals(child.type)){
        this.shfInfo = child;
        break;
      }
    }
  }

  /**
   * Initialize the components based on name.  Note, if using {@link #INIT_FIRST_COMPONENTS} or {@link #INIT_LAST_COMPONENTS},
   * then the {@link DebugComponent} will always occur last.  If this is not desired, then one must explicitly declare all components using
   * the {@link #INIT_COMPONENTS} syntax.
   */
  @SuppressWarnings("unchecked")
  public void inform(SolrCore core)
  {
    Object declaredComponents = initArgs.get(INIT_COMPONENTS);
    List<String> first = (List<String>) initArgs.get(INIT_FIRST_COMPONENTS);
    List<String> last  = (List<String>) initArgs.get(INIT_LAST_COMPONENTS);

    List<String> list = null;
    boolean makeDebugLast = true;
    if( declaredComponents == null ) {
      // Use the default component list
      list = getDefaultComponents();

      if( first != null ) {
        List<String> clist = first;
        clist.addAll( list );
        list = clist;
      }

      if( last != null ) {
        list.addAll( last );
      }
    }
    else {
      list = (List<String>)declaredComponents;
      if( first != null || last != null ) {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
            "First/Last components only valid if you do not declare 'components'");
      }
      makeDebugLast = false;
    }

    // Build the component list
    components = new ArrayList<SearchComponent>( list.size() );
    DebugComponent dbgCmp = null;
    for(String c : list){
      SearchComponent comp = core.getSearchComponent( c );
      if (comp instanceof DebugComponent && makeDebugLast == true){
        dbgCmp = (DebugComponent) comp;
      } else {
        components.add(comp);
        log.info("Adding  component:"+comp);
      }
    }
    if (makeDebugLast == true && dbgCmp != null){
      components.add(dbgCmp);
      log.info("Adding  debug component:" + dbgCmp);
    }
    if(shfInfo ==null) {
      Map m = new HashMap();
      m.put("class",HttpShardHandlerFactory.class.getName());
      shfInfo = new PluginInfo("shardHandlerFactory", m,null,Collections.<PluginInfo>emptyList());
    }
    shardHandlerFactory = core.createInitInstance(shfInfo, ShardHandlerFactory.class, null, null);
  }

  public List<SearchComponent> getComponents() {
    return components;
  }
  

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception, ParseException, InstantiationException, IllegalAccessException
  {
    // int sleep = req.getParams().getInt("sleep",0);
    // if (sleep > 0) {log.error("SLEEPING for " + sleep);  Thread.sleep(sleep);}
    ResponseBuilder rb = new ResponseBuilder(req, rsp, components);
    if (rb.requestInfo != null) {
      rb.requestInfo.setResponseBuilder(rb);
    }

    boolean dbg = req.getParams().getBool(CommonParams.DEBUG_QUERY, false);
    rb.setDebug(dbg);
    if (dbg == false){//if it's true, we are doing everything anyway.
      SolrPluginUtils.getDebugInterests(req.getParams().getParams(CommonParams.DEBUG), rb);
    }

    final RTimer timer = rb.isDebug() ? new RTimer() : null;


    ShardHandler shardHandler1 = shardHandlerFactory.getShardHandler();
    shardHandler1.checkDistributed(rb);

    if (timer == null) {
      // non-debugging prepare phase
      for( SearchComponent c : components ) {
        c.prepare(rb);
      }
    } else {
      // debugging prepare phase
      RTimer subt = timer.sub( "prepare" );
      for( SearchComponent c : components ) {
        rb.setTimer( subt.sub( c.getName() ) );
        c.prepare(rb);
        rb.getTimer().stop();
      }
      subt.stop();
    }

    if (!rb.isDistrib) {
      // a normal non-distributed request

      // The semantics of debugging vs not debugging are different enough that
      // it makes sense to have two control loops
      if(!rb.isDebug()) {
        // Process
        for( SearchComponent c : components ) {
          c.process(rb);
        }
      }
      else {
        // Process
        RTimer subt = timer.sub( "process" );
        for( SearchComponent c : components ) {
          rb.setTimer( subt.sub( c.getName() ) );
          c.process(rb);
          rb.getTimer().stop();
        }
        subt.stop();
        timer.stop();

        // add the timing info
        if (rb.isDebugTimings()) {
          rb.addDebugInfo("timing", timer.asNamedList() );
        }
      }

    } else {
      // a distributed request

      if (rb.outgoing == null) {
        rb.outgoing = new LinkedList<ShardRequest>();
      }
      rb.finished = new ArrayList<ShardRequest>();

      int nextStage = 0;
      do {
        rb.stage = nextStage;
        nextStage = ResponseBuilder.STAGE_DONE;

        // call all components
        for( SearchComponent c : components ) {
          // the next stage is the minimum of what all components report
          nextStage = Math.min(nextStage, c.distributedProcess(rb));
        }


        // check the outgoing queue and send requests
        while (rb.outgoing.size() > 0) {

          // submit all current request tasks at once
          while (rb.outgoing.size() > 0) {
            ShardRequest sreq = rb.outgoing.remove(0);
            sreq.actualShards = sreq.shards;
            if (sreq.actualShards==ShardRequest.ALL_SHARDS) {
              sreq.actualShards = rb.shards;
            }
            sreq.responses = new ArrayList<ShardResponse>();

            // TODO: map from shard to address[]
            for (String shard : sreq.actualShards) {
              ModifiableSolrParams params = new ModifiableSolrParams(sreq.params);
              params.remove(ShardParams.SHARDS);      // not a top-level request
              params.remove("distrib");               // not a top-level request
              params.remove("indent");
              params.remove(CommonParams.HEADER_ECHO_PARAMS);
              params.set(ShardParams.IS_SHARD, true);  // a sub (shard) request
              params.set(ShardParams.SHARD_URL, shard); // so the shard knows what was asked
              if (rb.requestInfo != null) {
                // we could try and detect when this is needed, but it could be tricky
                params.set("NOW", Long.toString(rb.requestInfo.getNOW().getTime()));
              }
              String shardQt = req.getParams().get(ShardParams.SHARDS_QT);
              if (shardQt == null) {
                params.remove(CommonParams.QT);
              } else {
                params.set(CommonParams.QT, shardQt);
              }
              shardHandler1.submit(sreq, shard, params);
            }
          }


          // now wait for replies, but if anyone puts more requests on
          // the outgoing queue, send them out immediately (by exiting
          // this loop)
          while (rb.outgoing.size() == 0) {
            ShardResponse srsp = shardHandler1.takeCompletedOrError();
            if (srsp == null) break;  // no more requests to wait for

            // Was there an exception?  If so, abort everything and
            // rethrow
            if (srsp.getException() != null) {
              shardHandler1.cancelAll();
              if (srsp.getException() instanceof SolrException) {
                throw (SolrException)srsp.getException();
              } else {
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, srsp.getException());
              }
            }

            rb.finished.add(srsp.getShardRequest());

            // let the components see the responses to the request
            for(SearchComponent c : components) {
              c.handleResponses(rb, srsp.getShardRequest());
            }
          }
        }

        for(SearchComponent c : components) {
            c.finishStage(rb);
         }

        // we are done when the next stage is MAX_VALUE
      } while (nextStage != Integer.MAX_VALUE);
    }
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    StringBuilder sb = new StringBuilder();
    sb.append("Search using components: ");
    if( components != null ) {
      for(SearchComponent c : components){
        sb.append(c.getName());
        sb.append(",");
      }
    }
    return sb.toString();
  }

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}


// TODO: generalize how a comm component can fit into search component framework
// TODO: statics should be per-core singletons


