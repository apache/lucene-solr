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
package org.apache.solr.handler.component;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.pkg.PackageListeners;
import org.apache.solr.pkg.PackageLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrQueryTimeoutImpl;
import org.apache.solr.search.facet.FacetModule;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.RTimerTree;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.PATH;


/**
 * Refer SOLR-281
 */
public class SearchHandler extends RequestHandlerBase implements SolrCoreAware, PluginInfoInitialized, PermissionNameProvider {
  static final String INIT_COMPONENTS = "components";
  static final String INIT_FIRST_COMPONENTS = "first-components";
  static final String INIT_LAST_COMPONENTS = "last-components";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected volatile List<SearchComponent> components;
  private ShardHandlerFactory shardHandlerFactory;
  private PluginInfo shfInfo;
  private SolrCore core;

  protected List<String> getDefaultComponents() {
    ArrayList<String> names = new ArrayList<>(8);
    names.add(QueryComponent.COMPONENT_NAME);
    names.add(FacetComponent.COMPONENT_NAME);
    names.add(FacetModule.COMPONENT_NAME);
    names.add(MoreLikeThisComponent.COMPONENT_NAME);
    names.add(HighlightComponent.COMPONENT_NAME);
    names.add(StatsComponent.COMPONENT_NAME);
    names.add(DebugComponent.COMPONENT_NAME);
    names.add(ExpandComponent.COMPONENT_NAME);
    names.add(TermsComponent.COMPONENT_NAME);

    return names;
  }

  @Override
  public void init(PluginInfo info) {
    init(info.initArgs);
    for (PluginInfo child : info.children) {
      if ("shardHandlerFactory".equals(child.type)) {
        this.shfInfo = child;
        break;
      }
    }
  }

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext ctx) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  /**
   * Initialize the components based on name.  Note, if using <code>INIT_FIRST_COMPONENTS</code> or <code>INIT_LAST_COMPONENTS</code>,
   * then the {@link DebugComponent} will always occur last.  If this is not desired, then one must explicitly declare all components using
   * the <code>INIT_COMPONENTS</code> syntax.
   */
  @Override
  @SuppressWarnings("unchecked")
  public void inform(SolrCore core) {
    this.core = core;
    List<String> c = (List<String>) initArgs.get(INIT_COMPONENTS);
    Set<String> missing = new HashSet<>(core.getSearchComponents().checkContains(c));
    List<String> first = (List<String>) initArgs.get(INIT_FIRST_COMPONENTS);
    missing.addAll(core.getSearchComponents().checkContains(first));
    List<String> last = (List<String>) initArgs.get(INIT_LAST_COMPONENTS);
    missing.addAll(core.getSearchComponents().checkContains(last));
    if (!missing.isEmpty()) throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
        "Missing SearchComponents named : " + missing);
    if (c != null && (first != null || last != null)) throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
        "First/Last components only valid if you do not declare 'components'");

    if (shfInfo == null) {
      shardHandlerFactory = core.getCoreContainer().getShardHandlerFactory();
    } else {
      shardHandlerFactory = core.createInitInstance(shfInfo, ShardHandlerFactory.class, null, null);
      core.addCloseHook(new CloseHook() {
        @Override
        public void preClose(SolrCore core) {
          shardHandlerFactory.close();
        }

        @Override
        public void postClose(SolrCore core) {
        }
      });
    }

    if (core.getCoreContainer().isZooKeeperAware()) {
      core.getPackageListeners().addListener(new PackageListeners.Listener() {
        @Override
        public String packageName() {
          return null;
        }

        @Override
        public PluginInfo pluginInfo() {
          return null;
        }

        @Override
        public void changed(PackageLoader.Package pkg) {
          //we could optimize this by listening to only relevant packages,
          // but it is not worth optimizing as these are lightweight objects
          components = null;
        }

        @Override
        public PackageLoader.Package.Version getPackageVersion() {
          return null;
        }
      });
    }

  }

  private void initComponents() {
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
    List<SearchComponent> components = new ArrayList<>(list.size());
    DebugComponent dbgCmp = null;
    for(String c : list){
      SearchComponent comp = core.getSearchComponent( c );
      if (comp instanceof DebugComponent && makeDebugLast == true){
        dbgCmp = (DebugComponent) comp;
      } else {
        components.add(comp);
        log.debug("Adding  component:{}", comp);
      }
    }
    if (makeDebugLast == true && dbgCmp != null){
      components.add(dbgCmp);
      log.debug("Adding  debug component:{}", dbgCmp);
    }
    this.components = components;
  }

  public List<SearchComponent> getComponents() {
    List<SearchComponent> result = components;  // volatile read
    if (result == null) {
      synchronized (this) {
        if (components == null) {
          initComponents();
        }
        result = components;
      }
    }
    return result;
  }

  private ShardHandler getAndPrepShardHandler(SolrQueryRequest req, ResponseBuilder rb) {
    ShardHandler shardHandler = null;

    CoreContainer cc = req.getCore().getCoreContainer();
    boolean isZkAware = cc.isZooKeeperAware();
    rb.isDistrib = req.getParams().getBool(DISTRIB, isZkAware);
    if (!rb.isDistrib) {
      // for back compat, a shards param with URLs like localhost:8983/solr will mean that this
      // search is distributed.
      final String shards = req.getParams().get(ShardParams.SHARDS);
      rb.isDistrib = ((shards != null) && (shards.indexOf('/') > 0));
    }
    
    if (rb.isDistrib) {
      shardHandler = shardHandlerFactory.getShardHandler();
      shardHandler.prepDistributed(rb);
      if (!rb.isDistrib) {
        shardHandler = null; // request is not distributed after all and so the shard handler is not needed
      }
    }

    if (isZkAware) {
      String shardsTolerant = req.getParams().get(ShardParams.SHARDS_TOLERANT);
      boolean requireZkConnected = shardsTolerant != null && shardsTolerant.equals(ShardParams.REQUIRE_ZK_CONNECTED);
      ZkController zkController = cc.getZkController();
      boolean zkConnected = zkController != null && ! zkController.getZkClient().getConnectionManager().isLikelyExpired();
      if (requireZkConnected && false == zkConnected) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "ZooKeeper is not connected");
      } else {
        NamedList<Object> headers = rb.rsp.getResponseHeader();
        if (headers != null) {
          headers.add("zkConnected", zkConnected);
        }
      }
    }

    return shardHandler;
  }
  
  /**
   * Override this method if you require a custom {@link ResponseBuilder} e.g. for use by a custom {@link SearchComponent}.
   */
  protected ResponseBuilder newResponseBuilder(SolrQueryRequest req, SolrQueryResponse rsp, List<SearchComponent> components) {
    return new ResponseBuilder(req, rsp, components);
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception
  {
    List<SearchComponent> components  = getComponents();
    ResponseBuilder rb = newResponseBuilder(req, rsp, components);
    if (rb.requestInfo != null) {
      rb.requestInfo.setResponseBuilder(rb);
    }

    boolean dbg = req.getParams().getBool(CommonParams.DEBUG_QUERY, false);
    rb.setDebug(dbg);
    if (dbg == false){//if it's true, we are doing everything anyway.
      SolrPluginUtils.getDebugInterests(req.getParams().getParams(CommonParams.DEBUG), rb);
    }

    final RTimerTree timer = rb.isDebug() ? req.getRequestTimer() : null;

    final ShardHandler shardHandler1 = getAndPrepShardHandler(req, rb); // creates a ShardHandler object only if it's needed
    
    if (timer == null) {
      // non-debugging prepare phase
      for( SearchComponent c : components ) {
        c.prepare(rb);
      }
    } else {
      // debugging prepare phase
      RTimerTree subt = timer.sub( "prepare" );
      for( SearchComponent c : components ) {
        rb.setTimer( subt.sub( c.getName() ) );
        c.prepare(rb);
        rb.getTimer().stop();
      }
      subt.stop();
    }

    if (!rb.isDistrib) {
      // a normal non-distributed request

      long timeAllowed = req.getParams().getLong(CommonParams.TIME_ALLOWED, -1L);
      if (timeAllowed > 0L) {
        SolrQueryTimeoutImpl.set(timeAllowed);
      }
      try {
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
          RTimerTree subt = timer.sub( "process" );
          for( SearchComponent c : components ) {
            rb.setTimer( subt.sub( c.getName() ) );
            c.process(rb);
            rb.getTimer().stop();
          }
          subt.stop();

          // add the timing info
          if (rb.isDebugTimings()) {
            rb.addDebugInfo("timing", timer.asNamedList() );
          }
        }
      } catch (ExitableDirectoryReader.ExitingReaderException ex) {
        log.warn( "Query: " + req.getParamString() + "; " + ex.getMessage());
        if( rb.rsp.getResponse() == null) {
          rb.rsp.addResponse(new SolrDocumentList());
        }
        if(rb.isDebug()) {
          NamedList debug = new NamedList();
          debug.add("explain", new NamedList());
          rb.rsp.add("debug", debug);
        }
        rb.rsp.getResponseHeader().asShallowMap()
              .put(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
      } finally {
        SolrQueryTimeoutImpl.reset();
      }
    } else {
      // a distributed request

      if (rb.outgoing == null) {
        rb.outgoing = new LinkedList<>();
      }
      rb.finished = new ArrayList<>();

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
            sreq.responses = new ArrayList<>(sreq.actualShards.length); // presume we'll get a response from each shard we send to

            // TODO: map from shard to address[]
            for (String shard : sreq.actualShards) {
              ModifiableSolrParams params = new ModifiableSolrParams(sreq.params);
              params.remove(ShardParams.SHARDS);      // not a top-level request
              params.set(DISTRIB, "false");               // not a top-level request
              params.remove("indent");
              params.remove(CommonParams.HEADER_ECHO_PARAMS);
              params.set(ShardParams.IS_SHARD, true);  // a sub (shard) request
              params.set(ShardParams.SHARDS_PURPOSE, sreq.purpose);
              params.set(ShardParams.SHARD_URL, shard); // so the shard knows what was asked
              params.set(CommonParams.OMIT_HEADER, false);
              if (rb.requestInfo != null) {
                // we could try and detect when this is needed, but it could be tricky
                params.set("NOW", Long.toString(rb.requestInfo.getNOW().getTime()));
              }
              String shardQt = params.get(ShardParams.SHARDS_QT);
              if (shardQt != null) {
                params.set(CommonParams.QT, shardQt);
              } else {
                // for distributed queries that don't include shards.qt, use the original path
                // as the default but operators need to update their luceneMatchVersion to enable
                // this behavior since it did not work this way prior to 5.1
                String reqPath = (String) req.getContext().get(PATH);
                if (!"/select".equals(reqPath)) {
                  params.set(CommonParams.QT, reqPath);
                } // else if path is /select, then the qt gets passed thru if set
              }
              shardHandler1.submit(sreq, shard, params);
            }
          }


          // now wait for replies, but if anyone puts more requests on
          // the outgoing queue, send them out immediately (by exiting
          // this loop)
          boolean tolerant = ShardParams.getShardsTolerantAsBool(rb.req.getParams());
          while (rb.outgoing.size() == 0) {
            ShardResponse srsp = tolerant ? 
                shardHandler1.takeCompletedIncludingErrors():
                shardHandler1.takeCompletedOrError();
            if (srsp == null) break;  // no more requests to wait for

            // Was there an exception?  
            if (srsp.getException() != null) {
              // If things are not tolerant, abort everything and rethrow
              if(!tolerant) {
                shardHandler1.cancelAll();
                if (srsp.getException() instanceof SolrException) {
                  throw (SolrException)srsp.getException();
                } else {
                  throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, srsp.getException());
                }
              } else {
                rsp.getResponseHeader().asShallowMap().put(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
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
    
    // SOLR-5550: still provide shards.info if requested even for a short circuited distrib request
    if(!rb.isDistrib && req.getParams().getBool(ShardParams.SHARDS_INFO, false) && rb.shortCircuitedURL != null) {  
      NamedList<Object> shardInfo = new SimpleOrderedMap<Object>();
      SimpleOrderedMap<Object> nl = new SimpleOrderedMap<Object>();        
      if (rsp.getException() != null) {
        Throwable cause = rsp.getException();
        if (cause instanceof SolrServerException) {
          cause = ((SolrServerException)cause).getRootCause();
        } else {
          if (cause.getCause() != null) {
            cause = cause.getCause();
          }          
        }
        nl.add("error", cause.toString() );
        StringWriter trace = new StringWriter();
        cause.printStackTrace(new PrintWriter(trace));
        nl.add("trace", trace.toString() );
      }
      else {
        nl.add("numFound", rb.getResults().docList.matches());
        nl.add("maxScore", rb.getResults().docList.maxScore());
      }
      nl.add("shardAddress", rb.shortCircuitedURL);
      nl.add("time", req.getRequestTimer().getTime()); // elapsed time of this request so far
      
      int pos = rb.shortCircuitedURL.indexOf("://");        
      String shardInfoName = pos != -1 ? rb.shortCircuitedURL.substring(pos+3) : rb.shortCircuitedURL;
      shardInfo.add(shardInfoName, nl);   
      rsp.getValues().add(ShardParams.SHARDS_INFO,shardInfo);            
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
  public Boolean registerV2() {
    return Boolean.TRUE;
  }
}


// TODO: generalize how a comm component can fit into search component framework
// TODO: statics should be per-core singletons


