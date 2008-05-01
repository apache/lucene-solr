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

import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RTimer;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.core.SolrCore;
import org.apache.lucene.queryParser.ParseException;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.HttpClient;

import java.util.logging.Logger;
import java.util.*;
import java.util.concurrent.*;

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

  protected static Logger log = Logger.getLogger(SearchHandler.class.getName());

  protected List<SearchComponent> components = null;

  protected List<String> getDefaultComponents()
  {
    ArrayList<String> names = new ArrayList<String>(5);
    names.add( QueryComponent.COMPONENT_NAME );
    names.add( FacetComponent.COMPONENT_NAME );
    names.add( MoreLikeThisComponent.COMPONENT_NAME );
    names.add( HighlightComponent.COMPONENT_NAME );
    names.add( DebugComponent.COMPONENT_NAME );
    return names;
  }

  /**
   * Initialize the components based on name
   */
  @SuppressWarnings("unchecked")
  public void inform(SolrCore core)
  {
    Object declaredComponents = initArgs.get(INIT_COMPONENTS);
    List<String> first = (List<String>) initArgs.get(INIT_FIRST_COMPONENTS);
    List<String> last  = (List<String>) initArgs.get(INIT_LAST_COMPONENTS);

    List<String> list = null;
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
    }

    // Build the component list
    components = new ArrayList<SearchComponent>( list.size() );
    for(String c : list){
      SearchComponent comp = core.getSearchComponent( c );
      components.add(comp);
      log.info("Adding  component:"+comp);
    }
  }

  public List<SearchComponent> getComponents() {
    return components;
  }
  

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception, ParseException, InstantiationException, IllegalAccessException
  {
    ResponseBuilder rb = new ResponseBuilder();
    rb.req = req;
    rb.rsp = rsp;
    rb.components = components;
    rb.setDebug(req.getParams().getBool(CommonParams.DEBUG_QUERY, false));

    final RTimer timer = rb.isDebug() ? new RTimer() : null;

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

    if (rb.shards == null) {
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
        if( rb.getDebugInfo() == null ) {
          rb.setDebugInfo( new SimpleOrderedMap<Object>() );
        }
        rb.getDebugInfo().add( "timing", timer.asNamedList() );
      }

    } else {
      // a distributed request

      HttpCommComponent comm = new HttpCommComponent();

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
              ModifiableSolrParams params = sreq.params;
              params.remove("shards");      // not a top-level request
              params.remove("indent");
              params.remove("echoParams");
              params.set("isShard", true);  // a sub (shard) request
              String shardHandler = req.getParams().get("shards.qt");
              if (shardHandler == null) {
                params.remove("qt");
              } else {
                params.set("qt", shardHandler);
              }
              comm.submit(sreq, shard, params);
            }
          }


          // now wait for replies, but if anyone puts more requests on
          // the outgoing queue, send them out immediately (by exiting
          // this loop)
          while (rb.outgoing.size() == 0) {
            ShardResponse srsp = comm.takeCompletedOrError();
            if (srsp == null) break;  // no more requests to wait for

            // Was there an exception?  If so, abort everything and
            // rethrow
            if (srsp.exception != null) {
              comm.cancelAll();
              if (srsp.exception instanceof SolrException) {
                throw (SolrException)srsp.exception;
              } else {
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, srsp.exception);
              }
            }

            rb.finished.add(srsp.req);

            // let the components see the responses to the request
            for(SearchComponent c : components) {
              c.handleResponses(rb, srsp.req);
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
    for(SearchComponent c : components){
      sb.append(c.getName());
      sb.append(",");
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

class HttpCommComponent {

  // We want an executor that doesn't take up any resources if
  // it's not used, so it could be created statically for
  // the distributed search component if desired.
  //
  // Consider CallerRuns policy and a lower max threads to throttle
  // requests at some point (or should we simply return failure?)
  static Executor commExecutor = new ThreadPoolExecutor(
          0,
          Integer.MAX_VALUE,
          5, TimeUnit.SECONDS, // terminate idle threads after 5 sec
          new SynchronousQueue<Runnable>()  // directly hand off tasks
  );


  static HttpClient client;

  static {
    MultiThreadedHttpConnectionManager mgr = new MultiThreadedHttpConnectionManager();
    mgr.getParams().setDefaultMaxConnectionsPerHost(20);
    mgr.getParams().setMaxTotalConnections(10000);
    // mgr.getParams().setStaleCheckingEnabled(false);
    client = new HttpClient(mgr);    
  }

  CompletionService<ShardResponse> completionService = new ExecutorCompletionService<ShardResponse>(commExecutor);
  Set<Future<ShardResponse>> pending = new HashSet<Future<ShardResponse>>();

  HttpCommComponent() {
  }

  private static class SimpleSolrResponse extends SolrResponse {
    long elapsedTime;
    NamedList<Object> nl;
    public long getElapsedTime() {
      return elapsedTime;
    }

    public NamedList<Object> getResponse() {
      return nl;
    }
  }

  void submit(final ShardRequest sreq, final String shard, final ModifiableSolrParams params) {
    Callable<ShardResponse> task = new Callable<ShardResponse>() {
      public ShardResponse call() throws Exception {

        ShardResponse srsp = new ShardResponse();
        srsp.req = sreq;
        srsp.shard = shard;
        SimpleSolrResponse ssr = new SimpleSolrResponse();
        srsp.rsp = ssr;
        long startTime = System.currentTimeMillis();

        try {
          // String url = "http://" + shard + "/select";
          String url = "http://" + shard;

          params.remove("wt"); // use default (or should we explicitly set it?)
          params.remove("version");

          SolrServer server = new CommonsHttpSolrServer(url, client);
          // SolrRequest req = new QueryRequest(SolrRequest.METHOD.POST, "/select");
          // use generic request to avoid extra processing of queries
          QueryRequest req = new QueryRequest(sreq.params);
          req.setMethod(SolrRequest.METHOD.POST);
          req.setResponseParser(new BinaryResponseParser());  // this sets the wt param
          // srsp.rsp = server.request(req);
          // srsp.rsp = server.query(sreq.params);

          ssr.nl = server.request(req);
        } catch (Throwable th) {
          srsp.exception = th;
          if (th instanceof SolrException) {
            srsp.rspCode = ((SolrException)th).code();
          } else {
            srsp.rspCode = -1;
          }
        }

        ssr.elapsedTime = System.currentTimeMillis() - startTime;

        return srsp;
      }
    };

    pending.add( completionService.submit(task) );
  }

  /** returns a ShardResponse of the last response correlated with a ShardRequest */
  ShardResponse take() {
    while (pending.size() > 0) {
      try {
        Future<ShardResponse> future = completionService.take();
        pending.remove(future);
        ShardResponse rsp = future.get();
        rsp.req.responses.add(rsp);
        if (rsp.req.responses.size() == rsp.req.actualShards.length) {
          return rsp;
        }
      } catch (InterruptedException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (ExecutionException e) {
        // should be impossible... the problem with catching the exception
        // at this level is we don't know what ShardRequest it applied to
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Impossible Exception",e);
      }
    }
    return null;
  }


  /** returns a ShardResponse of the last response correlated with a ShardRequest,
   * or immediately returns a ShardResponse if there was an error detected
   */
  ShardResponse takeCompletedOrError() {
    while (pending.size() > 0) {
      try {
        Future<ShardResponse> future = completionService.take();
        pending.remove(future);
        ShardResponse rsp = future.get();
        if (rsp.exception != null) return rsp; // if exception, return immediately
        // add response to the response list... we do this after the take() and
        // not after the completion of "call" so we know when the last response
        // for a request was received.  Otherwise we might return the same
        // request more than once.
        rsp.req.responses.add(rsp);
        if (rsp.req.responses.size() == rsp.req.actualShards.length) {
          return rsp;
        }
      } catch (InterruptedException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (ExecutionException e) {
        // should be impossible... the problem with catching the exception
        // at this level is we don't know what ShardRequest it applied to
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Impossible Exception",e);
      }
    }
    return null;
  }


  void cancelAll() {
    for (Future<ShardResponse> future : pending) {
      // TODO: any issues with interrupting?  shouldn't be if
      // there are finally blocks to release connections.
      future.cancel(true);
    }
  }

}
