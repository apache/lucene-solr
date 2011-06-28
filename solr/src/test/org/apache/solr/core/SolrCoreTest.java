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

package org.apache.solr.core;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.component.SpellCheckComponent;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.*;
public class SolrCoreTest extends SolrTestCaseJ4 {
  @Override
  public void setUp() throws Exception {
    super.setUp();
    initCore("solrconfig.xml", "schema.xml");
  }

  @Override
  public void tearDown() throws Exception {
    deleteCore();
    super.tearDown();
  }

  @Test
  public void testRequestHandlerRegistry() {
    SolrCore core = h.getCore();

    EmptyRequestHandler handler1 = new EmptyRequestHandler();
    EmptyRequestHandler handler2 = new EmptyRequestHandler();

    String path = "/this/is A path /that won't be registered!";
    SolrRequestHandler old = core.registerRequestHandler( path, handler1 );
    assertNull( old ); // should not be anything...
    assertEquals( core.getRequestHandlers().get( path ), handler1 );
    old = core.registerRequestHandler( path, handler2 );
    assertEquals( old, handler1 ); // should pop out the old one
    assertEquals( core.getRequestHandlers().get( path ), handler2 );
  }

  @Test
  public void testClose() throws Exception {
    final CoreContainer cores = h.getCoreContainer();
    SolrCore core = cores.getCore("");

    ClosingRequestHandler handler1 = new ClosingRequestHandler();
    handler1.inform( core );

    String path = "/this/is A path /that won't be registered 2!!!!!!!!!!!";
    SolrRequestHandler old = core.registerRequestHandler( path, handler1 );
    assertNull( old ); // should not be anything...
    assertEquals( core.getRequestHandlers().get( path ), handler1 );
    core.close();
    cores.shutdown();
    assertTrue("Handler not closed", handler1.closed == true);
  }
  
  @Test
  public void testRefCount() throws Exception {
    SolrCore core = h.getCore();
    assertTrue("Refcount != 1", core.getOpenCount() == 1);
    
    final CoreContainer cores = h.getCoreContainer();
    SolrCore c1 = cores.getCore("");
    assertTrue("Refcount != 2", core.getOpenCount() == 2);

    ClosingRequestHandler handler1 = new ClosingRequestHandler();
    handler1.inform( core );

    String path = "/this/is A path /that won't be registered!";
    SolrRequestHandler old = core.registerRequestHandler( path, handler1 );
    assertNull( old ); // should not be anything...
    assertEquals( core.getRequestHandlers().get( path ), handler1 );
   
    SolrCore c2 = cores.getCore("");
    c1.close();
    assertTrue("Refcount < 1", core.getOpenCount() >= 1);
    assertTrue("Handler is closed", handler1.closed == false);
    
    c1 = cores.getCore("");
    assertTrue("Refcount < 2", core.getOpenCount() >= 2);
    assertTrue("Handler is closed", handler1.closed == false);
    
    c2.close();
    assertTrue("Refcount < 1", core.getOpenCount() >= 1);
    assertTrue("Handler is closed", handler1.closed == false);

    c1.close();
    cores.shutdown();
    assertTrue("Refcount != 0", core.getOpenCount() == 0);
    assertTrue("Handler not closed", core.isClosed() && handler1.closed == true);
  }
    

  @Test
  public void testRefCountMT() throws Exception {
    SolrCore core = h.getCore();
    assertTrue("Refcount != 1", core.getOpenCount() == 1);

    final ClosingRequestHandler handler1 = new ClosingRequestHandler();
    handler1.inform(core);
    String path = "/this/is A path /that won't be registered!";
    SolrRequestHandler old = core.registerRequestHandler(path, handler1);
    assertNull(old); // should not be anything...
    assertEquals(core.getRequestHandlers().get(path), handler1);

    final int LOOP = 100;
    final int MT = 16;
    ExecutorService service = Executors.newFixedThreadPool(MT);
    List<Callable<Integer>> callees = new ArrayList<Callable<Integer>>(MT);
    final CoreContainer cores = h.getCoreContainer();
    for (int i = 0; i < MT; ++i) {
      Callable<Integer> call = new Callable<Integer>() {
        void yield(int n) {
          try {
            Thread.sleep(0, (n % 13 + 1) * 10);
          } catch (InterruptedException xint) {
          }
        }
        
        public Integer call() {
          SolrCore core = null;
          int r = 0;
          try {
            for (int l = 0; l < LOOP; ++l) {
              r += 1;
              core = cores.getCore("");
              // sprinkle concurrency hinting...
              yield(l);
              assertTrue("Refcount < 1", core.getOpenCount() >= 1);              
              yield(l);
              assertTrue("Refcount > 17", core.getOpenCount() <= 17);             
              yield(l);
              assertTrue("Handler is closed", handler1.closed == false);
              yield(l);
              core.close();
              core = null;
              yield(l);
            }
            return r;
          } finally {
            if (core != null)
              core.close();
          }
        }
      };
      callees.add(call);
    }

    List<Future<Integer>> results = service.invokeAll(callees);
    for (Future<Integer> result : results) {
      assertTrue("loop=" + result.get() +" < " + LOOP, result.get() >= LOOP);
    }
    
    cores.shutdown();
    assertTrue("Refcount != 0", core.getOpenCount() == 0);
    assertTrue("Handler not closed", core.isClosed() && handler1.closed == true);
    
    service.shutdown();
    assertTrue("Running for too long...", service.awaitTermination(60, TimeUnit.SECONDS));
  }

  @Test
  public void testInfoRegistry() throws Exception {
    //TEst that SolrInfoMBeans are registered, including SearchComponents
    SolrCore core = h.getCore();

    Map<String, SolrInfoMBean> infoRegistry = core.getInfoRegistry();
    assertTrue("infoRegistry Size: " + infoRegistry.size() + " is not greater than: " + 0, infoRegistry.size() > 0);
    //try out some that we know are in the config
    SolrInfoMBean bean = infoRegistry.get(SpellCheckComponent.class.getName());
    assertNotNull("bean not registered", bean);
    //try a default one
    bean = infoRegistry.get(QueryComponent.class.getName());
    assertNotNull("bean not registered", bean);
    //try a Req Handler, which are stored by name, not clas
    bean = infoRegistry.get("standard");
    assertNotNull("bean not registered", bean);
  }

}



class ClosingRequestHandler extends EmptyRequestHandler implements SolrCoreAware {
  boolean closed = false;

  public void inform(SolrCore core) {
    core.addCloseHook( new CloseHook() {
      @Override
      public void preClose(SolrCore core) {
        closed = true;
      }

      @Override
      public void postClose(SolrCore core) {}
    });
  }
}

/**
 * An empty handler for testing
 */
class EmptyRequestHandler extends RequestHandlerBase
{
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // nothing!
  }

  @Override public String getDescription() { return null; }
  @Override public String getSource() { return null; }
  @Override public String getSourceId() { return null; }
  @Override public String getVersion() { return null; }
}
