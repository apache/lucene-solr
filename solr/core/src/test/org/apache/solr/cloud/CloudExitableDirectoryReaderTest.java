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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricRegistry;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster.JettySolrRunnerWithMetrics;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.FacetComponent;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.facet.FacetModule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.TrollingIndexReaderFactory.CheckMethodName;
import static org.apache.solr.cloud.TrollingIndexReaderFactory.Trap;
import static org.apache.solr.cloud.TrollingIndexReaderFactory.catchClass;
import static org.apache.solr.cloud.TrollingIndexReaderFactory.catchCount;
import static org.apache.solr.cloud.TrollingIndexReaderFactory.catchTrace;

/**
* Distributed test for {@link org.apache.lucene.index.ExitableDirectoryReader} 
*/
public class CloudExitableDirectoryReaderTest extends SolrCloudTestCase {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int NUM_DOCS_PER_TYPE = 20;
  private static final String sleep = "2";

  private static final String COLLECTION = "exitable";
  private static Map<String, Metered> fiveHundredsByNode;

  /**
   * Client used for all test requests.
   * <p>
   * LBSolrClient (and by extension CloudSolrClient) has it's own enforcement of timeAllowed 
   * in an attempt to prevent "retrying" failed requests far longer then the client requested.
   * Because of this client side logic, we do not want to use any LBSolrClient (derivative) in 
   * this test, in order to ensure that on a "slow" machine, the client doesn't pre-emptively 
   * abort any of our requests that use very low 'timeAllowed' values.
   * </p>
   * <p>
   * ie: This test is not about testing the SolrClient, so keep the SOlrClient simple.
   * </p>
   */
  private static SolrClient client;
  
  @BeforeClass
  public static void setupCluster() throws Exception {
    // create one more node then shard, so that we also test the case of proxied requests.
    Builder clusterBuilder = configureCluster(3)
        .addConfig("conf", TEST_PATH().resolve("configsets").resolve("exitable-directory").resolve("conf"));
    clusterBuilder.withMetrics(true);
    clusterBuilder
        .configure();

    // pick an arbitrary node to use for our requests
    client = cluster.getRandomJetty(random()).newClient();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.getSolrClient().waitForState(COLLECTION, DEFAULT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, 2, 1));

    fiveHundredsByNode = new LinkedHashMap<>();
    int httpOk = 0;
    for (JettySolrRunner jetty: cluster.getJettySolrRunners()) {
      MetricRegistry metricRegistry = ((JettySolrRunnerWithMetrics)jetty).getMetricRegistry();
      
      httpOk += ((Metered) metricRegistry.getMetrics()
                 .get("org.eclipse.jetty.servlet.ServletContextHandler.2xx-responses")).getCount();
      
      Metered old = fiveHundredsByNode.put(jetty.getNodeName(),
          (Metered) metricRegistry.getMetrics()
             .get("org.eclipse.jetty.servlet.ServletContextHandler.5xx-responses"));
      assertNull("expecting uniq nodenames",old);
    }
    assertTrue("expecting some http activity during collection creation", httpOk > 0);
    indexDocs();
  }
  
  @AfterClass
  public static void closeClient() throws Exception {
    if (null != client) {
      client.close();
      client = null;
    }
  }

  public static void indexDocs() throws Exception {
    int counter;
    counter = 1;
    UpdateRequest req = new UpdateRequest();

    for(; (counter % NUM_DOCS_PER_TYPE) != 0; counter++ ) {
      final String v = "a" + counter;
      req.add(sdoc("id", Integer.toString(counter), "name", v,
          "name_dv", v,
          "name_dvs", v,"name_dvs", v+"1",
          "num",""+counter));
    }

    counter++;
    for(; (counter % NUM_DOCS_PER_TYPE) != 0; counter++ ) {
      final String v = "b" + counter;
      req.add(sdoc("id", Integer.toString(counter), "name", v,
          "name_dv", v,
          "name_dvs", v,"name_dvs", v+"1",
          "num",""+counter));
    }

    counter++;
    for(; counter % NUM_DOCS_PER_TYPE != 0; counter++ ) {
      final String v = "dummy term doc" + counter;
      req.add(sdoc("id", Integer.toString(counter), "name", 
          v,
          "name_dv", v,
          "name_dvs", v,"name_dvs", v+"1",
          "num",""+counter));
    }

    req.commit(client, COLLECTION);
  }

  @Test
  public void test() throws Exception {
    assertPartialResults(params("q", "name:a*", "timeAllowed", "1", "sleep", sleep));

    /*
    query rewriting for NUM_DOCS_PER_TYPE terms should take less 
    time than this. Keeping it at 5 because the delaying search component delays all requests 
    by at 1 second.
     */
    int fiveSeconds = 5000;
    
    Integer timeAllowed = TestUtil.nextInt(random(), fiveSeconds, Integer.MAX_VALUE);
    assertSuccess(params("q", "name:a*", "timeAllowed", timeAllowed.toString()));

    assertPartialResults(params("q", "name:a*", "timeAllowed", "1", "sleep", sleep));

    timeAllowed = TestUtil.nextInt(random(), fiveSeconds, Integer.MAX_VALUE);
    assertSuccess(params("q", "name:b*", "timeAllowed",timeAllowed.toString()));

    // negative timeAllowed should disable timeouts
    timeAllowed = TestUtil.nextInt(random(), Integer.MIN_VALUE, -1); 
    assertSuccess(params("q", "name:b*", "timeAllowed",timeAllowed.toString()));

    assertSuccess(params("q","name:b*")); // no time limitation
  }

  @Test
  public void testWhitebox() throws Exception {
    
    try (Trap catchIds = catchTrace(
        new CheckMethodName("doProcessSearchByIds"), () -> {})) {
      assertPartialResults(params("q", "{!cache=false}name:a*", "sort", "query($q,1) asc"),
          () -> assertTrue(catchIds.hasCaught()));
    } catch (AssertionError ae) {
      Trap.dumpLastStackTraces(log);
      throw ae;
    }

    // the point is to catch sort_values (fsv) timeout, between search and facet
    // I haven't find a way to encourage fsv to read index
    try (Trap catchFSV = catchTrace(
        new CheckMethodName("doFieldSortValues"), () -> {})) {
      assertPartialResults(params("q", "{!cache=false}name:a*", "sort", "query($q,1) asc"),
          () -> assertTrue(catchFSV.hasCaught()));
    } catch (AssertionError ae) {
      Trap.dumpLastStackTraces(log);
      throw ae;
    }
    
    try (Trap catchClass = catchClass(
        QueryComponent.class.getSimpleName(), () -> {  })) {
      assertPartialResults(params("q", "{!cache=false}name:a*"),
          ()->assertTrue(catchClass.hasCaught()));
    }catch(AssertionError ae) {
      Trap.dumpLastStackTraces(log);
      throw ae;
    }
    try(Trap catchClass = catchClass(FacetComponent.class.getSimpleName())){
      assertPartialResults(params("q", "{!cache=false}name:a*", "facet","true", "facet.method", "enum", 
          "facet.field", "id"),
          ()->assertTrue(catchClass.hasCaught()));
    }catch(AssertionError ae) {
      Trap.dumpLastStackTraces(log);
      throw ae;
    }

    try (Trap catchClass = catchClass(FacetModule.class.getSimpleName())) {
      assertPartialResults(params("q", "{!cache=false}name:a*", "json.facet", "{ ids: {"
          + " type: range, field : num, start : 0, end : 100, gap : 10 }}"),
          () -> assertTrue(catchClass.hasCaught()));
    } catch (AssertionError ae) {
      Trap.dumpLastStackTraces(log);
      throw ae;
    }
  }

  @Test 
  @Repeat(iterations=5)
  public void testCreepThenBite() throws Exception {
    int creep=100;
    ModifiableSolrParams params = params("q", "{!cache=false}name:a*");
    SolrParams cases[] = new SolrParams[] {
        params( "sort","query($q,1) asc"),
        params("rows","0", "facet","true", "facet.method", "enum", "facet.field", "name"),
        params("rows","0", "json.facet","{ ids: { type: range, field : num, start : 1, end : 99, gap : 9 }}"),
        params("q", "*:*", "rows","0", "json.facet","{ ids: { type: field, field : num}}"),
        params("q", "*:*", "rows","0", "json.facet","{ ids: { type: field, field : name_dv}}"),
        params("q", "*:*", "rows","0", "json.facet","{ ids: { type: field, field : name_dvs}}")
    }; // add more cases here

    params.add(cases[random().nextInt(cases.length)]);
    for (; ; creep*=1.5) {
      final int boundary = creep;
      try(Trap catchClass = catchCount(boundary)){
        
        params.set("boundary", boundary);
        QueryResponse rsp = client.query(COLLECTION, 
            params);
        assertEquals(""+rsp, rsp.getStatus(), 0);
        assertNo500s(""+rsp);
        if (!isPartial(rsp)) {
          assertFalse(catchClass.hasCaught());
          break;
        }
        assertTrue(catchClass.hasCaught());
      }catch(AssertionError ae) {
        Trap.dumpLastStackTraces(log);
        throw ae;
      }
    }
    int numBites = atLeast(100);
    for(int bite=0; bite<numBites; bite++) {
      int boundary = random().nextInt(creep);
      boolean omitHeader = random().nextBoolean();
      try(Trap catchCount = catchCount(boundary)){
        params.set("omitHeader", "" + omitHeader);
        params.set("boundary", boundary);
        QueryResponse rsp = client.query(COLLECTION, 
            params);
        assertEquals(""+rsp, rsp.getStatus(), 0);
        assertNo500s(""+rsp);
        // without responseHeader, whether the response is partial or not can't be known
        // omitHeader=true used in request to ensure that no NPE exceptions are thrown
        if (omitHeader) {
          continue;
        }
        assertEquals("" + creep + " ticks were successful; trying " + boundary + " yields " + rsp,
            catchCount.hasCaught(), isPartial(rsp));
      }catch(AssertionError ae) {
        Trap.dumpLastStackTraces(log);
        throw ae;
      }
    }
  }

  public boolean isPartial(QueryResponse rsp) {
    return Boolean.TRUE.equals(rsp.getHeader().getBooleanArg(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
  }

  public void assertNo500s(String msg) {
    assertTrue(msg,fiveHundredsByNode.values().stream().allMatch((m)->m.getCount()==0));
  }
  
  /**
   * execute a request, verify that we get an expected error
   */
  public void assertPartialResults(ModifiableSolrParams p) throws Exception {
    assertPartialResults(p, ()->{});
  }
  
  public void assertPartialResults(ModifiableSolrParams p, Runnable postRequestCheck) throws Exception {
      QueryResponse rsp = client.query(COLLECTION, p);
      postRequestCheck.run();
      assertEquals(rsp.getStatus(), 0);
      assertEquals(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY+" were expected at "+rsp,
          true, rsp.getHeader().getBooleanArg(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
      assertNo500s(""+rsp);
  }
  
  public void assertSuccess(ModifiableSolrParams p) throws Exception {
    QueryResponse rsp = client.query(COLLECTION, p);
    assertEquals(rsp.getStatus(), 0);
    assertEquals("Wrong #docs in response", NUM_DOCS_PER_TYPE - 1, rsp.getResults().getNumFound());
    assertNotEquals(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY+" weren't expected "+rsp,
        true, rsp.getHeader().getBooleanArg(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
    assertNo500s(""+rsp);
  }
}

