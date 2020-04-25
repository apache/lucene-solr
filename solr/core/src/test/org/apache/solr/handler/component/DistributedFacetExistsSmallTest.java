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

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;

import static org.hamcrest.CoreMatchers.is;

public class DistributedFacetExistsSmallTest extends BaseDistributedSearchTestCase {

  public static final String FLD = "t_s";
  private int maxId;
  
  public DistributedFacetExistsSmallTest() {
  }

  @Before
  public void prepareIndex() throws Exception {
    del("*:*");

    final Random rnd = random();
    index(id, maxId=rnd.nextInt(5), FLD, "AAA");
    index(id, maxId+=1+rnd.nextInt(5), FLD, "B");
    index(id, maxId+=1+rnd.nextInt(5), FLD, "BB");
    index(id, maxId+=1+rnd.nextInt(5), FLD, "BB");
    index(id, maxId+=1+rnd.nextInt(5), FLD, "BBB");
    index(id, maxId+=1+rnd.nextInt(5), FLD, "BBB");
    index(id, maxId+=1+rnd.nextInt(5), FLD, "BBB");
    index(id, maxId+=1+rnd.nextInt(5), FLD, "CC");
    index(id, maxId+=1+rnd.nextInt(5), FLD, "CC");
    index(id, maxId+=1+rnd.nextInt(5), FLD, "CCC");
    index(id, maxId+=1+rnd.nextInt(5), FLD, "CCC");
    index(id, maxId+=1+rnd.nextInt(5), FLD, "CCC");

    final SolrClient shard0 = clients.get(0);
    // expectidly fails test
    //shard0.add(sdoc("id", 13, FLD, "DDD"));
    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);
    handle.put("_version_", SKIPVAL);
  }

  @ShardsFixed(num=4)
  public void test() throws Exception{
    checkBasicRequest();
    checkWithMinCountEqOne();
    checkWithSortCount();
    checkWithMethodSetPerField();
    
    {
      // empty enum for checking npe
      final ModifiableSolrParams params = buildParams();
      params.remove("facet.exists");
      QueryResponse rsp = query(params);
    }
    
    checkRandomParams();
    
    checkInvalidMincount();
  }

  private void checkRandomParams() throws Exception {
    final ModifiableSolrParams params = buildParams();
    Random rand = random();

    if (rand.nextBoolean()) {
      int from;
      params.set("q", "["+(from = rand.nextInt(maxId/2))+
                  " TO "+((from-1)+(rand.nextInt(maxId)))+"]");
    }
    
    int offset = 0;
    int indexSize = 6;
    if (rand .nextInt(100) < 20) {
      if (rand.nextBoolean()) {
        offset = rand.nextInt(100) < 10 ? rand.nextInt(indexSize *2) : rand.nextInt(indexSize/3+1);
      }
      params.add("facet.offset", Integer.toString(offset));
    }

    int limit = 100;
    if (rand.nextInt(100) < 20) {
      if (rand.nextBoolean()) {
        limit = rand.nextInt(100) < 10 ? rand.nextInt(indexSize/2+1) : rand.nextInt(indexSize*2);
      }
      params.add("facet.limit", Integer.toString(limit));
    }

    if (rand.nextBoolean()) {
      params.add("facet.sort", rand.nextBoolean() ? "index" : "count");
    }

    if ( rand.nextInt(100) < 20) {
      final String[] prefixes = new String[] {"A","B","C"};
      params.add("facet.prefix", prefixes[rand.nextInt(prefixes.length)]);
    }

    // don't bother trying to to test facet.missing=true + facet.limit=0
    // distrib & non-distrib are known to behave differently in this 
    if (rand.nextInt(100) < 20 && 0 < params.getInt("facet.limit", 100)) {
      params.add("facet.missing", "true");
    }
    
    if (rand.nextInt(100) < 20) { // assigning only valid vals
      params.add("facet.mincount", rand.nextBoolean() ? "0": "1" );
    }
    
    query(params);
  }
  
  private void checkInvalidMincount() throws SolrServerException, IOException {
    final ModifiableSolrParams params = buildParams();
    if (random().nextBoolean()) {
      params.remove("facet.exists");
      params.set("f."+FLD+".facet.exists","true");
    }
    
    if (random().nextBoolean()) {
      params.set("facet.mincount",  ""+(2+random().nextInt(100)) );
    } else {
      params.set("f."+FLD+".facet.mincount",  ""+(2+random().nextInt(100)) );
    }

    SolrException e = expectThrows(SolrException.class, () -> {
      if (random().nextBoolean()) {
        setDistributedParams(params);
        queryServer(params);
      } else {
        params.set("distrib", "false");
        controlClient.query(params);
      }
    });
    assertEquals(e.code(), ErrorCode.BAD_REQUEST.code);
    assertTrue(e.getMessage().contains("facet.exists"));
    assertTrue(e.getMessage().contains("facet.mincount"));
    assertTrue(e.getMessage().contains(FLD));
  }

  private void checkBasicRequest() throws Exception {
    final ModifiableSolrParams params = buildParams();
    QueryResponse rsp = query(params);
    assertResponse(rsp);
  }

  private void checkWithMinCountEqOne() throws Exception {
    final ModifiableSolrParams params = buildParams("facet.mincount","1");
    QueryResponse rsp = query(params);
    assertResponse(rsp);
  }

  private void checkWithSortCount() throws Exception {
    final ModifiableSolrParams params = buildParams("facet.sort","count");
    QueryResponse rsp = query(params);
    assertResponse(rsp);
  }

  private void checkWithMethodSetPerField() throws Exception {
    final ModifiableSolrParams params = buildParams("f." + FLD + ".facet.exists", "true");
    params.remove("facet.exists");
    QueryResponse rsp = query(params);
    assertResponse(rsp);
  }

  private ModifiableSolrParams buildParams(String... additionalParams) {
    final ModifiableSolrParams params = new ModifiableSolrParams();

    params.add("q", "*:*");
    params.add("rows", "0");
    //params.add("debugQuery", "true");
    params.add("facet", "true");
    params.add("sort", "id asc");
    
    if(random().nextBoolean()){
      params.add("facet.method", "enum");
    }
    
    params.add("facet.exists", "true");
    params.add("facet.field", FLD);
    for(int i = 0; i < additionalParams.length;) {
      params.add(additionalParams[i++], additionalParams[i++]);
    }
    return params;
  }

  private void assertResponse(QueryResponse rsp) {
    final FacetField facetField = rsp.getFacetField(FLD);

    assertThat(facetField.getValueCount(), is(6));
    final List<FacetField.Count> counts = facetField.getValues();
    for (FacetField.Count count : counts) {
      assertThat("Count for: " + count.getName(), count.getCount(), is(1L));
    }
    assertThat(counts.get(0).getName(), is("AAA"));
    assertThat(counts.get(1).getName(), is("B"));
    assertThat(counts.get(2).getName(), is("BB"));
  }
}
