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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.LuceneTestCase.SuppressTempFileChecks;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for SpellCheckComponent's distributed querying
 *
 * @since solr 1.5
 *
 * @see org.apache.solr.handler.component.SpellCheckComponent
 */
@Slow
@SuppressTempFileChecks(bugUrl = "https://issues.apache.org/jira/browse/SOLR-1877 Spellcheck IndexReader leak bug?")
public class DistributedSpellCheckComponentTest extends BaseDistributedSearchTestCase {
  
  public DistributedSpellCheckComponentTest()
  {
    //Helpful for debugging
    //fixShardCount=true;
    //shardCount=2;
    //stress=0;
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    useFactory(null); // need an FS factory
  }

  private void q(Object... q) throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();

    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }

    controlClient.query(params);

    // query a random server
    params.set("shards", shards);
    int which = r.nextInt(clients.size());
    SolrClient client = clients.get(which);
    client.query(params);
  }
  
  @Override
  public void validateControlData(QueryResponse control) throws Exception
  {    
    NamedList<Object> nl = control.getResponse();
    @SuppressWarnings("unchecked")
    NamedList<Object> sc = (NamedList<Object>) nl.get("spellcheck");
    @SuppressWarnings("unchecked")
    NamedList<Object> sug = (NamedList<Object>) sc.get("suggestions");
    if(sug.size()==0) {
      Assert.fail("Control data did not return any suggestions.");
    }
  }

  @Test
  public void test() throws Exception {
    del("*:*");
    index(id, "1", "lowerfilt", "toyota");
    index(id, "2", "lowerfilt", "chevrolet");
    index(id, "3", "lowerfilt", "suzuki");
    index(id, "4", "lowerfilt", "ford");
    index(id, "5", "lowerfilt", "ferrari");
    index(id, "6", "lowerfilt", "jaguar");
    index(id, "7", "lowerfilt", "mclaren");
    index(id, "8", "lowerfilt", "sonata");
    index(id, "9", "lowerfilt", "The quick red fox jumped over the lazy brown dogs.");
    index(id, "10", "lowerfilt", "blue");
    index(id, "12", "lowerfilt", "glue");
    index(id, "13", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.");
    index(id, "14", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.");
    index(id, "15", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.");
    index(id, "16", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.");
    index(id, "17", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.");
    index(id, "18", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.");
    index(id, "19", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.");
    index(id, "20", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.");
    index(id, "21", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.");
    index(id, "22", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.");
    index(id, "23", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.");
    index(id, "24", "lowerfilt", "The quote red fox jumped over the lazy brown dogs.");
    index(id, "25", "lowerfilt", "The quicker red fox jumped over the lazy brown dogs.");
    index(id, "26", "lowerfilt", "rod fix");
    commit();

    handle.clear();
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);
    // we care only about the spellcheck results
    handle.put("response", SKIP);
    handle.put("grouped", SKIP);
    
    //Randomly select either IndexBasedSpellChecker or DirectSolrSpellChecker
    String requestHandlerName = "/spellCheckCompRH_Direct";
    String reqHandlerWithWordbreak = "/spellCheckWithWordbreak_Direct";
    if(random().nextBoolean()) {
      requestHandlerName = "/spellCheckCompRH";
      reqHandlerWithWordbreak = "/spellCheckWithWordbreak";
    } 
    
    //Shortcut names
    String build = SpellingParams.SPELLCHECK_BUILD;
    String extended = SpellingParams.SPELLCHECK_EXTENDED_RESULTS;
    String count = SpellingParams.SPELLCHECK_COUNT;
    String collate = SpellingParams.SPELLCHECK_COLLATE;
    String collateExtended = SpellingParams.SPELLCHECK_COLLATE_EXTENDED_RESULTS;
    String maxCollationTries = SpellingParams.SPELLCHECK_MAX_COLLATION_TRIES;
    String maxCollations = SpellingParams.SPELLCHECK_MAX_COLLATIONS;
    String altTermCount = SpellingParams.SPELLCHECK_ALTERNATIVE_TERM_COUNT;
    String maxResults = SpellingParams.SPELLCHECK_MAX_RESULTS_FOR_SUGGEST;
     
    //Build the dictionary for IndexBasedSpellChecker
    q(buildRequest("*:*", false, "/spellCheckCompRH", false, build, "true"));
    
    //Test Basic Functionality
    query(buildRequest("toyata", true, requestHandlerName, random().nextBoolean(), (String[]) null));
    query(buildRequest("toyata", true, requestHandlerName, random().nextBoolean(), extended, "true"));
    query(buildRequest("bluo", true, requestHandlerName, random().nextBoolean(), extended, "true", count, "4"));
    
    //Test Collate functionality
    query(buildRequest("The quick reb fox jumped over the lazy brown dogs", 
        false, requestHandlerName, random().nextBoolean(), extended, "true", count, "4", collate, "true"));    
    query(buildRequest("lowerfilt:(+quock +reb)", 
        false, requestHandlerName, random().nextBoolean(), extended, "true", count, "10", 
        collate, "true", maxCollationTries, "10", maxCollations, "10", collateExtended, "true"));
    query(buildRequest("lowerfilt:(+quock +reb)", 
        false, requestHandlerName, random().nextBoolean(), extended, "true", count, "10", 
        collate, "true", maxCollationTries, "10", maxCollations, "10", collateExtended, "false"));
    query(buildRequest("lowerfilt:(+quock +reb)", 
        false, requestHandlerName, random().nextBoolean(), extended, "true", count, "10", 
        collate, "true", maxCollationTries, "0", maxCollations, "1", collateExtended, "false"));
    
    //Test context-sensitive collate
    query(buildRequest("lowerfilt:(\"quick red fox\")", 
        false, requestHandlerName, random().nextBoolean(), extended, "true", count, "10", 
        collate, "true", maxCollationTries, "10", maxCollations, "1", collateExtended, "false",
        altTermCount, "5", maxResults, "10"));
    query(buildRequest("lowerfilt:(\"rod fix\")", 
        false, requestHandlerName, random().nextBoolean(), extended, "true", count, "10", 
        collate, "true", maxCollationTries, "10", maxCollations, "1", collateExtended, "false",
        altTermCount, "5", maxResults, "10"));
    query(buildRequest("lowerfilt:(\"rod fix\")", 
        false, requestHandlerName, random().nextBoolean(), extended, "true", count, "10", 
        collate, "true", maxCollationTries, "10", maxCollations, "1", collateExtended, "false",
        altTermCount, "5", maxResults, ".10", "fq", "id:[13 TO 22]"));
    
    //Test word-break spellchecker
    query(buildRequest("lowerfilt:(+quock +redfox +jum +ped)", 
        false, reqHandlerWithWordbreak, random().nextBoolean(), extended, "true", count, "10", 
        collate, "true", maxCollationTries, "0", maxCollations, "1", collateExtended, "true"));
    query(buildRequest("lowerfilt:(+rodfix)", 
        false, reqHandlerWithWordbreak, random().nextBoolean(), extended, "true", count, "10", 
        collate, "true", maxCollationTries, "0", maxCollations, "1", collateExtended, "true"));
    query(buildRequest("lowerfilt:(+son +ata)", 
        false, reqHandlerWithWordbreak, random().nextBoolean(), extended, "true", count, "10", 
        collate, "true", maxCollationTries, "0", maxCollations, "1", collateExtended, "true"));
  }
  private Object[] buildRequest(String q, boolean useSpellcheckQ, String handlerName, boolean useGrouping, String... addlParams) {
    List<Object> params = new ArrayList<>();
    
    params.add("q");
    params.add(useSpellcheckQ ? "*:*" : q);
    
    if(useSpellcheckQ) {
      params.add("spellcheck.q");
      params.add(q);
    }
    
    params.add("fl");
    params.add("id,lowerfilt");
    
    params.add("qt");
    params.add(handlerName);
    
    params.add("shards.qt");
    params.add(handlerName);
    
    params.add("spellcheck");
    params.add("true");
    
    if(useGrouping) {
      params.add("group");
      params.add("true");
      
      params.add("group.field");
      params.add("id");
    }
    
    if(addlParams!=null) {
      params.addAll(Arrays.asList(addlParams));
    }
    return params.toArray(new Object[params.size()]);    
  }
  
}
