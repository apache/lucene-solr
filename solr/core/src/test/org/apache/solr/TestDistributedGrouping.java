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
package org.apache.solr;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.SolrTestCaseJ4.SuppressPointFields;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;

/**
 * TODO? perhaps use:
 *  http://docs.codehaus.org/display/JETTY/ServletTester
 * rather then open a real connection?
 *
 * @since solr 4.0
 */
@Slow
@SuppressPointFields(bugUrl="https://issues.apache.org/jira/browse/SOLR-10844")
public class TestDistributedGrouping extends BaseDistributedSearchTestCase {

  public TestDistributedGrouping() {
    // SOLR-10844: Even with points suppressed, this test breaks if we (randomize) docvalues="true" on trie fields?!?!?!!?
    System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"false");
  }
  
  String t1="a_t";
  String i1dv="a_idv";
  String i1="a_i1";
  String s1="a_s";
  String s1dv = "a_s_dvo";
  String b1dv = "a_b_dvo";
  String tlong = "other_tl1";
  String tdate_a = "a_n_tdt1"; // use single-valued date field
  String tdate_b = "b_n_tdt1";
  String oddField="oddField_s1";

  @Test
  public void test() throws Exception {
    del("*:*");
    commit();

    handle.clear();
    handle.put("timestamp", SKIPVAL);
    handle.put("_version_", SKIP);
    handle.put("grouped", UNORDERED);   // distrib grouping doesn't guarantee order of top level group commands

    // Test distributed grouping with empty indices
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc");
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc", "hl","true","hl.fl",t1);
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc", "facet", "true", "facet.field", t1);
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc", "stats", "true", "stats.field", i1);
    query("q", "kings", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc", "spellcheck", "true", "spellcheck.build", "true", "qt", "spellCheckCompRH", "df", "subject");
    query("q", "*:*", "fq", s1 + ":a", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc", "group.truncate", "true", "facet", "true", "facet.field", t1);

    indexr(id,1, i1, 100, tlong, 100, i1dv, 100, t1,"now is the time for all good men",
           tdate_a, "2010-04-20T11:00:00Z", b1dv, true,
           tdate_b, "2009-08-20T11:00:00Z", s1dv, "Trillian",
           "foo_f", 1.414f, "foo_b", "true", "foo_d", 1.414d);
    indexr(id,2, i1, 50 , tlong, 50, i1dv, 50, t1,"to come to the aid of their country.",
           tdate_a, "2010-05-02T11:00:00Z", b1dv, false,
           tdate_b, "2009-11-02T11:00:00Z");
    indexr(id,3, i1, 2, tlong, 2,t1,"how now brown cow",
           tdate_a, "2010-05-03T11:00:00Z");
    indexr(id,4, i1, -100 ,tlong, 101, i1dv, 101,
           t1,"the quick fox jumped over the lazy dog", b1dv, true, s1dv, "Zaphod",
           tdate_a, "2010-05-03T11:00:00Z",
           tdate_b, "2010-05-03T11:00:00Z");
    indexr(id,5, i1, 500, tlong, 500 , i1dv, 500,
           t1,"the quick fox jumped way over the lazy dog",
           tdate_a, "2010-05-05T11:00:00Z");
    indexr(id,6, i1, -600, tlong, 600 , i1dv, 600, t1,"humpty dumpy sat on a wall");
    indexr(id,7, i1, 123, tlong, 123 ,i1dv, 123, t1,"humpty dumpy had a great fall");
    indexr(id,8, i1, 876, tlong, 876,
           tdate_b, "2010-01-05T11:00:00Z",
           t1,"all the kings horses and all the kings men");
    indexr(id,9, i1, 7, tlong, 7, i1dv, 7, t1,"couldn't put humpty together again");
    indexr(id,10, i1, 4321, tlong, 4321, i1dv, 4321, t1,"this too shall pass");
    indexr(id,11, i1, -987, tlong, 987, i1dv, 2015,
           t1,"An eye for eye only ends up making the whole world blind.");
    indexr(id,12, i1, 379, tlong, 379, i1dv, 379,
           t1,"Great works are performed, not by strength, but by perseverance.");

    indexr(id, 14, "SubjectTerms_mfacet", new String[]  {"mathematical models", "mathematical analysis"});
    indexr(id, 15, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
    indexr(id, 16, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
    String[] vals = new String[100];
    for (int i=0; i<100; i++) {
      vals[i] = "test " + i;
    }
    indexr(id, 17, "SubjectTerms_mfacet", vals);

    indexr(
        id, 18, i1, 232, tlong, 332, i1dv, 150,
        t1,"no eggs on wall, lesson learned", b1dv, true, s1dv, "dent",
        oddField, "odd man out"
    );
    indexr(
        id, 19, i1, 232, tlong, 432, i1dv, 300,
        t1, "many eggs on wall", b1dv, false, s1dv, "dent",
        oddField, "odd man in"
    );
    indexr(
        id, 20, i1, 232, tlong, 532, i1dv, 150,
        t1, "some eggs on wall", b1dv, false, s1dv, "author",
        oddField, "odd man between"
    );
    indexr(
        id, 21, i1, 232, tlong, 632, i1dv, 120,
        t1, "a few eggs on wall", b1dv, true, s1dv, "ford prefect",
        oddField, "odd man under"
    );
    indexr(
        id, 22, i1, 232, tlong, 732, i1dv, 120,
        t1, "any eggs on wall", b1dv, false, s1dv, "ford prefect",
        oddField, "odd man above"
    );
    indexr(
        id, 23, i1, 233, tlong, 734, i1dv, 120,
        t1, "dirty eggs", b1dv, true, s1dv, "Marvin",
        oddField, "odd eggs"
    );

    for (int i = 100; i < 150; i++) {
      indexr(id, i);
    }

    int[] values = new int[]{9999, 99999, 999999, 9999999};
    for (int shard = 0; shard < clients.size(); shard++) {
      int groupValue = values[shard];
      for (int i = 500; i < 600; i++) {
        index_specific(shard, 
                       i1, groupValue, 
                       s1, "a", 
                       id, i * (shard + 1), 
                       t1, random().nextInt(7));
      }
    }

    commit();

    // test grouping
    // The second sort = id asc . The sorting behaviour is different in dist mode. See TopDocs#merge
    // The shard the result came from matters in the order if both document sortvalues are equal
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc");
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", 0, "sort", i1 + " asc, id asc");
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", "id asc, _docid_ asc");
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", "{!func}add(" + i1 + ",5) asc, id asc");
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc", "facet", "true", "facet.field", t1);
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc", "stats", "true", "stats.field", tlong);
    query("q", "kings", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc", "spellcheck", "true", "spellcheck.build", "true", "qt", "spellCheckCompRH", "df", "subject");
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc", "facet", "true", "hl","true","hl.fl",t1);
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc", "group.sort", "id desc");

    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.offset", 5, "group.limit", -1, "sort", i1 + " asc, id asc");
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "offset", 5, "rows", 5, "group.offset", 5, "group.limit", -1, "sort", i1 + " asc, id asc");
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "offset", 5, "rows", 5, "sort", i1 + " asc, id asc", "group.format", "simple");
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "offset", 5, "rows", 5, "sort", i1 + " asc, id asc", "group.main", "true");
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.offset", 5, "group.limit", 5, "sort", i1 + " asc, id asc", "group.format", "simple", "offset", 5, "rows", 5);
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.offset", 5, "group.limit", 5, "sort", i1 + " asc, id asc", "group.main", "true", "offset", 5, "rows", 5);

    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.query", t1 + ":kings OR " + t1 + ":eggs", "group.limit", -1, "sort", i1 + " asc, id asc");
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.query", t1 + ":kings OR " + t1 + ":eggs", "group.limit", 10, "sort", i1 + " asc, id asc");

    query("q", "*:*", "fl", "id," + i1dv, "group", "true", "group.field", i1dv, "group.limit", 10, "sort", i1 + " asc, id asc");

    
    // SOLR-4150: what if group.query has no matches, 
    // or only matches on one shard
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true",
          "group.query", t1 + ":kings OR " + t1 + ":eggs",
          "group.query", "id:5", // single doc, so only one shard will have it
          "group.limit", -1, "sort", i1 + " asc, id asc");
    query("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true",
          "group.query", t1 + ":kings OR " + t1 + ":eggs",
          "group.query", t1 + ":this_will_never_match",
          "group.limit", 10, "sort", i1 + " asc, id asc");

    // SOLR-4164: main query matches nothing, or only matches on one shard
    query("q", "bogus_s:nothing", // no docs match
          "group", "true", 
          "group.query", t1 + ":this_will_never_match",
          "group.field", i1, 
          "fl", "id", "group.limit", "2", "group.format", "simple");
    query("q", "id:5", // one doc matches, so only one shard
          "rows", 100, "fl", "id," + i1, "group", "true", 
          "group.query", t1 + ":kings OR " + t1 + ":eggs", 
          "group.field", i1,
          "group.limit", 10, "sort", i1 + " asc, id asc");

    // SOLR-13404
    query("q", "*:*",
        "group", "true",
        "group.query", t1 + ":kings OR " + t1 + ":eggs",
        "fl", "id", "group.format", "grouped", "group.limit", "2", "group.offset", "2",
        "sort", i1 + " asc, id asc");
    query("q", "*:*",
        "group", "true",
        "group.query", t1 + ":kings OR " + t1 + ":eggs",
        "fl", "id", "group.format", "grouped", "group.limit", "-12",
        "sort", i1 + " asc, id asc");

    ignoreException("'group.offset' parameter cannot be negative");
    SolrException exception = expectThrows(SolrException.class, () -> query("q", "*:*",
        "group", "true",
        "group.query", t1 + ":kings OR " + t1 + ":eggs", "group.offset", "-1")
    );
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertThat(exception.getMessage(), containsString("'group.offset' parameter cannot be negative"));
    resetExceptionIgnores();

    query("q", "*:*",
        "group", "true",
        "group.query", t1 + ":kings OR " + t1 + ":eggs", "group.limit", "3",
        "fl", "id", "group.format", "simple", "sort", i1 + " asc, id asc");
    query("q", "*:*",
        "group", "true",
        "group.query", t1 + ":kings OR " + t1 + ":eggs",
        "fl", "id", "group.main", "true", "sort", i1 + " asc, id asc");
    query("q", "*:*",
        "group", "true",
        "group.query", t1 + ":kings OR " + t1 + ":eggs", "rows", "13", "start", "2",
        "fl", "id", "group.main", "true", "sort", i1 + " asc, id asc");

    // SOLR-9802
    query("q", "*:*", "group", "true", "group.field", tdate_a, "sort", i1 + " asc, id asc", "fl", "id");

    // SOLR-3109
    query("q", t1 + ":eggs", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", 10, "sort", tlong + " asc, id asc");
    query("q", i1 + ":232", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", 10, "sort", tlong + " asc, id asc");

    // SOLR-12248
    query("q", "*:*", "rows", 100, "fl", "id," + s1dv, "group", "true", "group.field", s1dv, "group.limit", -1, "sort", b1dv + " asc, id asc", "group.sort", "id desc");
    query("q", "*:*", "fl", "id," + b1dv, "group", "true", "group.field", b1dv, "group.limit", 10, "sort", s1dv + " asc, id asc");
    query("q", s1dv + ":dent", "fl", "id," + b1dv, "group", "true", "group.field", b1dv, "group.limit", 10, "sort", i1 + " asc, id asc");

    // In order to validate this we need to make sure that during indexing that all documents of one group only occur on the same shard
    query("q", "*:*", "fq", s1 + ":a", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", 10, "sort", i1 + " asc, id asc", "group.ngroups", "true");
    query("q", "*:*", "fq", s1 + ":a", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", 10, "sort", i1 + " asc, id asc", "group.truncate", "true");
    query("q", "*:*", "fq", s1 + ":a", "rows", 100, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", 10, "sort", i1 + " asc, id asc", "group.truncate", "true", "facet", "true", "facet.field", t1);
    for (String gfacet : new String[] { "true", "false" }) {
      for (String flimit : new String[] { "-100","-1", "1", "2", "10000" }) {
        for (String foffset : new String[] { "0","1", "2", "1000" }) {
          query("q", "*:*", "fq", s1+":a", 
                "rows", 100, "fl", "id,"+i1, "sort", i1+" asc, id asc", 
                "group", "true", "group.field", i1, "group.limit", 10, 
                "facet", "true", "facet.field", t1, "group.facet", gfacet, 
                "facet.limit", flimit, "facet.offset", foffset);
        }
      }
    }

    // SOLR-3316
    query("q", "*:*", "fq", s1 + ":a", "rows", 0, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc", "facet", "true", "facet.field", t1);
    query("q", "*:*", "fq", s1 + ":a", "rows", 0, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " asc, id asc", "group.truncate", "true", "facet", "true", "facet.field", t1);

    // SOLR-3436
    query("q", "*:*", "fq", s1 + ":a", "fl", "id," + i1, "group", "true", "group.field", i1, "sort", i1 + " asc, id asc", "group.ngroups", "true");
    query("q", "*:*", "fq", s1 + ":a", "rows", 0, "fl", "id," + i1, "group", "true", "group.field", i1, "sort", i1 + " asc, id asc", "group.ngroups", "true");

    // SOLR-3960 - include a postfilter
    for (String facet : new String[] { "false", "true"}) {
      for (String fcache : new String[] { "", " cache=false cost=200"}) {
      query("q", "*:*", "rows", 100, "fl", "id," + i1, 
            "group.limit", 10, "sort", i1 + " asc, id asc",
            "group", "true", "group.field", i1, 
            "fq", "{!frange l=50 "+fcache+"}"+tlong,
            "facet.field", t1,
            "facet", facet
            );
      }
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    Object[] q =  {"q", "*:*", "fq", s1 + ":a", "rows", 1, "fl", "id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "group.ngroups", "true"};

    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }

    setDistributedParams(params);

    int which = r.nextInt(clients.size());
    SolrClient client = clients.get(which);
    QueryResponse rsp = client.query(params);
    NamedList nl = (NamedList<?>) rsp.getResponse().get("grouped");
    nl = (NamedList<?>) nl.getVal(0);
    int matches = (Integer) nl.getVal(0);
    int groupCount = (Integer) nl.get("ngroups");
    assertEquals(100 * shardsArr.length, matches);
    assertEquals(shardsArr.length, groupCount);


    // We validate distributed grouping with scoring as first sort.
    // note: this 'q' matches all docs and returns the 'id' as the score, which is unique and so our results should be deterministic.
    handle.put("maxScore", SKIP);// TODO see SOLR-6612
    query("q", "{!func}id_i1", "rows", 100, "fl", "score,id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", i1 + " desc", "group.sort", "score desc"); // SOLR-2955
    query("q", "{!func}id_i1", "rows", 100, "fl", "score,id," + i1, "group", "true", "group.field", i1, "group.limit", -1, "sort", "score desc, _docid_ asc, id asc");
    query("q", "{!func}id_i1", "rows", 100, "fl", "score,id," + i1, "group", "true", "group.field", i1, "group.limit", -1);

    query("q", "*:*",
        "group", "true",
        "group.query", t1 + ":kings OR " + t1 + ":eggs", "group.limit", "3",
        "fl", "id,score", "sort", i1 + " asc, id asc");
    query("q", "*:*",
        "group", "true",
        "group.query", t1 + ":kings OR " + t1 + ":eggs", "group.limit", "3",
        "fl", "id,score", "group.format", "simple", "sort", i1 + " asc, id asc");
    query("q", "*:*",
        "group", "true",
        "group.query", t1 + ":kings OR " + t1 + ":eggs", "group.limit", "3",
        "fl", "id,score", "group.main", "true", "sort", i1 + " asc, id asc");

    // grouping shouldn't care if there are multiple fl params, or what order the fl field names are in
    variantQuery(params("q", "*:*",
                        "group", "true", "group.field", i1dv, "group.limit", "10",
                        "sort", i1 + " asc, id asc")
                 , params("fl", "id," + i1dv)
                 , params("fl", i1dv + ",id")
                 , params("fl", "id", "fl", i1dv)
                 , params("fl", i1dv, "fl", "id")
                 );
    variantQuery(params("q", "*:*", "rows", "100",
                        "group", "true", "group.field", s1dv, "group.limit", "-1", 
                        "sort", b1dv + " asc, id asc",
                        "group.sort", "id desc")
                 , params("fl", "id," + s1dv + "," + tdate_a)
                 , params("fl", "id", "fl", s1dv, "fl", tdate_a)
                 , params("fl", tdate_a, "fl", s1dv, "fl", "id")
                 );
    variantQuery(params("q", "*:*", "rows", "100",
                        "group", "true", "group.field", s1dv, "group.limit", "-1", 
                        "sort", b1dv + " asc, id asc",
                        "group.sort", "id desc")
                 , params("fl", s1dv + "," + tdate_a)
                 , params("fl", s1dv, "fl", tdate_a)
                 , params("fl", tdate_a, "fl", s1dv)
                 );
    variantQuery(params("q", "{!func}id_i1", "rows", "100",
                        "group", "true", "group.field", i1, "group.limit", "-1",
                        "sort", tlong+" asc, id desc")
                 , params("fl", t1 + ",score," + i1dv)
                 , params("fl", t1, "fl", "score", "fl", i1dv)
                 , params("fl", "score", "fl", t1, "fl", i1dv)
                 );
                             
    // some explicit checks of non default sorting, and sort/group.sort with diff clauses
    query("q", "{!func}id_i1", "rows", 100, "fl", tlong + ",id," + i1, "group", "true",
          "group.field", i1, "group.limit", -1,
          "sort", tlong+" asc, id desc");
    query("q", "{!func}id_i1", "rows", 100, "fl", tlong + ",id," + i1, "group", "true",
          "group.field", i1, "group.limit", -1,
          "sort", "id asc",
          "group.sort", tlong+" asc, id desc");
    query("q", "{!func}id_i1", "rows", 100, "fl", tlong + ",id," + i1, "group", "true",
          "group.field", i1, "group.limit", -1,
          "sort", tlong+" asc, id desc",
          "group.sort", "id asc");
    for (boolean withFL : new boolean[] {true, false}) {
      if (withFL) {
        rsp = variantQuery(params("q", "{!func}id_i1", "fq", oddField+":[* TO *]",
                                  "rows", "100",
                                  "group", "true", "group.field", i1, "group.limit", "-1",
                                  "sort", tlong+" asc", "group.sort", oddField+" asc")
                           , params("fl", tlong + ",id," + i1)
                           , params("fl", tlong, "fl", "id", "fl", i1)
                           , params("fl", "id", "fl", i1, "fl", tlong)
                           );
      } else {
        // special check: same query, but empty fl...
        rsp = query("q", "{!func}id_i1", "fq", oddField+":[* TO *]",
                    "rows", "100",
                    "group", "true", "group.field", i1, "group.limit", "-1",
                    "sort", tlong+" asc", "group.sort", oddField+" asc");
      }
      nl = (NamedList<?>) rsp.getResponse().get("grouped");
      nl = (NamedList<?>) nl.get(i1);
      assertEquals(rsp.toString(), 6, nl.get("matches"));
      assertEquals(rsp.toString(), 2, ((List<NamedList<?>>)nl.get("groups")).size());
      nl = ((List<NamedList<?>>)nl.get("groups")).get(0);
      assertEquals(rsp.toString(), 232, nl.get("groupValue"));
      SolrDocumentList docs = (SolrDocumentList) nl.get("doclist");
      assertEquals(docs.toString(), 5, docs.getNumFound());
      //
      assertEquals(docs.toString(), "22", docs.get(0).getFirstValue("id"));
      assertEquals(docs.toString(), 732L, docs.get(0).getFirstValue(tlong));
      assertEquals(docs.toString(), 232,  docs.get(0).getFirstValue(i1));
      //
      assertEquals(docs.toString(), "21", docs.get(4).getFirstValue("id"));
      assertEquals(docs.toString(), 632L, docs.get(4).getFirstValue(tlong));
      assertEquals(docs.toString(), 232,  docs.get(4).getFirstValue(i1));
      //
      if (withFL == false) {
        // exact number varies based on test randomization, but there should always be at least the 8
        // explicitly indexed in these 2 docs...
        assertTrue(docs.toString(), 8 <= docs.get(0).getFieldNames().size());
        assertTrue(docs.toString(), 8 <= docs.get(4).getFieldNames().size());
      }
    }
    
    // grouping on boolean non-stored docValued enabled field
    rsp = query("q", b1dv + ":*", "fl", "id," + b1dv, "group", "true", "group.field",
        b1dv, "group.limit", 10, "sort", b1dv + " asc, id asc");
    nl = (NamedList<?>) rsp.getResponse().get("grouped");
    nl = (NamedList<?>) nl.get(b1dv);
    assertEquals(rsp.toString(), 9, nl.get("matches"));
    assertEquals(rsp.toString(), 2, ((List<NamedList<?>>)nl.get("groups")).size());
    nl = ((List<NamedList<?>>)nl.get("groups")).get(0);
    assertEquals(rsp.toString(), false, nl.get("groupValue"));
    SolrDocumentList docs = (SolrDocumentList) nl.get("doclist");
    assertEquals(docs.toString(), 4, docs.getNumFound());
    
    // Can't validate the response, but can check if no errors occur.
    simpleQuery("q", "*:*", "rows", 100, "fl", "id," + i1, "group", "true", "group.query", t1 + ":kings OR " + t1 + ":eggs", "group.limit", 10, "sort", i1 + " asc, id asc", CommonParams.TIME_ALLOWED, 1);
    
    //Debug
    simpleQuery("q", "*:*", "rows", 10, "fl", "id," + i1, "group", "true", "group.field", i1, "debug", "true");
  }

  private void simpleQuery(Object... queryParams) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    for (int i = 0; i < queryParams.length; i += 2) {
      params.add(queryParams[i].toString(), queryParams[i + 1].toString());
    }
    params.set("shards", shards);
    queryServer(params);
  }

  /**
   * Special helper method for verifying that multiple queries behave the same as each other, 
   * both in distributed and single node queries
   *
   * @param commonParams params that are common to all queries
   * @param variantParams params that will be appended to the common params to create a variant query
   * @return the last response returned by the last variant
   * @see #query
   * @see #compareResponses
   * @see SolrParams#wrapAppended
   */
  protected QueryResponse variantQuery(final SolrParams commonParams,
                                       final SolrParams... variantParams) throws Exception {
    QueryResponse lastResponse = null;
    for (SolrParams extra : variantParams) {
      final QueryResponse rsp = query(SolrParams.wrapAppended(commonParams, extra));
      if (null != lastResponse) {
        compareResponses(rsp, lastResponse);
      }
      lastResponse = rsp;
    }
    return lastResponse;
  }
  
}
