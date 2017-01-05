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
package org.apache.solr.request;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.IntervalFacets.FacetInterval;
import org.apache.solr.request.IntervalFacets.IntervalCompareResult;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;

public class TestIntervalFaceting extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-basic.xml", "schema-docValuesFaceting.xml");
  }

  @Override
  public void tearDown() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
    assertU(optimize());
    assertQ(req("*:*"), "//*[@numFound='0']");
    super.tearDown();
  }

  @Test
  public void testMultiValueFields() {
    assertU(adoc("id", "1", "test_ss_dv", "dog"));
    assertU(adoc("id", "2", "test_ss_dv", "cat"));
    assertU(adoc("id", "3", "test_ss_dv", "bird"));
    assertU(adoc("id", "4", "test_ss_dv", "turtle"));
    assertU(commit());
    assertU(adoc("id", "5", "test_ss_dv", "\\goodbye,"));
    assertU(adoc("id", "6", "test_ss_dv", ",hello\\"));
    assertU(adoc("id", "7", "test_ss_dv", "dog"));
    assertU(commit());
    assertU(adoc("id", "8", "test_ss_dv", "dog"));
    assertU(adoc("id", "9", "test_ss_dv", "cat"));
    assertU(adoc("id", "10"));
    assertU(commit());

    assertIntervalQueriesString("test_ss_dv");
    assertU(delQ("*:*"));
    assertU(commit());
    assertU(optimize());

    assertU(adoc("id", "1", "test_ss_dv", "dog", "test_ss_dv", "cat"));
    assertU(adoc("id", "2", "test_ss_dv", "cat", "test_ss_dv", "bird"));
    assertU(commit());

    assertIntervalQuery("test_ss_dv", "[hello,hello]", "0");
    assertIntervalQuery("test_ss_dv", "[dog,dog]", "1");
    assertIntervalQuery("test_ss_dv", "[cat,cat]", "2");
    assertIntervalQuery("test_ss_dv", "[*,*]", "2", "[*,cat)", "1", "[cat,dog)", "2", "[dog,*)", "1");
  }

  @Test
  public void testMultipleSegments() {
    assertU(adoc("id", "1", "test_s_dv", "dog"));
    assertU(adoc("id", "2", "test_s_dv", "cat"));
    assertU(adoc("id", "3", "test_s_dv", "bird"));
    assertU(adoc("id", "4", "test_s_dv", "turtle"));
    assertU(commit());
    assertU(adoc("id", "5", "test_s_dv", "\\goodbye,"));
    assertU(adoc("id", "6", "test_s_dv", ",hello\\"));
    assertU(adoc("id", "7", "test_s_dv", "dog"));
    assertU(commit());
    assertU(adoc("id", "8", "test_s_dv", "dog"));
    assertU(adoc("id", "9", "test_s_dv", "cat"));
    assertU(adoc("id", "10"));
    assertU(commit());
    int i = 11;
    while (getNumberOfReaders() < 2 && i < 10) {
      //try to get more than one segment
      assertU(adoc("id", String.valueOf(i), "test_i_dv", String.valueOf(i)));
      assertU(commit());
    }
    if (getNumberOfReaders() < 2) {
      // It is OK if for some seeds we fall into this case (for example, TieredMergePolicy with
      // segmentsPerTier=2). Most of the case we shouldn't and the test should proceed.
      log.warn("Could not generate more than 1 segment for this seed. Will skip the test");
      return;
    }

    assertIntervalQueriesString("test_s_dv");

  }

  @Test
  public void testMultipleTerms() {
    assertU(adoc("id", "1", "test_s_dv", "Buenos Aires"));
    assertU(adoc("id", "2", "test_s_dv", "New York"));
    assertU(adoc("id", "3", "test_s_dv", "Los Angeles"));
    assertU(adoc("id", "4", "test_s_dv", "San Francisco"));
    assertU(adoc("id", "5", "test_s_dv", "Las Vegas"));
    assertU(adoc("id", "6", "test_s_dv", "São Paulo"));
    assertU(adoc("id", "10"));
    assertU(commit());

    assertIntervalQuery("test_s_dv", "[*,*]", "6");
    assertIntervalQuery("test_s_dv", "[A,C]", "1");
    assertIntervalQuery("test_s_dv", "[Buenos Aires,Buenos Aires]", "1");
    assertIntervalQuery("test_s_dv", "[Las,Los]", "1");
    assertIntervalQuery("test_s_dv", "[Las,Los Angeles]", "2");
    assertIntervalQuery("test_s_dv", "[Las,Los Angeles)", "1");
    assertIntervalQuery("test_s_dv", "[Las\\,,Los Angeles]", "1");
  }

  private int getNumberOfReaders() {
    RefCounted<SolrIndexSearcher> searcherRef = h.getCore().getSearcher();
    try {
      SolrIndexSearcher searcher = searcherRef.get();
      return searcher.getTopReaderContext().leaves().size();
    } finally {
      searcherRef.decref();
    }
  }

  @Test
  public void testBasic() {
    assertU(adoc("id", "1", "test_s_dv", "dog"));
    assertU(adoc("id", "2", "test_s_dv", "cat"));
    assertU(adoc("id", "3", "test_s_dv", "bird"));
    assertU(adoc("id", "4", "test_s_dv", "turtle"));
    assertU(adoc("id", "5", "test_s_dv", "\\goodbye,"));
    assertU(adoc("id", "6", "test_s_dv", ",hello\\"));
    assertU(adoc("id", "7", "test_s_dv", "dog"));
    assertU(adoc("id", "8", "test_s_dv", "dog"));
    assertU(adoc("id", "9", "test_s_dv", "cat"));
    assertU(adoc("id", "10"));
    assertU(commit());

    assertIntervalQueriesString("test_s_dv");

    // error cases
    assertQEx("missing beginning of range",
        req("fl", "test_s_dv", "q", "*:*", "facet", "true", "facet.interval", "test_s_dv",
            "f.test_s_dv.facet.interval.set", "bird,bird]"),
        SolrException.ErrorCode.BAD_REQUEST
    );
    assertQEx("only separator is escaped",
        req("fl", "test_s_dv", "q", "*:*", "facet", "true", "facet.interval", "test_s_dv",
            "f.test_s_dv.facet.interval.set", "(bird\\,turtle]"),
        SolrException.ErrorCode.BAD_REQUEST
    );
    assertQEx("missing separator",
        req("fl", "test_s_dv", "q", "*:*", "facet", "true", "facet.interval", "test_s_dv",
            "f.test_s_dv.facet.interval.set", "(bird]"),
        SolrException.ErrorCode.BAD_REQUEST
    );
    assertQEx("missing end of range",
        req("fl", "test_s_dv", "q", "*:*", "facet", "true", "facet.interval", "test_s_dv",
            "f.test_s_dv.facet.interval.set", "(bird,turtle"),
        SolrException.ErrorCode.BAD_REQUEST
    );
  }

  @Test
  public void testMultipleFields() {
    assertU(adoc("id", "1", "test_s_dv", "dog", "test_l_dv", "1"));
    assertU(adoc("id", "2", "test_s_dv", "cat", "test_l_dv", "2"));
    assertU(adoc("id", "3", "test_s_dv", "bird", "test_l_dv", "3"));
    assertU(adoc("id", "4", "test_s_dv", "turtle", "test_l_dv", "4"));
    assertU(adoc("id", "5", "test_s_dv", "\\goodbye,", "test_l_dv", "5"));
    assertU(adoc("id", "6", "test_s_dv", ",hello\\", "test_l_dv", "6"));
    assertU(adoc("id", "7", "test_s_dv", "dog", "test_l_dv", "7"));
    assertU(adoc("id", "8", "test_s_dv", "dog", "test_l_dv", "8"));
    assertU(adoc("id", "9", "test_s_dv", "cat", "test_l_dv", "9"));
    assertU(adoc("id", "10"));
    assertU(commit());

    assertQ(req("q", "*:*", "facet", "true", "facet.interval", "test_s_dv",
            "facet.interval", "test_l_dv", "f.test_s_dv.facet.interval.set", "[cat,dog]",
            "f.test_l_dv.facet.interval.set", "[3,6]",
            "f.test_l_dv.facet.interval.set", "[5,9]"),
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[cat,dog]'][.=5]",
        "//lst[@name='facet_intervals']/lst[@name='test_l_dv']/int[@name='[3,6]'][.=4]",
        "//lst[@name='facet_intervals']/lst[@name='test_l_dv']/int[@name='[5,9]'][.=5]");
    
  }
  
  @Test
  public void testWithFieldCache() {
    assertU(adoc("id", "1", "test_s", "dog", "test_l", "1"));
    assertU(adoc("id", "2", "test_s", "cat", "test_l", "2"));
    assertU(adoc("id", "3", "test_s", "bird", "test_l", "3"));
    assertU(adoc("id", "4", "test_s", "turtle", "test_l", "4"));
    assertU(adoc("id", "5", "test_s", "\\goodbye,", "test_l", "5"));
    assertU(adoc("id", "6", "test_s", ",hello\\", "test_l", "6"));
    assertU(adoc("id", "7", "test_s", "dog", "test_l", "7"));
    assertU(adoc("id", "8", "test_s", "dog", "test_l", "8"));
    assertU(adoc("id", "9", "test_s", "cat", "test_l", "9"));
    assertU(adoc("id", "10"));
    assertU(commit());

    assertQ(req("q", "*:*", "facet", "true", "facet.interval", "test_s",
            "facet.interval", "test_l", "f.test_s.facet.interval.set", "[cat,dog]",
            "f.test_l.facet.interval.set", "[3,6]",
            "f.test_l.facet.interval.set", "[5,9]"),
        "//lst[@name='facet_intervals']/lst[@name='test_s']/int[@name='[cat,dog]'][.=5]",
        "//lst[@name='facet_intervals']/lst[@name='test_l']/int[@name='[3,6]'][.=4]",
        "//lst[@name='facet_intervals']/lst[@name='test_l']/int[@name='[5,9]'][.=5]");
    
  }

  @Test
  @Slow
  public void testRandom() throws Exception {
    // All field values will be a number between 0 and cardinality
    int cardinality = 100000;
    // Fields to use for interval faceting
    String[] fields = new String[]{"test_s_dv", "test_i_dv", "test_l_dv", "test_f_dv", "test_d_dv",
        "test_ss_dv", "test_is_dv", "test_fs_dv", "test_ls_dv", "test_ds_dv", "test_s", "test_i", 
        "test_l", "test_f", "test_d", "test_ss", "test_is", "test_fs", "test_ls", "test_ds"};
    for (int i = 0; i < atLeast(500); i++) {
      if (random().nextInt(50) == 0) {
        //have some empty docs
        assertU(adoc("id", String.valueOf(i)));
        continue;
      }

      if (random().nextInt(100) == 0 && i > 0) {
        //delete some docs
        assertU(delI(String.valueOf(i - 1)));
      }
      String[] docFields = new String[(random().nextInt(5)) * 10 + 12];
      docFields[0] = "id";
      docFields[1] = String.valueOf(i);
      docFields[2] = "test_s";
      docFields[3] = String.valueOf(random().nextInt(cardinality));
      docFields[4] = "test_i";
      docFields[5] = String.valueOf(random().nextInt(cardinality));
      docFields[6] = "test_l";
      docFields[7] = String.valueOf(random().nextInt(cardinality));
      docFields[8] = "test_f";
      docFields[9] = String.valueOf(random().nextFloat() * cardinality);
      docFields[10] = "test_d";
      docFields[11] = String.valueOf(random().nextDouble() * cardinality);
      for (int j = 12; j < docFields.length; ) {
        docFields[j++] = "test_ss";
        docFields[j++] = String.valueOf(random().nextInt(cardinality));
        docFields[j++] = "test_is";
        docFields[j++] = String.valueOf(random().nextInt(cardinality));
        docFields[j++] = "test_ls";
        docFields[j++] = String.valueOf(random().nextInt(cardinality));
        docFields[j++] = "test_fs";
        docFields[j++] = String.valueOf(random().nextFloat() * cardinality);
        docFields[j++] = "test_ds";
        docFields[j++] = String.valueOf(random().nextDouble() * cardinality);
      }
      assertU(adoc(docFields));
      if (random().nextInt(50) == 0) {
        assertU(commit());
      }
    }
    assertU(commit());

    for (int i = 0; i < atLeast(100); i++) {
      doTestQuery(cardinality, fields);
    }

  }

  /**
   * Executes one query using interval faceting and compares with the same query using
   * facet query with the same range
   */
  @SuppressWarnings("unchecked")
  private void doTestQuery(int cardinality, String[] fields) throws Exception {
    String[] startOptions = new String[]{"(", "["};
    String[] endOptions = new String[]{")", "]"};
    // the query should match some documents in most cases
    Integer[] qRange = getRandomRange(cardinality, "id");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "id:[" + qRange[0] + " TO " + qRange[1] + "]");
    params.set("facet", "true");
    String field = fields[random().nextInt(fields.length)]; //choose from any of the fields
    params.set("facet.interval", field);
    // number of intervals
    for (int i = 0; i < 1 + random().nextInt(20); i++) {
      Integer[] interval = getRandomRange(cardinality, field);
      String open = startOptions[interval[0] % 2];
      String close = endOptions[interval[1] % 2];
      params.add("f." + field + ".facet.interval.set", open + interval[0] + "," + interval[1] + close);
      params.add("facet.query", field + ":" + open.replace('(', '{') + interval[0] + " TO " + interval[1] + close.replace(')', '}'));
    }
    SolrQueryRequest req = req(params);
    try {
      SolrQueryResponse rsp = h.queryAndResponse("standard", req);
      NamedList<Object> facetQueries = (NamedList<Object>) ((NamedList<Object>) rsp.getValues().get("facet_counts")).get("facet_queries");
      NamedList<Object> facetIntervals = (NamedList<Object>) ((NamedList<Object>) (NamedList<Object>) ((NamedList<Object>) rsp.getValues().get("facet_counts"))
          .get("facet_intervals")).get(field);
      assertEquals("Responses don't have the same number of facets: \n" + facetQueries + "\n" + facetIntervals,
          facetQueries.size(), facetIntervals.size());
      for (int i = 0; i < facetIntervals.size(); i++) {
        assertEquals("Interval did not match: " + facetIntervals.getName(i), facetIntervals.getVal(i).toString(),
            facetQueries.get(field + ":" + facetIntervals.getName(i).replace(",", " TO ").replace('(', '{').replace(')', '}')).toString());
      }
    } finally {
      req.close();
    }

  }

  /**
   * Returns a random range. It's guaranteed that the first
   * number will be lower than the second, and both of them
   * between 0 (inclusive) and <code>max</code> (exclusive).
   * If the fieldName is "test_s_dv" or "test_ss_dv" (the
   * two fields used for Strings), the comparison will be done
   * alphabetically
   */
  private Integer[] getRandomRange(int max, String fieldName) {
    Integer[] values = new Integer[2];
    values[0] = random().nextInt(max);
    values[1] = random().nextInt(max);
    if (fieldName.startsWith("test_s")) {
      Arrays.sort(values, (o1, o2) -> String.valueOf(o1).compareTo(String.valueOf(o2)));
    } else {
      Arrays.sort(values);
    }
    return values;
  }

  @Test
  public void testParse() throws SyntaxError {
    assertInterval("test_l_dv", "(0,2)", new long[]{1}, new long[]{0, -1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{2, 3, Integer.MAX_VALUE, Long.MAX_VALUE});
    assertInterval("test_l_dv", "(0,2]", new long[]{1, 2}, new long[]{0, -1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{3, Integer.MAX_VALUE, Long.MAX_VALUE});
    assertInterval("test_l_dv", "[0,2]", new long[]{0, 1, 2}, new long[]{-1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{3, Integer.MAX_VALUE, Long.MAX_VALUE});
    assertInterval("test_l_dv", "[0,2)", new long[]{0, 1}, new long[]{-1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{2, 3, Integer.MAX_VALUE, Long.MAX_VALUE});

    assertInterval("test_l_dv", "(0,*)", new long[]{1, 2, 3, Integer.MAX_VALUE, Long.MAX_VALUE}, new long[]{-1, 0, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{});
    assertInterval("test_l_dv", "(*,2)", new long[]{-1, Integer.MIN_VALUE, Long.MIN_VALUE, 0, 1}, new long[]{}, new long[]{2, 3, Integer.MAX_VALUE, Long.MAX_VALUE});
    assertInterval("test_l_dv", "(*,*)", new long[]{-1, Integer.MIN_VALUE, Long.MIN_VALUE, 0, 1, 2, 3, Integer.MAX_VALUE, Long.MAX_VALUE}, new long[]{}, new long[]{});

    assertInterval("test_l_dv", "[0,*]", new long[]{0, 1, 2, 3, Integer.MAX_VALUE, Long.MAX_VALUE}, new long[]{-1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{});
    assertInterval("test_l_dv", "[*,2]", new long[]{-1, Integer.MIN_VALUE, Long.MIN_VALUE, 0, 1, 2}, new long[]{}, new long[]{3, Integer.MAX_VALUE, Long.MAX_VALUE});
    assertInterval("test_l_dv", "[*,*]", new long[]{-1, Integer.MIN_VALUE, Long.MIN_VALUE, 0, 1, 2, 3, Integer.MAX_VALUE, Long.MAX_VALUE}, new long[]{}, new long[]{});

    assertInterval("test_l_dv", "(2,2)", new long[]{}, new long[]{2, 1, 0, -1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{3, Integer.MAX_VALUE, Long.MAX_VALUE});
    assertInterval("test_l_dv", "(0,0)", new long[]{}, new long[]{0, -1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{1, 2, 3, Integer.MAX_VALUE, Long.MAX_VALUE});

    assertInterval("test_l_dv", "(0," + Long.MAX_VALUE + "]", new long[]{1, 2, 3, Integer.MAX_VALUE, Long.MAX_VALUE}, new long[]{0, -1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{});
    assertInterval("test_l_dv", "(0," + Long.MAX_VALUE + ")", new long[]{1, 2, 3, Integer.MAX_VALUE}, new long[]{0, -1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{Long.MAX_VALUE});
    assertInterval("test_l_dv", "(" + Long.MIN_VALUE + ",0)", new long[]{-1, Integer.MIN_VALUE}, new long[]{Long.MIN_VALUE}, new long[]{1, 2, Integer.MAX_VALUE, Long.MAX_VALUE});
    assertInterval("test_l_dv", "[" + Long.MIN_VALUE + ",0)", new long[]{-1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{}, new long[]{1, 2, Integer.MAX_VALUE, Long.MAX_VALUE});
    assertInterval("test_l_dv", "[" + Long.MIN_VALUE + "," + Long.MAX_VALUE + "]", new long[]{-1, Integer.MIN_VALUE, Long.MIN_VALUE, 1, 2, Integer.MAX_VALUE, Long.MAX_VALUE}, new long[]{}, new long[]{});
    assertInterval("test_l_dv", "(" + Long.MIN_VALUE + "," + Long.MAX_VALUE + ")", new long[]{-1, Integer.MIN_VALUE, 1, 2, Integer.MAX_VALUE}, new long[]{Long.MIN_VALUE}, new long[]{Long.MAX_VALUE});

    assertInterval("test_l_dv", "( 0,2)", new long[]{1}, new long[]{0, -1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{2, 3, Integer.MAX_VALUE, Long.MAX_VALUE});
    assertInterval("test_l_dv", "(   0,2)", new long[]{1}, new long[]{0, -1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{2, 3, Integer.MAX_VALUE, Long.MAX_VALUE});
    assertInterval("test_l_dv", "(0,   2)", new long[]{1}, new long[]{0, -1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{2, 3, Integer.MAX_VALUE, Long.MAX_VALUE});
    assertInterval("test_l_dv", "(  0  ,   2  )", new long[]{1}, new long[]{0, -1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{2, 3, Integer.MAX_VALUE, Long.MAX_VALUE});
    assertInterval("test_l_dv", "  (  0  ,   2  )  ", new long[]{1}, new long[]{0, -1, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{2, 3, Integer.MAX_VALUE, Long.MAX_VALUE});

    assertInterval("test_l_dv", "[-1,1]", new long[]{-1, 0, 1}, new long[]{-2, Integer.MIN_VALUE, Long.MIN_VALUE}, new long[]{2, 3, Integer.MAX_VALUE, Long.MAX_VALUE});

    assertStringInterval("test_s_dv", "[A,B]", "A", "B");
    assertStringInterval("test_s_dv", "[A,b]", "A", "b");
    assertStringInterval("test_s_dv", "[A\\,,B]", "A,", "B");
    assertStringInterval("test_s_dv", "[A\\),B]", "A)", "B");
    assertStringInterval("test_s_dv", "['A',B]", "'A'", "B");
    assertStringInterval("test_s_dv", "[\"A\",B]", "\"A\"", "B");
    assertStringInterval("test_s_dv", "[A B C,B]", "A B C", "B");
    assertStringInterval("test_s_dv", "[ A B C ,B]", "A B C", "B");
//    These two are currently not possible
//    assertStringInterval("test_s_dv", "[\\ A B C ,B]", " A B C", "B");
//    assertStringInterval("test_s_dv", "[\\*,B]", "*", "B");
    
    //invalid intervals
    assertBadInterval("test_l_dv", "0,2)", "Invalid start character");
    assertBadInterval("test_l_dv", "{0,2)", "Invalid start character");
    assertBadInterval("test_l_dv", "(0,2", "Invalid end character");
    assertBadInterval("test_l_dv", "(0,2}", "Invalid end character");
    assertBadInterval("test_l_dv", "(0, )", "Empty interval limit");
    assertBadInterval("test_l_dv", "(0)", "Missing unescaped comma separating interval");
    assertBadInterval("test_l_dv", "(,0)", "Empty interval limit");
    assertBadInterval("test_l_dv", "(0 2)", "Missing unescaped comma separating interval");
    assertBadInterval("test_l_dv", "(0 TO 2)", "Missing unescaped comma separating interval");
    assertBadInterval("test_l_dv", "(0 \\, 2)", "Missing unescaped comma separating interval");
    assertBadInterval("test_l_dv", "(A, 2)", "Invalid start interval for key");
    assertBadInterval("test_l_dv", "(2, A)", "Invalid end interval for key");
    assertBadInterval("test_l_dv", "(0,)", "Empty interval limit");
    assertBadInterval("test_l_dv", "(0,-1)", "Start is higher than end in interval for key");

    assertBadInterval("test_s_dv", "A,B)", "Invalid start character");
    assertBadInterval("test_s_dv", "(B,A)", "Start is higher than end in interval for key");
    assertBadInterval("test_s_dv", "(a,B)", "Start is higher than end in interval for key");
    
    assertIntervalKey("test_s_dv", "[A,B]", "[A,B]");
    assertIntervalKey("test_s_dv", "(A,*]", "(A,*]");
    assertIntervalKey("test_s_dv", "{!}(A,*]", "(A,*]");
    assertIntervalKey("test_s_dv", "{!key=foo}(A,*]", "foo");
    assertIntervalKey("test_s_dv", "{!key='foo'}(A,*]", "foo");
    assertIntervalKey("test_s_dv", "{!key='foo bar'}(A,*]", "foo bar");
    assertIntervalKey("test_s_dv", "{!key='foo' bar}(A,*]", "foo");
    assertIntervalKey("test_s_dv", "{!key=$i}(A,*]", "foo", "i", "foo");
    assertIntervalKey("test_s_dv", "{!key=$i}(A,*]", "foo bar", "i", "foo bar");
    assertIntervalKey("test_s_dv", "{!key=$i}(A,*]", "'foo'", "i", "'foo'");
    assertIntervalKey("test_s_dv", "{!key=$i}(A,*]", "\"foo\"", "i", "\"foo\"");
    assertIntervalKey("test_s_dv", "{!key='[A,B]'}(A,B)", "[A,B]");
    assertIntervalKey("test_s_dv", "{!key='\\{\\{\\{'}(A,B)", "{{{");
    assertIntervalKey("test_s_dv", "{!key='\\{A,B\\}'}(A,B)", "{A,B}");
    assertIntervalKey("test_s_dv", "{!key='\"A,B\"'}(A,B)", "\"A,B\"");
    assertIntervalKey("test_s_dv", "{!key='A..B'}(A,B)", "A..B");
    assertIntervalKey("test_s_dv", "{!key='A TO B'}(A,B)", "A TO B");
    
    
    assertU(adoc("id", "1", "test_s_dv", "dog", "test_l_dv", "1"));
    assertU(adoc("id", "2", "test_s_dv", "cat", "test_l_dv", "2"));
    assertU(adoc("id", "3", "test_s_dv", "bird", "test_l_dv", "3"));
    assertU(adoc("id", "4", "test_s_dv", "turtle", "test_l_dv", "4"));
    assertU(adoc("id", "5", "test_s_dv", "\\goodbye,", "test_l_dv", "5"));
    assertU(adoc("id", "6", "test_s_dv", ",hello\\", "test_l_dv", "6"));
    assertU(adoc("id", "7", "test_s_dv", "dog", "test_l_dv", "7"));
    assertU(adoc("id", "8", "test_s_dv", "dog", "test_l_dv", "8"));
    assertU(adoc("id", "9", "test_s_dv", "cat", "test_l_dv", "9"));
    assertU(adoc("id", "10"));
    assertU(commit());
    
    // facet.interval not set
    assertQ(req("q", "*:*", "facet", "true",
        "f.test_s_dv.facet.interval.set", "[cat,dog]",
        "f.test_l_dv.facet.interval.set", "[3,6]",
        "f.test_l_dv.facet.interval.set", "[5,9]"),
      "count(//lst[@name='facet_intervals']/lst)=0");
    
    // facet.interval only on one of the fields
    assertQ(req("q", "*:*", "facet", "true",
        "facet.interval", "test_s_dv",
        "f.test_s_dv.facet.interval.set", "[cat,dog]",
        "f.test_l_dv.facet.interval.set", "[3,6]",
        "f.test_l_dv.facet.interval.set", "[5,9]"),
      "count(//lst[@name='facet_intervals']/lst)=1",
      "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[cat,dog]'][.=5]");
    
    // existing fields in facet.interval with no intervals defined
    assertQEx("Unexpected exception", 
        "Missing required parameter: f.test_l_dv.facet.interval.set (or default: facet.interval.set)",
        req("q", "*:*", "facet", "true",
        "facet.interval", "test_s_dv",
        "facet.interval", "test_l_dv",
        "f.test_s_dv.facet.interval.set", "[cat,dog]"),
        SolrException.ErrorCode.BAD_REQUEST);
    
    // use of facet.interval.set
    assertQ(req("q", "*:*", "facet", "true",
        "facet.interval", "test_s_dv",
        "facet.interval", "test_l_dv",
        "facet.interval.set", "[1,2]"),
      "count(//lst[@name='facet_intervals']/lst)=2",
      "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[1,2]'][.=0]",
      "//lst[@name='facet_intervals']/lst[@name='test_l_dv']/int[@name='[1,2]'][.=2]"
      );
    
    // multiple facet.interval.set 
    assertQ(req("q", "*:*", "facet", "true",
        "facet.interval", "test_s_dv",
        "facet.interval", "test_l_dv",
        "facet.interval.set", "[1,2]",
        "facet.interval.set", "[2,3]",
        "facet.interval.set", "[3,4]"),
      "count(//lst[@name='facet_intervals']/lst)=2",
      "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[1,2]'][.=0]",
      "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[2,3]'][.=0]",
      "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[3,4]'][.=0]",
      "//lst[@name='facet_intervals']/lst[@name='test_l_dv']/int[@name='[1,2]'][.=2]",
      "//lst[@name='facet_intervals']/lst[@name='test_l_dv']/int[@name='[2,3]'][.=2]",
      "//lst[@name='facet_intervals']/lst[@name='test_l_dv']/int[@name='[3,4]'][.=2]"
      );
    
    // use of facet.interval.set and override
    assertQ(req("q", "*:*", "facet", "true",
        "facet.interval", "test_s_dv",
        "facet.interval", "test_l_dv",
        "facet.interval.set", "[1,2]",
        "f.test_l_dv.facet.interval.set", "[3,4]",
        "f.test_l_dv.facet.interval.set", "[4,5]"),
      "count(//lst[@name='facet_intervals']/lst)=2",
      "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[1,2]'][.=0]",
      "count(//lst[@name='facet_intervals']/lst[@name='test_l_dv']/int)=2", // interval [1,2] not present
      "//lst[@name='facet_intervals']/lst[@name='test_l_dv']/int[@name='[3,4]'][.=2]",
      "//lst[@name='facet_intervals']/lst[@name='test_l_dv']/int[@name='[4,5]'][.=2]"
      );
    
    assertQ(req("q", "*:*", "facet", "true",
        "facet.interval", "test_s_dv",
        "facet.interval", "test_l_dv",
        "facet.interval.set", "[1,2]",
        "facet.interval.set", "[2,3]",
        "facet.interval.set", "[3,4]",
        "f.test_s_dv.facet.interval.set", "[cat,dog]"),
      "count(//lst[@name='facet_intervals']/lst)=2",
      "count(//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int)=1", // only [cat,dog] in test_s_dv
      "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[cat,dog]'][.=5]",
      "count(//lst[@name='facet_intervals']/lst[@name='test_l_dv']/int)=3",
      "//lst[@name='facet_intervals']/lst[@name='test_l_dv']/int[@name='[1,2]'][.=2]",
      "//lst[@name='facet_intervals']/lst[@name='test_l_dv']/int[@name='[2,3]'][.=2]",
      "//lst[@name='facet_intervals']/lst[@name='test_l_dv']/int[@name='[3,4]'][.=2]"
      );
    
    // use of facet.interval.set with wrong field type
    assertQEx("Unexpected Exception",
        "Invalid start interval",
        req("q", "*:*", "facet", "true",
        "facet.interval", "test_l_dv",
        "f.test_l_dv.facet.interval.set", "[cat,dog]"),
        SolrException.ErrorCode.BAD_REQUEST);

  }

  private void assertStringInterval(String fieldName, String intervalStr,
                                    String expectedStart, String expectedEnd) throws SyntaxError {
    SchemaField f = h.getCore().getLatestSchema().getField(fieldName);
    FacetInterval interval = new FacetInterval(f, intervalStr, new ModifiableSolrParams());

    assertEquals("Expected start " + expectedStart + " but found " + f.getType().toObject(f, interval.start),
        interval.start, new BytesRef(f.getType().toInternal(expectedStart)));

    assertEquals("Expected end " + expectedEnd + " but found " + f.getType().toObject(f, interval.end),
        interval.end, new BytesRef(f.getType().toInternal(expectedEnd)));
  }

  private void assertBadInterval(String fieldName, String intervalStr, String errorMsg) {
    SchemaField f = h.getCore().getLatestSchema().getField(fieldName);
    try {
      new FacetInterval(f, intervalStr, new ModifiableSolrParams());
      fail("Expecting SyntaxError for interval String: " + intervalStr);
    } catch (SyntaxError e) {
      assertTrue("Unexpected error message for interval String: " + intervalStr + ": " +
          e.getMessage(), e.getMessage().contains(errorMsg));
    }
  }

  private void assertInterval(String fieldName, String intervalStr, long[] included, long[] lowerThanStart, long[] graterThanEnd) throws SyntaxError {
    SchemaField f = h.getCore().getLatestSchema().getField(fieldName);
    FacetInterval interval = new FacetInterval(f, intervalStr, new ModifiableSolrParams());
    for (long l : included) {
      assertEquals("Value " + l + " should be INCLUDED for interval" + interval,
          IntervalCompareResult.INCLUDED, interval.includes(l));
    }
    for (long l : lowerThanStart) {
      assertEquals("Value " + l + " should be LOWER_THAN_START for inteval " + interval,
          IntervalCompareResult.LOWER_THAN_START, interval.includes(l));
    }
    for (long l : graterThanEnd) {
      assertEquals("Value " + l + " should be GRATER_THAN_END for inteval " + interval,
          IntervalCompareResult.GREATER_THAN_END, interval.includes(l));
    }

  }
  
  private void assertIntervalKey(String fieldName, String intervalStr,
      String expectedKey, String...params) throws SyntaxError {
    assert (params.length&1)==0:"Params must have an even number of elements";
    SchemaField f = h.getCore().getLatestSchema().getField(fieldName);
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    for (int i = 0; i < params.length - 1;) {
      solrParams.set(params[i], params[i+1]);
      i+=2;
    }
    FacetInterval interval = new FacetInterval(f, intervalStr, solrParams);
    
    assertEquals("Expected key " + expectedKey + " but found " + interval.getKey(), 
        expectedKey, interval.getKey());
  }
  
  public void testChangeKey() {
    assertU(adoc("id", "1", "test_s_dv", "dog"));
    assertU(adoc("id", "2", "test_s_dv", "cat"));
    assertU(adoc("id", "3", "test_s_dv", "bird"));
    assertU(adoc("id", "4", "test_s_dv", "cat"));
    assertU(adoc("id", "5", "test_s_dv", "turtle"));
    assertU(adoc("id", "6", "test_s_dv", "dog"));
    assertU(adoc("id", "7", "test_s_dv", "dog"));
    assertU(adoc("id", "8", "test_s_dv", "dog"));
    assertU(adoc("id", "9", "test_s_dv", "cat"));
    assertU(adoc("id", "10"));
    assertU(commit());
    
    assertQ(req("q", "*:*", "facet", "true", "facet.interval", "test_s_dv", 
        "f.test_s_dv.facet.interval.set", "{!key=foo}[bird,bird]", 
        "f.test_s_dv.facet.interval.set", "{!key='bar'}(bird,dog)"), 
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='foo'][.=1]",
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='bar'][.=3]");
    
    assertQ(req("q", "*:*", "facet", "true", "facet.interval", "test_s_dv", 
        "f.test_s_dv.facet.interval.set", "{!key=Birds}[bird,bird]", 
        "f.test_s_dv.facet.interval.set", "{!key='foo bar'}(bird,dog)"), 
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='Birds'][.=1]",
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='foo bar'][.=3]");
    
    assertQ(req("q", "*:*", "facet", "true", "facet.interval", "test_s_dv", 
        "f.test_s_dv.facet.interval.set", "{!key=$p}[bird,bird]", 
        "p", "foo bar"), 
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='foo bar'][.=1]");
    
    assertQ(req("q", "*:*", "facet", "true", "facet.interval", "test_s_dv", 
        "f.test_s_dv.facet.interval.set", "{!key='[bird,\\}'}[bird,*]", 
        "f.test_s_dv.facet.interval.set", "{!key='\\{bird,dog\\}'}(bird,dog)",
        "f.test_s_dv.facet.interval.set", "{!key='foo'}(bird,dog})"), 
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[bird,}'][.=9]",
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='{bird,dog}'][.=3]",
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='foo'][.=7]");
    
  }

  @Test
  public void testLongFields() {
    assertU(adoc("id", "1", "test_l_dv", "0"));
    assertU(adoc("id", "2", "test_l_dv", "1"));
    assertU(adoc("id", "3", "test_l_dv", "2"));
    assertU(adoc("id", "4", "test_l_dv", "3"));
    assertU(adoc("id", "5", "test_l_dv", "4"));
    assertU(adoc("id", "6", "test_l_dv", "5"));
    assertU(adoc("id", "7", "test_l_dv", "6"));
    assertU(adoc("id", "8", "test_l_dv", "7"));
    assertU(adoc("id", "9", "test_l_dv", "8"));
    assertU(adoc("id", "10"));
    assertU(adoc("id", "11", "test_l_dv", "10"));
    assertU(commit());

    assertIntervalQueriesNumeric("test_l_dv");
    assertU(adoc("id", "12", "test_l_dv", String.valueOf(Long.MAX_VALUE - 3)));
    assertU(adoc("id", "13", "test_l_dv", String.valueOf(Long.MAX_VALUE - 2)));
    assertU(adoc("id", "14", "test_l_dv", String.valueOf(Long.MAX_VALUE - 1)));
    assertU(commit());

    assertIntervalQuery("test_l_dv", "[0," + Integer.MAX_VALUE + "]", "10");
    assertIntervalQuery("test_l_dv", "[" + Integer.MAX_VALUE + "," + Long.MAX_VALUE + "]", "3");
    assertIntervalQuery("test_l_dv", "[" + Integer.MAX_VALUE + ",*]", "3");
  }

  @Test
  public void testFloatFields() {
    doTestFloat("test_f_dv");
  }

  private void doTestFloat(String field) {
    assertU(adoc("id", "1", field, "0"));
    assertU(adoc("id", "2", field, "1"));
    assertU(adoc("id", "3", field, "2"));
    assertU(adoc("id", "4", field, "3"));
    assertU(adoc("id", "5", field, "4"));
    assertU(adoc("id", "6", field, "5"));
    assertU(adoc("id", "7", field, "6"));
    assertU(adoc("id", "8", field, "7"));
    assertU(adoc("id", "9", field, "8"));
    assertU(adoc("id", "10"));
    assertU(adoc("id", "11", field, "10"));
    assertU(commit());

    assertIntervalQueriesNumeric(field);

    assertU(adoc("id", "12", field, "1.3"));
    assertU(adoc("id", "13", field, "4.5"));
    assertU(adoc("id", "14", field, "6.7"));
    assertU(adoc("id", "15", field, "123.45"));
    assertU(commit());

    assertIntervalQuery(field, "[0," + Integer.MAX_VALUE + "]", "14");
    assertIntervalQuery(field, "[0,1]", "2");
    assertIntervalQuery(field, "[0,2]", "4");
    assertIntervalQuery(field, "(1,2)", "1");
    assertIntervalQuery(field, "(1,1)", "0");
    assertIntervalQuery(field, "(4,7)", "4");
    assertIntervalQuery(field, "(123,*)", "1");
  }

  @Test
  public void testDoubleFields() {
    doTestFloat("test_d_dv");
  }

  @Test
  public void testIntFields() {
    assertU(adoc("id", "1", "test_i_dv", "0"));
    assertU(adoc("id", "2", "test_i_dv", "1"));
    assertU(adoc("id", "3", "test_i_dv", "2"));
    assertU(adoc("id", "4", "test_i_dv", "3"));
    assertU(adoc("id", "5", "test_i_dv", "4"));
    assertU(adoc("id", "6", "test_i_dv", "5"));
    assertU(adoc("id", "7", "test_i_dv", "6"));
    assertU(adoc("id", "8", "test_i_dv", "7"));
    assertU(adoc("id", "9", "test_i_dv", "8"));
    assertU(adoc("id", "10"));
    assertU(adoc("id", "11", "test_i_dv", "10"));
    assertU(commit());

    assertIntervalQueriesNumeric("test_i_dv");
  }

  @Test
  public void testIntFieldsMultipleSegments() {
    assertU(adoc("id", "1", "test_i_dv", "0"));
    assertU(adoc("id", "2", "test_i_dv", "1"));
    assertU(adoc("id", "3", "test_i_dv", "2"));
    assertU(commit());
    assertU(adoc("id", "4", "test_i_dv", "3"));
    assertU(adoc("id", "5", "test_i_dv", "4"));
    assertU(adoc("id", "6", "test_i_dv", "5"));
    assertU(adoc("id", "7", "test_i_dv", "6"));
    assertU(commit());
    assertU(adoc("id", "8", "test_i_dv", "7"));
    assertU(adoc("id", "9", "test_i_dv", "8"));
    assertU(adoc("id", "10"));
    assertU(adoc("id", "11", "test_i_dv", "10"));
    assertU(commit());
    
    int i = 12;
    while (getNumberOfReaders() < 2 && i < 10) {
      //try to get more than one segment
      assertU(adoc("id", String.valueOf(i), "test_s_dv", String.valueOf(i)));
      assertU(commit());
    }
    if (getNumberOfReaders() < 2) {
      // It is OK if for some seeds we fall into this case (for example, TieredMergePolicy with
      // segmentsPerTier=2). Most of the case we shouldn't and the test should proceed.
      log.warn("Could not generate more than 1 segment for this seed. Will skip the test");
      return;
    }

    assertIntervalQueriesNumeric("test_i_dv");
  }

  @Test
  public void testIntMultivaluedFields() {
    assertU(adoc("id", "1", "test_is_dv", "0"));
    assertU(adoc("id", "2", "test_is_dv", "1"));
    assertU(adoc("id", "3", "test_is_dv", "2"));
    assertU(adoc("id", "4", "test_is_dv", "3"));
    assertU(adoc("id", "5", "test_is_dv", "4"));
    assertU(adoc("id", "6", "test_is_dv", "5"));
    assertU(adoc("id", "7", "test_is_dv", "6"));
    assertU(adoc("id", "8", "test_is_dv", "7"));
    assertU(adoc("id", "9", "test_is_dv", "8"));
    assertU(adoc("id", "10"));
    assertU(adoc("id", "11", "test_is_dv", "10"));
    assertU(commit());

    assertIntervalQueriesNumeric("test_is_dv");

    assertU(adoc("id", "1", "test_is_dv", "0", "test_is_dv", "1", "test_is_dv", "2", "test_is_dv", "3"));
    assertU(adoc("id", "2", "test_is_dv", "1", "test_is_dv", "2", "test_is_dv", "3", "test_is_dv", "4"));
    assertU(adoc("id", "3", "test_is_dv", "2", "test_is_dv", "3", "test_is_dv", "4", "test_is_dv", "5"));
    assertU(adoc("id", "4", "test_is_dv", "3", "test_is_dv", "4", "test_is_dv", "5", "test_is_dv", "6"));
    assertU(commit());

    assertIntervalQuery("test_is_dv", "[1,3]", "4");
    assertIntervalQuery("test_is_dv", "[3,3]", "4");
    assertIntervalQuery("test_is_dv", "[5,9]", "6");
    assertIntervalQuery("test_is_dv", "(5,9)", "4");
    assertIntervalQuery("test_is_dv", "[*,*]", "10");
  }

  @Test
  public void testDateFields() {
    assertU(adoc("id", "1", "test_dt_dv", "2013-01-01T00:00:00Z"));
    assertU(adoc("id", "2", "test_dt_dv", "2013-01-02T00:00:00Z"));
    assertU(adoc("id", "3", "test_dt_dv", "2013-02-01T00:00:00Z"));
    assertU(adoc("id", "4", "test_dt_dv", "2014-01-01T00:00:00Z"));
    assertU(adoc("id", "5", "test_dt_dv", "2100-01-01T00:00:00Z"));
    assertU(adoc("id", "6", "test_dt_dv", "2013-01-01T10:00:00Z"));
    assertU(adoc("id", "7", "test_dt_dv", "2013-01-01T00:10:00Z"));
    assertU(adoc("id", "8", "test_dt_dv", "2013-01-01T00:00:10Z"));
    assertU(adoc("id", "9"));
    assertU(commit());

    assertIntervalQuery("test_dt_dv", "[*,*]", "8");
    assertIntervalQuery("test_dt_dv", "[*,2014-01-01T00:00:00Z]", "7");
    assertIntervalQuery("test_dt_dv", "[*,2014-01-01T00:00:00Z)", "6");
    assertIntervalQuery("test_dt_dv", "[*,2014-01-01T00:00:00.001Z)", "7");
    assertIntervalQuery("test_dt_dv", "[*,2013-12-31T23:59:59.999Z]", "6");
    assertIntervalQuery("test_dt_dv", "[2013-12-31T23:59:59.9999Z,2014-01-01T00:00:00.001Z]", "1");

    assertIntervalQuery("test_dt_dv", "[NOW,*]", "1");
    assertIntervalQuery("test_dt_dv", "[*,NOW]", "7");

    assertU(adoc("id", "5", "test_dt_dv", "NOW"));
    assertU(commit());
    assertIntervalQuery("test_dt_dv", "[NOW/DAY-1DAY,NOW+2DAY]", "1");

  }

  @Test
  public void testWithDeletedDocs() {
    assertU(adoc("id", "1", "test_s_dv", "dog"));
    assertU(adoc("id", "2", "test_s_dv", "cat"));
    assertU(adoc("id", "3", "test_s_dv", "bird"));
    assertU(adoc("id", "16", "test_s_dv", "cat"));
    assertU(adoc("id", "4", "test_s_dv", "turtle"));
    assertU(adoc("id", "5", "test_s_dv", "\\goodbye,"));
    assertU(adoc("id", "6", "test_s_dv", ",hello\\"));
    assertU(adoc("id", "7", "test_s_dv", "dog"));
    assertU(adoc("id", "15", "test_s_dv", "dog"));
    assertU(adoc("id", "8", "test_s_dv", "dog"));
    assertU(adoc("id", "9", "test_s_dv", "cat"));
    assertU(adoc("id", "10"));
    assertU(commit());

    assertU(adoc("id", "11", "test_s_dv", "the"));
    assertU(adoc("id", "12", "test_s_dv", "quick brown"));
    assertU(adoc("id", "13", "test_s_dv", "fox"));
    assertU(adoc("id", "14", "test_s_dv", "jumped over the lazy dog"));
    assertU(commit());

    assertU(delI("16"));
    assertU(delI("15"));
    assertU(delQ("id:[11 TO 14]"));
    assertU(commit());

    assertIntervalQueriesString("test_s_dv");
  }
  
  @Test
  public void testChangeFieldKey() {
    assertU(adoc("id", "1", "test_s_dv", "dog", "test_l_dv", "1"));
    assertU(adoc("id", "2", "test_s_dv", "cat", "test_l_dv", "2"));
    assertU(commit());

    assertQ(req("q", "*:*", "facet", "true", "facet.interval", "{!key=foo}test_s_dv",
            "facet.interval", "{!key=bar}test_l_dv", "f.test_s_dv.facet.interval.set", "[cat,dog]",
            "f.test_l_dv.facet.interval.set", "[0,1]",
            "f.test_l_dv.facet.interval.set", "[2,*]"),
        "//lst[@name='facet_intervals']/lst[@name='foo']/int[@name='[cat,dog]'][.=2]",
        "//lst[@name='facet_intervals']/lst[@name='bar']/int[@name='[0,1]'][.=1]",
        "//lst[@name='facet_intervals']/lst[@name='bar']/int[@name='[2,*]'][.=1]");
  }
  
  
  @Test
  public void testFilterExclusion() {
    assertU(adoc("id", "1", "test_s_dv", "dog"));
    assertU(adoc("id", "2", "test_s_dv", "cat"));
    assertU(adoc("id", "3", "test_s_dv", "bird"));
    assertU(adoc("id", "4", "test_s_dv", "cat"));
    assertU(adoc("id", "5", "test_s_dv", "turtle"));
    assertU(adoc("id", "6", "test_s_dv", "dog"));
    assertU(adoc("id", "7", "test_s_dv", "dog"));
    assertU(adoc("id", "8", "test_s_dv", "dog"));
    assertU(adoc("id", "9", "test_s_dv", "cat"));
    assertU(adoc("id", "10"));
    assertU(commit());

    assertQ(req("q", "*:*", "facet", "true", "facet.interval", "test_s_dv", "rows", "0",
            "f.test_s_dv.facet.interval.set", "[a,d]",
            "f.test_s_dv.facet.interval.set", "[d,z]"),
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[a,d]'][.=4]",
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[d,z]'][.=5]");
    
    assertQ(req("q", "*:*", "facet", "true", "facet.interval", "test_s_dv", "rows", "0",
            "f.test_s_dv.facet.interval.set", "[a,d]",
            "f.test_s_dv.facet.interval.set", "[d,z]",
            "fq", "test_s_dv:dog"),
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[a,d]'][.=0]",
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[d,z]'][.=4]");
    
    assertQ(req("q", "*:*", "facet", "true", "facet.interval", "{!ex=dogs}test_s_dv", "rows", "0",
            "f.test_s_dv.facet.interval.set", "[a,d]",
            "f.test_s_dv.facet.interval.set", "[d,z]",
            "fq", "{!tag='dogs'}test_s_dv:dog"),
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[a,d]'][.=4]",
        "//lst[@name='facet_intervals']/lst[@name='test_s_dv']/int[@name='[d,z]'][.=5]");
  }
  
  @Test
  public void testSolrJ() throws Exception  {
    assertU(adoc("id", "1", "test_i_dv", "0"));
    assertU(adoc("id", "2", "test_i_dv", "1"));
    assertU(adoc("id", "3", "test_i_dv", "2"));
    assertU(commit());
    
    // Don't close this client, it would shutdown the CoreContainer
    @SuppressWarnings("resource")
    SolrClient client = new EmbeddedSolrServer(h.getCoreContainer(), h.coreName);
    
    SolrQuery q = new SolrQuery();
    q.setQuery("*:*");
    q.addIntervalFacets("test_i_dv", new String[]{"[0,1]","[2,*]"});
    QueryResponse response = client.query(q);
    assertEquals(1, response.getIntervalFacets().size());
    assertEquals("test_i_dv", response.getIntervalFacets().get(0).getField());
    assertEquals(2, response.getIntervalFacets().get(0).getIntervals().size());
    assertEquals("[0,1]", response.getIntervalFacets().get(0).getIntervals().get(0).getKey());
    assertEquals("[2,*]", response.getIntervalFacets().get(0).getIntervals().get(1).getKey());
    
    q = new SolrQuery();
    q.setQuery("*:*");
    q.setFacet(true);
    q.add("facet.interval", "{!key=foo}test_i_dv");
    q.add("f.test_i_dv.facet.interval.set", "{!key=first}[0,1]");
    q.add("f.test_i_dv.facet.interval.set", "{!key=second}[2,*]");
    response = client.query(q);
    assertEquals(1, response.getIntervalFacets().size());
    assertEquals("foo", response.getIntervalFacets().get(0).getField());
    assertEquals(2, response.getIntervalFacets().get(0).getIntervals().size());
    assertEquals("first", response.getIntervalFacets().get(0).getIntervals().get(0).getKey());
    assertEquals("second", response.getIntervalFacets().get(0).getIntervals().get(1).getKey());
    
  }
  
  

  private void assertIntervalQueriesNumeric(String field) {
    assertIntervalQuery(field, "[0,1]", "2");
    assertIntervalQuery(field, "(0,2)", "1");
    assertIntervalQuery(field, "[0,2)", "2");
    assertIntervalQuery(field, "(0,2]", "2");
    assertIntervalQuery(field, "[*,5]", "6");
    assertIntervalQuery(field, "[*,3)", "3", "[2,5)", "3", "[6,8)", "2", "[3,*]", "7", "[10,10]", "1", "[10,10]", "1", "[10,10]", "1");
    assertIntervalQuery(field, "(5,*]", "4", "[5,5]", "1", "(*,5)", "5");
    assertIntervalQuery(field, "[5,5]", "1", "(*,5)", "5", "(5,*]", "4");
    assertIntervalQuery(field, "(5,*]", "4", "(*,5)", "5", "[5,5]", "1");

  }

  private void assertIntervalQueriesString(String field) {
    assertIntervalQuery(field, "[bird,bird]", "1");
    assertIntervalQuery(field, "(bird,dog)", "2");
    assertIntervalQuery(field, "[bird,dog)", "3");
    assertIntervalQuery(field, "(bird,turtle]", "6");
    assertIntervalQuery(field, "[*,bird]", "3");
    assertIntervalQuery(field, "[*,bird)", "2", "[bird,cat)", "1", "[cat,dog)", "2", "[dog,*]", "4");
    assertIntervalQuery(field, "[*,*]", "9", "[*,dog)", "5", "[*,dog]", "8", "[dog,*]", "4");
    assertIntervalQuery(field, field + ":dog", 3, "[*,*]", "3", "[*,dog)", "0", "[*,dog]", "3", "[dog,*]", "3", "[bird,cat]", "0");
    assertIntervalQuery(field, "(*,dog)", "5", "[dog, dog]", "3", "(dog,*)", "1");
    assertIntervalQuery(field, "[dog, dog]", "3", "(dog,*)", "1", "(*,dog)", "5");
    assertIntervalQuery(field, "(dog,*)", "1", "(*,dog)", "5", "[dog, dog]", "3");
  }

  /**
   * Will run a match all query, and ask for interval facets in the specified field.
   * The intervals facet are indicated in the <code>intervals</code> parameter, followed
   * by the expected count result. For example:
   * <code>assertIntervalQuery("my_string_field", "[0,10]", "3", "(20,*), "12");</code>
   *
   * @param field     The field in which the interval facet should be asked
   * @param intervals a variable array of intervals followed by the expected count (also a string)
   */
  private void assertIntervalQuery(String field, String... intervals) {
    assertIntervalQuery(field, "*:*", -1, intervals);
  }

  private void assertIntervalQuery(String field, String query, int resultCount, String... intervals) {
    assert (intervals.length & 1) == 0;
    int idx = 0;
    String[] params = new String[intervals.length + 6];
    params[idx++] = "q";
    params[idx++] = query;
    params[idx++] = "facet";
    params[idx++] = "true";
    params[idx++] = "facet.interval";
    params[idx++] = field;

    for (int i = 0; i < intervals.length; i += 2) {
      params[idx++] = "f." + field + ".facet.interval.set";
      params[idx++] = intervals[i];
    }

    String[] tests = new String[intervals.length / 2 + (resultCount > 0 ? 1 : 0)];
    idx = 0;
    for (int i = 0; i < intervals.length; i += 2) {
      tests[idx++] = "//lst[@name='facet_intervals']/lst[@name='" + field + "']/int[@name='" + intervals[i] + "'][.=" + intervals[i + 1] + "]";
    }
    if (resultCount >= 0) {
      tests[idx++] = "//*[@numFound='" + resultCount + "']";
    }

    assertQ("Unexpected facet iterval count. Field:" + field + ", Intervals: " + Arrays.toString(intervals) + "Query: " + query,
        req(params), tests);
  }
}
