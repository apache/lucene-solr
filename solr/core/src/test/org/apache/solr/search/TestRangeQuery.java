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
package org.apache.solr.search;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.StrField;
import org.apache.solr.util.TestInjection;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRangeQuery extends SolrTestCaseJ4 {
  
  private final static long DATE_START_TIME_RANDOM_TEST = 1499797224224L;
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT);

  @BeforeClass
  public static void beforeClass() throws Exception {
    // use a solrconfig that does not have autowarming
    initCore("solrconfig_perf.xml", "schema11.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @After
  @Override
  public void tearDown() throws Exception {
    TestInjection.reset();
    super.tearDown();
  }

  void addInt(SolrInputDocument doc, int l, int u, String... fields) {
    int v=0;
    if (0==l && l==u) {
      v=random().nextInt();
    } else {
      v=random().nextInt(u-l)+l;
    }
    for (String field : fields) {
      doc.addField(field, v);
    }
  }

  interface DocProcessor {
    public void process(SolrInputDocument doc);
  }

  public void createIndex(int nDocs, DocProcessor proc) {
    for (int i=0; i<nDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", ""+i);
      if (proc != null) {
        proc.process(doc);
      }
      assertU(adoc(doc));
    }
    assertU(commit());
  }

  @Test
  public void testRangeQueries() throws Exception {
    // ensure that we aren't losing precision on any fields in addition to testing other non-numeric fields
    // that aren't tested in testRandomRangeQueries()

    int i=2000000000;
    long l=500000000000000000L;
    double d=0.3333333333333333;

    // first 3 values will be indexed, the last two won't be
    String[] ints = {""+(i-1), ""+(i), ""+(i+1),   ""+(i-2), ""+(i+2)};
    String[] longs = {""+(l-1), ""+(l), ""+(l+1),  ""+(l-2), ""+(l+2)};
    String[] doubles = {""+(d-1e-16), ""+(d), ""+(d+1e-16),   ""+(d-2e-16), ""+(d+2e-16)};
    String[] strings = {"aaa","bbb","ccc",  "aa","cccc" };
    String[] dates = {"0299-12-31T23:59:59.999Z","2000-01-01T00:00:00.000Z","2000-01-01T00:00:00.001Z",  "0299-12-31T23:59:59.998Z","2000-01-01T00:00:00.002Z" };

    // fields that normal range queries should work on
    Map<String,String[]> norm_fields = new HashMap<>();
    norm_fields.put("foo_i", ints);
    norm_fields.put("foo_l", longs);
    norm_fields.put("foo_d", doubles);

    norm_fields.put("foo_ti", ints);
    norm_fields.put("foo_tl", longs);
    norm_fields.put("foo_td", doubles);
    norm_fields.put("foo_tdt", dates);

    norm_fields.put("foo_s", strings);
    norm_fields.put("foo_dt", dates);


    // fields that frange queries should work on
    Map<String,String[]> frange_fields = new HashMap<>();
    frange_fields.put("foo_i", ints);
    frange_fields.put("foo_l", longs);
    frange_fields.put("foo_d", doubles);

    frange_fields.put("foo_ti", ints);
    frange_fields.put("foo_tl", longs);
    frange_fields.put("foo_td", doubles);
    frange_fields.put("foo_tdt", dates);

    frange_fields.put("foo_s", strings);
    frange_fields.put("foo_dt", dates);

    Map<String,String[]> all_fields = new HashMap<>();
    all_fields.putAll(norm_fields);
    all_fields.putAll(frange_fields);

    for (int j=0; j<ints.length-2; j++) {
      List<String> fields = new ArrayList<>();
      fields.add("id");
      fields.add(""+j);
      for (Map.Entry<String,String[]> entry : all_fields.entrySet()) {
        fields.add(entry.getKey());
        fields.add(entry.getValue()[j]);
      }
      assertU(adoc(fields.toArray(new String[fields.size()])));
    }

    assertU(commit());

    // simple test of a function rather than just the field
    assertQ(req("{!frange l=0 u=2}id"), "*[count(//doc)=3]");
    assertQ(req("{!frange l=0 u=2}product(id_i,2)"), "*[count(//doc)=2]");
    assertQ(req("{!frange l=100 u=102}sum(id_i,100)"), "*[count(//doc)=3]");


    for (Map.Entry<String,String[]> entry : norm_fields.entrySet()) {
      String f = entry.getKey();
      String[] v = entry.getValue();

      assertQ(req(f + ":[* TO *]" ), "*[count(//doc)=3]");
      assertQ(req(f + ":["+v[0]+" TO "+v[2]+"]"), "*[count(//doc)=3]");
      assertQ(req(f + ":["+v[1]+" TO "+v[2]+"]"), "*[count(//doc)=2]");
      assertQ(req(f + ":["+v[0]+" TO "+v[1]+"]"), "*[count(//doc)=2]");
      assertQ(req(f + ":["+v[0]+" TO "+v[0]+"]"), "*[count(//doc)=1]");
      assertQ(req(f + ":["+v[1]+" TO "+v[1]+"]"), "*[count(//doc)=1]");
      assertQ(req(f + ":["+v[2]+" TO "+v[2]+"]"), "*[count(//doc)=1]");
      assertQ(req(f + ":["+v[3]+" TO "+v[3]+"]"), "*[count(//doc)=0]");
      assertQ(req(f + ":["+v[4]+" TO "+v[4]+"]"), "*[count(//doc)=0]");

      assertQ(req(f + ":{"+v[0]+" TO "+v[2]+"}"), "*[count(//doc)=1]");
      assertQ(req(f + ":{"+v[1]+" TO "+v[2]+"}"), "*[count(//doc)=0]");
      assertQ(req(f + ":{"+v[0]+" TO "+v[1]+"}"), "*[count(//doc)=0]");
      assertQ(req(f + ":{"+v[3]+" TO "+v[4]+"}"), "*[count(//doc)=3]");
    }

    for (Map.Entry<String,String[]> entry : frange_fields.entrySet()) {
      String f = entry.getKey();
      String[] v = entry.getValue();

      assertQ(req("{!frange}"+f ), "*[count(//doc)=3]");
      assertQ(req("{!frange" + " l="+v[0]+"}"+f ), "*[count(//doc)=3]");
      assertQ(req("{!frange" + " l="+v[1]+"}"+f ), "*[count(//doc)=2]");
      assertQ(req("{!frange" + " l="+v[2]+"}"+f ), "*[count(//doc)=1]");
      assertQ(req("{!frange" + " l="+v[3]+"}"+f ), "*[count(//doc)=3]");
      assertQ(req("{!frange" + " l="+v[4]+"}"+f ), "*[count(//doc)=0]");

      assertQ(req("{!frange" + " u="+v[0]+"}"+f ), "*[count(//doc)=1]");
      assertQ(req("{!frange" + " u="+v[1]+"}"+f ), "*[count(//doc)=2]");
      assertQ(req("{!frange" + " u="+v[2]+"}"+f ), "*[count(//doc)=3]");
      assertQ(req("{!frange" + " u="+v[3]+"}"+f ), "*[count(//doc)=0]");
      assertQ(req("{!frange" + " u="+v[4]+"}"+f ), "*[count(//doc)=3]");

      assertQ(req("{!frange incl=false" + " l="+v[0]+"}"+f ), "*[count(//doc)=2]");
      assertQ(req("{!frange incl=false" + " l="+v[1]+"}"+f ), "*[count(//doc)=1]");
      assertQ(req("{!frange incl=false" + " l="+v[2]+"}"+f ), "*[count(//doc)=0]");
      assertQ(req("{!frange incl=false" + " l="+v[3]+"}"+f ), "*[count(//doc)=3]");
      assertQ(req("{!frange incl=false" + " l="+v[4]+"}"+f ), "*[count(//doc)=0]");

      assertQ(req("{!frange incu=false" + " u="+v[0]+"}"+f ), "*[count(//doc)=0]");
      assertQ(req("{!frange incu=false" + " u="+v[1]+"}"+f ), "*[count(//doc)=1]");
      assertQ(req("{!frange incu=false" + " u="+v[2]+"}"+f ), "*[count(//doc)=2]");
      assertQ(req("{!frange incu=false" + " u="+v[3]+"}"+f ), "*[count(//doc)=0]");
      assertQ(req("{!frange incu=false" + " u="+v[4]+"}"+f ), "*[count(//doc)=3]");

      assertQ(req("{!frange incl=true incu=true" + " l=" +v[0] +" u="+v[2]+"}"+f ), "*[count(//doc)=3]");
      assertQ(req("{!frange incl=false incu=false" + " l=" +v[0] +" u="+v[2]+"}"+f ), "*[count(//doc)=1]");
      assertQ(req("{!frange incl=false incu=false" + " l=" +v[3] +" u="+v[4]+"}"+f ), "*[count(//doc)=3]");
    }

    // now pick a random range to use to delete (some of) the docs...
    
    final boolean incl = random().nextBoolean();
    final boolean incu = random().nextBoolean();
    final int expected = 0 + (incl ? 0 : 1) + (incu ? 0 : 1);
    String dbq = null;
    if (random().nextBoolean()) { // regular range
      String field = randomKey(norm_fields);
      String[] values = norm_fields.get(field);
      dbq = field + ":" + (incl ? "[" : "{") + values[0] + " TO " + values[2] + (incu ? "]" : "}");
    } else { // frange
      String field = randomKey(frange_fields);
      String[] values = frange_fields.get(field);
      dbq = "{!frange incl=" + incl + " incu=" + incu + " l=" + values[0] + " u=" + values[2] + "}" + field;
    }
    if (random().nextBoolean()) {
      // wrap in a BQ
      String field = randomKey(norm_fields);
      String value = norm_fields.get(field)[1];
      // wraping shouldn't affect expected
      dbq = "("+field+":\""+value+"\" OR " + dbq + ")";
    }    
      
    assertU(delQ(dbq));
    assertU(commit());
    assertQ(req("q","*:*","_trace_dbq",dbq),
            "*[count(//doc)=" + expected + "]");
    
  }

  @Test
  public void testRandomRangeQueries() throws Exception {
    String handler="";
    final String[] fields = {"foo_s","foo_i","foo_l","foo_f","foo_d",
                             "foo_ti","foo_tl","foo_tf","foo_td" };
    
    // NOTE: foo_s supports ranges, but for the arrays below we are only
    // interested in fields that support *equivalent* ranges -- strings
    // are not ordered the same as ints/longs, so we can't test the ranges
    // for equivilence across diff fields.
    //
    // fields that a normal range query will work correctly on
    String[] norm_fields = {"foo_i","foo_l","foo_f","foo_d",
                            "foo_ti","foo_tl","foo_tf","foo_td" };
    // fields that a value source range query should work on
    String[] frange_fields = {"foo_i","foo_l","foo_f","foo_d"};

    final int l= -1 * atLeast(50);
    final int u= atLeast(250);

    // sometimes a very small index, sometimes a very large index
    final int numDocs = random().nextBoolean() ? random().nextInt(50) : atLeast(1000);
    createIndex(numDocs, doc -> {
      // 10% of the docs have missing values
      if (random().nextInt(10)!=0) addInt(doc, l,u, fields);
    });

    final int numIters = atLeast(1000);
    for (int i=0; i < numIters; i++) {
      int lower = TestUtil.nextInt(random(), 2 * l, u);
      int upper = TestUtil.nextInt(random(), lower, 2 * u);
      boolean lowerMissing = random().nextInt(10)==1;
      boolean upperMissing = random().nextInt(10)==1;
      boolean inclusive = lowerMissing || upperMissing || random().nextBoolean();

      // lower=2; upper=2; inclusive=true;      
      // inclusive=true; lowerMissing=true; upperMissing=true;    

      List<String> qs = new ArrayList<>();
      for (String field : norm_fields) {
        String q = field + ':' + (inclusive?'[':'{')
                + (lowerMissing?"*":lower)
                + " TO "
                + (upperMissing?"*":upper)
                + (inclusive?']':'}');
        qs.add(q);
      }
      for (String field : frange_fields) {
        String q = "{!frange v="+field
                + (lowerMissing?"":(" l="+lower))
                + (upperMissing?"":(" u="+upper))
                + (inclusive?"":" incl=false")
                + (inclusive?"":" incu=false")
                + "}";
        qs.add(q);
      }
      String lastQ = null;
      SolrQueryResponse last=null;
      for (String q : qs) {
        // System.out.println("QUERY="+q);
        SolrQueryRequest req = req("q",q,"rows",""+numDocs);
        SolrQueryResponse qr = h.queryAndResponse(handler, req);
        if (last != null) {
          // we only test if the same docs matched since some queries will include factors like idf, etc.
          DocList rA = ((ResultContext)qr.getResponse()).getDocList();
          DocList rB = ((ResultContext)last.getResponse()).getDocList();
          sameDocs(q + " vs " + lastQ, rA, rB );
        }
        req.close();
        last = qr;
        lastQ = q;
      }
    }

    // now build some random queries (against *any* field) and validate that using it in a DBQ changes
    // the index by the expected number of docs
    long numDocsLeftInIndex = numDocs;
    final int numDBQs= atLeast(10);
    for (int i=0; i < numDBQs; i++) {
      int lower = TestUtil.nextInt(random(), 2 * l, u);
      int upper = TestUtil.nextInt(random(), lower, 2 * u);
      boolean lowerMissing = random().nextInt(10)==1;
      boolean upperMissing = random().nextInt(10)==1;
      boolean inclusive = lowerMissing || upperMissing || random().nextBoolean();
      
      String dbq = null;
      if (random().nextBoolean()) { // regular range
        String field = fields[random().nextInt(fields.length)];
        dbq = field + ':' + (inclusive?'[':'{')
          + (lowerMissing?"*":lower)
          + " TO "
          + (upperMissing?"*":upper)
          + (inclusive?']':'}');
       } else { // frange
        String field = frange_fields[random().nextInt(frange_fields.length)];
        dbq = "{!frange v="+field
          + (lowerMissing?"":(" l="+lower))
          + (upperMissing?"":(" u="+upper))
          + (inclusive?"":" incl=false")
          + (inclusive?"":" incu=false")
          + "}";
      }
      try (SolrQueryRequest req = req("q",dbq,"rows","0")) {
        SolrQueryResponse qr = h.queryAndResponse(handler, req);
        numDocsLeftInIndex -= ((ResultContext)qr.getResponse()).getDocList().matches();
      }
      assertU(delQ(dbq));
      assertU(commit());
      try (SolrQueryRequest req = req("q","*:*","rows","0","_trace_after_dbq",dbq)) {
        SolrQueryResponse qr = h.queryAndResponse(handler, req);
        final long allDocsFound = ((ResultContext)qr.getResponse()).getDocList().matches();
        assertEquals(dbq, numDocsLeftInIndex, allDocsFound);
      }
    }
  }

  @Test
  public void testRangeQueryWithFilterCache() throws Exception {
    // sometimes a very small index, sometimes a very large index
    // final int numDocs = random().nextBoolean() ? random().nextInt(50) : atLeast(1000);
    final int numDocs = 99;
    createIndex(numDocs, doc -> {
      addInt(doc, 0, 0, "foo_i");
    });

    // ensure delay comes after createIndex - so we don't affect/count any cache warming from queries left over by other test methods
    TestInjection.delayBeforeCreatingNewDocSet = TEST_NIGHTLY ? 50 : 500; // Run more queries nightly, so use shorter delay

    final int MAX_QUERY_RANGE = 222;                            // Arbitrary number in the middle of the value range
    final int QUERY_START = TEST_NIGHTLY ? 1 : MAX_QUERY_RANGE; // Either run queries for the full range, or just the last one
    final int NUM_QUERIES = TEST_NIGHTLY ? 101 : 10;
    for (int j = QUERY_START ; j <= MAX_QUERY_RANGE; j++) {
      ExecutorService queryService = ExecutorUtil.newMDCAwareFixedThreadPool(4, new SolrNamedThreadFactory("TestRangeQuery-" + j));
      try (SolrCore core = h.getCoreInc()) {
        SolrRequestHandler defaultHandler = core.getRequestHandler("");

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("q", "*:*");
        params.add("fq", "id:[0 TO " + j + "]"); // These should all come from FilterCache

        // Regular: 10 threads with 4 executors would be enough for 3 waves, or approximately 1500ms of delay
        // Nightly: 101 threads with 4 executors is 26 waves, approximately 1300ms delay
        CountDownLatch atLeastOnceCompleted = new CountDownLatch(TEST_NIGHTLY ? 30 : 1);
        for (int i = 0; i < NUM_QUERIES; i++) {
          queryService.submit(() -> {
            try (SolrQueryRequest req = req(params)) {
              core.execute(defaultHandler, req, new SolrQueryResponse());
            }
            atLeastOnceCompleted.countDown();
          });
        }

        queryService.shutdown(); // No more requests will be queued up
        atLeastOnceCompleted.await(); // Wait for the first batch of queries to complete
        assertTrue(queryService.awaitTermination(1, TimeUnit.SECONDS)); // All queries after should be very fast

        assertEquals("Create only one DocSet outside of cache", 1, TestInjection.countDocSetDelays.get());
      }
      TestInjection.countDocSetDelays.set(0);
    }
  }

  @Test
  public void testRangeQueryEndpointTO() throws Exception {
    assertEquals("[to TO to]", QParser.getParser("[to TO to]", req("df", "text")).getQuery().toString("text"));
    assertEquals("[to TO to]", QParser.getParser("[to TO TO]", req("df", "text")).getQuery().toString("text"));
    assertEquals("[to TO to]", QParser.getParser("[TO TO to]", req("df", "text")).getQuery().toString("text"));
    assertEquals("[to TO to]", QParser.getParser("[TO TO TO]", req("df", "text")).getQuery().toString("text"));

    assertEquals("[to TO to]", QParser.getParser("[\"TO\" TO \"TO\"]", req("df", "text")).getQuery().toString("text"));
    assertEquals("[to TO to]", QParser.getParser("[\"TO\" TO TO]", req("df", "text")).getQuery().toString("text"));
    assertEquals("[to TO to]", QParser.getParser("[TO TO \"TO\"]", req("df", "text")).getQuery().toString("text"));

    assertEquals("[to TO xx]", QParser.getParser("[to TO xx]", req("df", "text")).getQuery().toString("text"));
    assertEquals("[to TO xx]", QParser.getParser("[\"TO\" TO xx]", req("df", "text")).getQuery().toString("text"));
    assertEquals("[to TO xx]", QParser.getParser("[TO TO xx]", req("df", "text")).getQuery().toString("text"));

    assertEquals("[xx TO to]", QParser.getParser("[xx TO to]", req("df", "text")).getQuery().toString("text"));
    assertEquals("[xx TO to]", QParser.getParser("[xx TO \"TO\"]", req("df", "text")).getQuery().toString("text"));
    assertEquals("[xx TO to]", QParser.getParser("[xx TO TO]", req("df", "text")).getQuery().toString("text"));
  }

  @Test
  public void testRangeQueryRequiresTO() throws Exception {
    assertEquals("{a TO b}", QParser.getParser("{A TO B}", req("df", "text")).getQuery().toString("text"));
    assertEquals("[a TO b}", QParser.getParser("[A TO B}", req("df", "text")).getQuery().toString("text"));
    assertEquals("{a TO b]", QParser.getParser("{A TO B]", req("df", "text")).getQuery().toString("text"));
    assertEquals("[a TO b]", QParser.getParser("[A TO B]", req("df", "text")).getQuery().toString("text"));

    // " TO " is required between range endpoints
    expectThrows(SyntaxError.class, () -> QParser.getParser("{A B}", req("df", "text")).getQuery());
    expectThrows(SyntaxError.class, () -> QParser.getParser("[A B}", req("df", "text")).getQuery());
    expectThrows(SyntaxError.class, () -> QParser.getParser("{A B]", req("df", "text")).getQuery());
    expectThrows(SyntaxError.class, () -> QParser.getParser("[A B]", req("df", "text")).getQuery());

    expectThrows(SyntaxError.class, () -> QParser.getParser("{TO B}", req("df", "text")).getQuery());
    expectThrows(SyntaxError.class, () -> QParser.getParser("[TO B}", req("df", "text")).getQuery());
    expectThrows(SyntaxError.class, () -> QParser.getParser("{TO B]", req("df", "text")).getQuery());
    expectThrows(SyntaxError.class, () -> QParser.getParser("[TO B]", req("df", "text")).getQuery());

    expectThrows(SyntaxError.class, () -> QParser.getParser("{A TO}", req("df", "text")).getQuery());
    expectThrows(SyntaxError.class, () -> QParser.getParser("[A TO}", req("df", "text")).getQuery());
    expectThrows(SyntaxError.class, () -> QParser.getParser("{A TO]", req("df", "text")).getQuery());
    expectThrows(SyntaxError.class, () -> QParser.getParser("[A TO]", req("df", "text")).getQuery());
  }

  @Test
  public void testCompareTypesRandomRangeQueries() throws Exception {
    int cardinality = 10000;
    Map<NumberType,String[]> types = new HashMap<>(); //single and multivalued field types
    Map<NumberType,String[]> typesMv = new HashMap<>(); // multivalued field types only
    types.put(NumberType.INTEGER, new String[]{"ti", "ti_dv", "ti_ni_dv", "i_p", "i_ni_p", "i_ndv_p", "tis", "tis_dv", "tis_ni_dv", "is_p", "is_ni_p", "is_ndv_p"});
    types.put(NumberType.LONG, new String[]{"tl", "tl_dv", "tl_ni_dv", "l_p", "l_ni_p", "l_ndv_p", "tls", "tls_dv", "tls_ni_dv", "ls_p", "ls_ni_p", "ls_ndv_p"});
    types.put(NumberType.FLOAT, new String[]{"tf", "tf_dv", "tf_ni_dv", "f_p", "f_ni_p", "f_ndv_p", "tfs", "tfs_dv", "tfs_ni_dv", "fs_p", "fs_ni_p", "fs_ndv_p"});
    types.put(NumberType.DOUBLE, new String[]{"td", "td_dv", "td_ni_dv", "d_p", "d_ni_p", "d_ndv_p", "tds", "tds_dv", "tds_ni_dv", "ds_p", "ds_ni_p", "ds_ndv_p"});
    types.put(NumberType.DATE, new String[]{"tdt", "tdt_dv", "tdt_ni_dv", "dt_p", "dt_ni_p", "dt_ndv_p", "tdts", "tdts_dv", "tdts_ni_dv", "dts_p", "dts_ni_p", "dts_ndv_p"});
    typesMv.put(NumberType.INTEGER, new String[]{"tis", "tis_dv", "tis_ni_dv", "is_p", "is_ni_p", "is_ndv_p"});
    typesMv.put(NumberType.LONG, new String[]{"tls", "tls_dv", "tls_ni_dv", "ls_p", "ls_ni_p", "ls_ndv_p"});
    typesMv.put(NumberType.FLOAT, new String[]{"tfs", "tfs_dv", "tfs_ni_dv", "fs_p", "fs_ni_p", "fs_ndv_p"});
    typesMv.put(NumberType.DOUBLE, new String[]{"tds", "tds_dv", "tds_ni_dv", "ds_p", "ds_ni_p", "ds_ndv_p"});
    typesMv.put(NumberType.DATE, new String[]{"tdts", "tdts_dv", "tdts_ni_dv", "dts_p", "dts_ni_p", "dts_ndv_p"});

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
      SolrInputDocument document = new SolrInputDocument();
      document.setField("id", i);
      for (Map.Entry<NumberType,String[]> entry:types.entrySet()) {
        NumberType type = entry.getKey();
        String val = null;
        List<String> vals = null;
        switch (type) {
          case DATE:
            val = randomDate(cardinality);
            vals = getRandomDates(random().nextInt(10), cardinality);
            break;
          case DOUBLE:
            val = String.valueOf(randomDouble(cardinality));
            vals = toStringList(getRandomDoubles(random().nextInt(10), cardinality));
            break;
          case FLOAT:
            val = String.valueOf(randomFloat(cardinality));
            vals = toStringList(getRandomFloats(random().nextInt(10), cardinality));
            break;
          case INTEGER:
            val = String.valueOf(randomInt(cardinality));
            vals = toStringList(getRandomInts(random().nextInt(10), cardinality));
            break;
          case LONG:
            val = String.valueOf(randomLong(cardinality));
            vals = toStringList(getRandomLongs(random().nextInt(10), cardinality));
            break;
          default:
            throw new AssertionError();

        }
        // SingleValue
        for (String fieldSuffix:entry.getValue()) {
          document.setField("field_sv_" + fieldSuffix, val);
        }
        //  MultiValue
        for (String fieldSuffix:typesMv.get(type)) {
          for (String value:vals) {
            document.addField("field_mv_" + fieldSuffix, value);
          }
        }
      }

      assertU(adoc(document));
      if (random().nextInt(50) == 0) {
        assertU(commit());
      }
    }
    assertU(commit());

    String[][] possibleTypes = new String[types.size()][];
    types.values().toArray(possibleTypes);
    String[][] possibleTypesMv = new String[typesMv.size()][];
    typesMv.values().toArray(possibleTypesMv);
    for (int i = 0; i < atLeast(1000); i++) {
      doTestQuery(cardinality, false, pickRandom(possibleTypes));
      doTestQuery(cardinality, true, pickRandom(possibleTypesMv));
    }
  }

  private void doTestQuery(int cardinality, boolean mv, String[] types) throws Exception {
    String[] startOptions = new String[]{"{", "["};
    String[] endOptions = new String[]{"}", "]"};
    String[] qRange = getRandomRange(cardinality, types[0]);
    String start = pickRandom(startOptions);
    String end = pickRandom(endOptions);
    long expectedHits = doRangeQuery(mv, start, end, types[0], qRange);
    for (int i = 1; i < types.length; i++) {
      assertEquals("Unexpected results from query when comparing " + types[0] + " with " + types[i] + " and query: " +
          start + qRange[0] + " TO " + qRange[1] + end + "\n", 
          expectedHits, doRangeQuery(mv, start, end, types[i], qRange));
    }
  }

  private long doRangeQuery(boolean mv, String start, String end, String field, String[] qRange) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "field_" + (mv?"mv_":"sv_") + field + ":" + start + qRange[0] + " TO " + qRange[1] + end);
    try (SolrQueryRequest req = req(params)) {
      return (long) h.queryAndResponse("", req).getToLog().get("hits");
    }
  }

  private String[] getRandomRange(int max, String fieldName) {
    Number[] values = new Number[2];
    FieldType ft = h.getCore().getLatestSchema().getField("field_" + fieldName).getType();
    if (ft.getNumberType() == null) {
      assert ft instanceof StrField;
      values[0] = randomInt(max);
      values[1] = randomInt(max);
      Arrays.sort(values, (o1, o2) -> String.valueOf(o1).compareTo(String.valueOf(o2)));
    } else {
      switch (ft.getNumberType()) {
        case DOUBLE:
          values[0] = randomDouble(max);
          values[1] = randomDouble(max);
          break;
        case FLOAT:
          values[0] = randomFloat(max);
          values[1] = randomFloat(max);
          break;
        case INTEGER:
          values[0] = randomInt(max);
          values[1] = randomInt(max);
          break;
        case LONG:
          values[0] = randomLong(max);
          values[1] = randomLong(max);
          break;
        case DATE:
          values[0] = randomMs(max);
          values[1] = randomMs(max);
          break;
        default:
          throw new AssertionError("Unexpected number type");

      }
      if (random().nextInt(100) >= 1) {// sometimes don't sort the values. Should result in 0 hits
        Arrays.sort(values);
      }
    }
    String[] stringValues = new String[2];
    if (rarely()) {
      stringValues[0] = "*";
    } else {
      if (ft.getNumberType() == NumberType.DATE) {
        stringValues[0] = dateFormat.format(values[0]);
      } else {
        stringValues[0] = String.valueOf(values[0]);
      }
    }
    if (rarely()) {
      stringValues[1] = "*";
    } else {
      if (ft.getNumberType() == NumberType.DATE) {
        stringValues[1] = dateFormat.format(values[1]);
      } else {
        stringValues[1] = String.valueOf(values[1]);
      }
    }
    return stringValues;
  }


  // Helper methods
  private String randomDate(int cardinality) {
    return dateFormat.format(new Date(randomMs(cardinality)));
  }

  private List<String> getRandomDates(int numValues, int cardinality) {
    List<String> vals = new ArrayList<>(numValues);
    for (int i = 0; i < numValues;i++) {
      vals.add(randomDate(cardinality));
    }
    return vals;
  }

  private List<Double> getRandomDoubles(int numValues, int cardinality) {
    List<Double> vals = new ArrayList<>(numValues);
    for (int i = 0; i < numValues;i++) {
      vals.add(randomDouble(cardinality));
    }
    return vals;
  }
  
  private List<Float> getRandomFloats(int numValues, int cardinality) {
    List<Float> vals = new ArrayList<>(numValues);
    for (int i = 0; i < numValues;i++) {
      vals.add(randomFloat(cardinality));
    }
    return vals;
  }
  
  private List<Integer> getRandomInts(int numValues, int cardinality) {
    List<Integer> vals = new ArrayList<>(numValues);
    for (int i = 0; i < numValues;i++) {
      vals.add(randomInt(cardinality));
    }
    return vals;
  }
  
  private List<Long> getRandomLongs(int numValues, int cardinality) {
    List<Long> vals = new ArrayList<>(numValues);
    for (int i = 0; i < numValues;i++) {
      vals.add(randomLong(cardinality));
    }
    return vals;
  }

  <T> List<String> toStringList(List<T> input) {
    List<String> newList = new ArrayList<>(input.size());
    for (T element:input) {
      newList.add(String.valueOf(element));
    }
    return newList;
  }

  long randomMs(int cardinality) {
    return DATE_START_TIME_RANDOM_TEST + random().nextInt(cardinality) * 1000 * (random().nextBoolean()?1:-1);
  }

  double randomDouble(int cardinality) {
    if (rarely()) {
      int num = random().nextInt(8);
      if (num == 0) return Double.NEGATIVE_INFINITY;
      if (num == 1) return Double.POSITIVE_INFINITY;
      if (num == 2) return Double.MIN_VALUE;
      if (num == 3) return Double.MAX_VALUE;
      if (num == 4) return -Double.MIN_VALUE;
      if (num == 5) return -Double.MAX_VALUE;
      if (num == 6) return 0.0d;
      if (num == 7) return -0.0d;
    }
    Double d = Double.NaN;
    while (d.isNaN()) {
      d = random().nextDouble();
    }
    return d * cardinality * (random().nextBoolean()?1:-1);
  }

  float randomFloat(int cardinality) {
    if (rarely()) {
      int num = random().nextInt(8);
      if (num == 0) return Float.NEGATIVE_INFINITY;
      if (num == 1) return Float.POSITIVE_INFINITY;
      if (num == 2) return Float.MIN_VALUE;
      if (num == 3) return Float.MAX_VALUE;
      if (num == 4) return -Float.MIN_VALUE;
      if (num == 5) return -Float.MAX_VALUE;
      if (num == 6) return 0.0f;
      if (num == 7) return -0.0f;
    }
    Float f = Float.NaN;
    while (f.isNaN()) {
      f = random().nextFloat();
    }
    return f * cardinality * (random().nextBoolean()?1:-1);
  }

  int randomInt(int cardinality) {
    if (rarely()) {
      int num = random().nextInt(2);
      if (num == 0) return Integer.MAX_VALUE;
      if (num == 1) return Integer.MIN_VALUE;
    }
    return random().nextInt(cardinality) * (random().nextBoolean()?1:-1);
  }

  long randomLong(int cardinality) {
    if (rarely()) {
      int num = random().nextInt(2);
      if (num == 0) return Long.MAX_VALUE;
      if (num == 1) return Long.MIN_VALUE;
    }
    return randomInt(cardinality);
  }

  static boolean sameDocs(String msg, DocSet a, DocSet b) {
    DocIterator i = a.iterator();
    // System.out.println("SIZES="+a.size() + "," + b.size());
    assertEquals(msg, a.size(), b.size());
    while (i.hasNext()) {
      int doc = i.nextDoc();
      assertTrue(msg, b.exists(doc));
      // System.out.println("MATCH! " + doc);
    }
    return true;
  }

  private static <X extends Comparable<? super X>,Y> X randomKey(Map<X,Y> map) {
    assert ! map.isEmpty();
    List<X> sortedKeys = new ArrayList<>(map.keySet());
    Collections.sort(sortedKeys);
    return sortedKeys.get(random().nextInt(sortedKeys.size()));
  }
}
