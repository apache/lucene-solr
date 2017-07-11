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

import org.apache.lucene.util.TestUtil;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

public class TestRangeQuery extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema11.xml");
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
      proc.process(doc);
      assertU(adoc(doc));
    }
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
    // interested in fields that support *equivilent* ranges -- strings
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
    createIndex(numDocs, new DocProcessor() {
      @Override
      public void process(SolrInputDocument doc) {
        // 10% of the docs have missing values
        if (random().nextInt(10)!=0) addInt(doc, l,u, fields);
      }
    });
    assertU(commit());

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
