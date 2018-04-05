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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class MathExpressionTest extends SolrCloudTestCase {

  private static final String COLLECTIONORALIAS = "collection1";
  private static final int TIMEOUT = DEFAULT_TIMEOUT;
  private static final String id = "id";

  private static boolean useAlias;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .addConfig("ml", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("ml").resolve("conf"))
        .configure();

    String collection;
    useAlias = random().nextBoolean();
    if (useAlias) {
      collection = COLLECTIONORALIAS + "_collection";
    } else {
      collection = COLLECTIONORALIAS;
    }

    CollectionAdminRequest.createCollection(collection, "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(collection, cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
    if (useAlias) {
      CollectionAdminRequest.createAlias(COLLECTIONORALIAS, collection).process(cluster.getSolrClient());
    }
  }

  @Before
  public void cleanIndex() throws Exception {
    new UpdateRequest()
        .deleteByQuery("*:*")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

    @Test
  public void testAnalyzeEvaluator() throws Exception {

    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "1", "test_t", "l b c d c");
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);


    SolrClientCache cache = new SolrClientCache();
    try {

      String expr = "cartesianProduct(search("+COLLECTIONORALIAS+", q=\"*:*\", fl=\"id, test_t\", sort=\"id desc\"), analyze(test_t, test_t) as test_t)";
      ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
      paramsLoc.set("expr", expr);
      paramsLoc.set("qt", "/stream");
      String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;

      SolrStream solrStream = new SolrStream(url, paramsLoc);

      StreamContext context = new StreamContext();
      solrStream.setStreamContext(context);
      List<Tuple> tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 5);

      Tuple t = tuples.get(0);
      assertTrue(t.getString("test_t").equals("l"));
      assertTrue(t.getString("id").equals("1"));

      t = tuples.get(1);
      assertTrue(t.getString("test_t").equals("b"));
      assertTrue(t.getString("id").equals("1"));


      t = tuples.get(2);
      assertTrue(t.getString("test_t").equals("c"));
      assertTrue(t.getString("id").equals("1"));


      t = tuples.get(3);
      assertTrue(t.getString("test_t").equals("d"));
      assertTrue(t.getString("id").equals("1"));

      t = tuples.get(4);
      assertTrue(t.getString("test_t").equals("c"));
      assertTrue(t.getString("id").equals("1"));


      expr = "analyze(\"hello world\", test_t)";
      paramsLoc = new ModifiableSolrParams();
      paramsLoc.set("expr", expr);
      paramsLoc.set("qt", "/stream");

      solrStream = new SolrStream(url, paramsLoc);
      context = new StreamContext();
      solrStream.setStreamContext(context);
      tuples = getTuples(solrStream);
      assertEquals(tuples.size(), 1);
      List terms = (List)tuples.get(0).get("return-value");
      assertTrue(terms.get(0).equals("hello"));
      assertTrue(terms.get(1).equals("world"));

      //Try with single param
      expr = "cartesianProduct(search("+COLLECTIONORALIAS+", q=\"*:*\", fl=\"id, test_t\", sort=\"id desc\"), analyze(test_t) as test_t)";
      paramsLoc = new ModifiableSolrParams();
      paramsLoc.set("expr", expr);
      paramsLoc.set("qt", "/stream");

      solrStream = new SolrStream(url, paramsLoc);

      context = new StreamContext();
      solrStream.setStreamContext(context);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 5);

      t = tuples.get(0);
      assertTrue(t.getString("test_t").equals("l"));
      assertTrue(t.getString("id").equals("1"));

      t = tuples.get(1);
      assertTrue(t.getString("test_t").equals("b"));
      assertTrue(t.getString("id").equals("1"));


      t = tuples.get(2);
      assertTrue(t.getString("test_t").equals("c"));
      assertTrue(t.getString("id").equals("1"));


      t = tuples.get(3);
      assertTrue(t.getString("test_t").equals("d"));
      assertTrue(t.getString("id").equals("1"));

      t = tuples.get(4);
      assertTrue(t.getString("test_t").equals("c"));
      assertTrue(t.getString("id").equals("1"));

      //Try with null in the test_t field
      expr = "cartesianProduct(search("+COLLECTIONORALIAS+", q=\"*:*\", fl=\"id\", sort=\"id desc\"), analyze(test_t, test_t) as test_t)";
      paramsLoc = new ModifiableSolrParams();
      paramsLoc.set("expr", expr);
      paramsLoc.set("qt", "/stream");

      solrStream = new SolrStream(url, paramsLoc);

      context = new StreamContext();
      solrStream.setStreamContext(context);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 1);

      //Test annotating tuple
      expr = "select(search("+COLLECTIONORALIAS+", q=\"*:*\", fl=\"id, test_t\", sort=\"id desc\"), analyze(test_t, test_t) as test1_t)";
      paramsLoc = new ModifiableSolrParams();
      paramsLoc.set("expr", expr);
      paramsLoc.set("qt", "/stream");

      solrStream = new SolrStream(url, paramsLoc);

      context = new StreamContext();
      solrStream.setStreamContext(context);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 1);
      List l = (List)tuples.get(0).get("test1_t");
      assertTrue(l.get(0).equals("l"));
      assertTrue(l.get(1).equals("b"));
      assertTrue(l.get(2).equals("c"));
      assertTrue(l.get(3).equals("d"));
      assertTrue(l.get(4).equals("c"));
    } finally {
      cache.close();
    }
  }

  @Test
  public void testHist() throws Exception {
    String expr = "hist(sequence(100, 0, 1), 10)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Map> hist = (List<Map>)tuples.get(0).get("return-value");
    assertTrue(hist.size() == 10);
    for(int i=0; i<hist.size(); i++) {
      Map stats = hist.get(i);
      assertTrue(((Number)stats.get("N")).intValue() == 10);
      assertTrue(((Number)stats.get("min")).intValue() == 10*i);
      assertTrue(((Number)stats.get("var")).doubleValue() == 9.166666666666666);
      assertTrue(((Number)stats.get("stdev")).doubleValue() == 3.0276503540974917);
    }

    expr = "hist(sequence(100, 0, 1), 5)";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    hist = (List<Map>)tuples.get(0).get("return-value");
    assertTrue(hist.size() == 5);
    for(int i=0; i<hist.size(); i++) {
      Map stats = hist.get(i);
      assertTrue(((Number)stats.get("N")).intValue() == 20);
      assertTrue(((Number)stats.get("min")).intValue() == 20*i);
      assertTrue(((Number)stats.get("var")).doubleValue() == 35);
      assertTrue(((Number)stats.get("stdev")).doubleValue() == 5.916079783099616);
    }
  }

  @Test
  public void testCumulativeProbability() throws Exception {
    String expr = "cumulativeProbability(normalDistribution(500, 40), 500)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number number = (Number)tuples.get(0).get("return-value");
    assertTrue(number.doubleValue() == .5D);
  }

  private String getDateString(String year, String month, String day) {
    return year+"-"+month+"-"+day+"T00:00:00Z";
  }

  @Test
  public void testCorrelationStream() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", b=select("+expr+",mult(-1, count(*)) as nvalue), c=col(a, count(*)), d=col(b, nvalue), " +
                       "tuple(corr=corr(c,d), scorr=corr(array(500, 50, 50, 50),d, type=spearmans), kcorr=corr(array(500, 50, 50, 50),d, type=kendalls), d=d))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    assertTrue(tuples.get(0).getDouble("corr").equals(-1.0D));
    assertTrue(tuples.get(0).getDouble("scorr").equals(-1.0D));
    assertTrue(tuples.get(0).getDouble("kcorr").equals(-1.0D));
  }

  @Test
  public void testCovariance() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", b=select("+expr+",mult(-1, count(*)) as nvalue), c=col(a, count(*)), d=col(b, nvalue), tuple(colc=c, cold=d, cov=cov(c,d)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    assertTrue(tuples.get(0).getDouble("cov").equals(-625.0D));
  }

  @Test
  public void testDistance() throws Exception {
    String cexpr = "let(echo=true, " +
                       "a=array(1,2,3,4)," +
                       "b=array(2,3,4,5), " +
                       "c=array(3,4,5,6), " +
                       "d=distance(a, b), " +
                       "e=distance(a, c)," +
                       "f=distance(b, c)," +
                       "g=transpose(matrix(a, b, c))," +
                       "h=distance(g)," +
                       "i=distance(a, b, type=manhattan), " +
                       "j=distance(a, c, type=manhattan)," +
                       "k=distance(b, c, type=manhattan)," +
                       "l=transpose(matrix(a, b, c))," +
                       "m=distance(l, type=manhattan)," +
                       "n=distance(a, b, type=canberra), " +
                       "o=distance(a, c, type=canberra)," +
                       "p=distance(b, c, type=canberra)," +
                       "q=transpose(matrix(a, b, c))," +
                       "r=distance(q, type=canberra)," +
                       "s=distance(a, b, type=earthMovers), " +
                       "t=distance(a, c, type=earthMovers)," +
                       "u=distance(b, c, type=earthMovers)," +
                       "w=transpose(matrix(a, b, c))," +
                       "x=distance(w, type=earthMovers)," +
                       ")";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number d = (Number)tuples.get(0).get("d");
    assertEquals(d.doubleValue(), 2.0, 0.0);
    Number e = (Number)tuples.get(0).get("e");
    assertEquals(e.doubleValue(), 4.0, 0.0);
    Number f = (Number)tuples.get(0).get("f");
    assertEquals(f.doubleValue(), 2.0, 0.0);

    List<List<Number>> h = (List<List<Number>>)tuples.get(0).get("h");
    assertEquals(h.size(), 3);
    assertEquals(h.get(0).size(), 3);
    List<Number> row0 = h.get(0);
    assertEquals(row0.get(0).doubleValue(), 0, 0);
    assertEquals(row0.get(1).doubleValue(), 2, 0);
    assertEquals(row0.get(2).doubleValue(), 4, 0);

    List<Number> row1 = h.get(1);
    assertEquals(row1.get(0).doubleValue(), 2, 0);
    assertEquals(row1.get(1).doubleValue(), 0, 0);
    assertEquals(row1.get(2).doubleValue(), 2, 0);

    List<Number> row2 = h.get(2);
    assertEquals(row2.get(0).doubleValue(), 4, 0);
    assertEquals(row2.get(1).doubleValue(), 2, 0);
    assertEquals(row2.get(2).doubleValue(), 0, 0);

    Number i = (Number)tuples.get(0).get("i");
    assertEquals(i.doubleValue(), 4.0, 0.0);
    Number j = (Number)tuples.get(0).get("j");
    assertEquals(j.doubleValue(), 8.0, 0.0);
    Number k = (Number)tuples.get(0).get("k");
    assertEquals(k.doubleValue(), 4.0, 0.0);

    List<List<Number>> m = (List<List<Number>>)tuples.get(0).get("m");
    assertEquals(m.size(), 3);
    assertEquals(m.get(0).size(), 3);
    row0 = m.get(0);
    assertEquals(row0.get(0).doubleValue(), 0, 0);
    assertEquals(row0.get(1).doubleValue(), 4, 0);
    assertEquals(row0.get(2).doubleValue(), 8, 0);

    row1 = m.get(1);
    assertEquals(row1.get(0).doubleValue(), 4, 0);
    assertEquals(row1.get(1).doubleValue(), 0, 0);
    assertEquals(row1.get(2).doubleValue(), 4, 0);

    row2 = m.get(2);
    assertEquals(row2.get(0).doubleValue(), 8, 0);
    assertEquals(row2.get(1).doubleValue(), 4, 0);
    assertEquals(row2.get(2).doubleValue(), 0, 0);

    Number n = (Number)tuples.get(0).get("n");
    assertEquals(n.doubleValue(), 0.787302, 0.0001);
    Number o = (Number)tuples.get(0).get("o");
    assertEquals(o.doubleValue(), 1.283333, 0.0001);
    Number p = (Number)tuples.get(0).get("p");
    assertEquals(p.doubleValue(), 0.544877, 0.0001);

    List<List<Number>> r = (List<List<Number>>)tuples.get(0).get("r");
    assertEquals(r.size(), 3);
    assertEquals(r.get(0).size(), 3);
    row0 = r.get(0);
    assertEquals(row0.get(0).doubleValue(), 0, 0);
    assertEquals(row0.get(1).doubleValue(), 0.787302, .0001);
    assertEquals(row0.get(2).doubleValue(), 1.283333, .0001);

    row1 = r.get(1);
    assertEquals(row1.get(0).doubleValue(), 0.787302, .0001);
    assertEquals(row1.get(1).doubleValue(), 0, 0);
    assertEquals(row1.get(2).doubleValue(), 0.544877, .0001);

    row2 = r.get(2);
    assertEquals(row2.get(0).doubleValue(), 1.283333, .0001);
    assertEquals(row2.get(1).doubleValue(), 0.544877, .0001);
    assertEquals(row2.get(2).doubleValue(), 0, 0);


    Number s = (Number)tuples.get(0).get("s");
    assertEquals(s.doubleValue(), 10.0, 0);
    Number t = (Number)tuples.get(0).get("t");
    assertEquals(t.doubleValue(), 20.0, 0);
    Number u = (Number)tuples.get(0).get("u");
    assertEquals(u.doubleValue(), 10.0, 0);

    List<List<Number>> x = (List<List<Number>>)tuples.get(0).get("x");
    assertEquals(x.size(), 3);
    assertEquals(x.get(0).size(), 3);
    row0 = x.get(0);
    assertEquals(row0.get(0).doubleValue(), 0, 0);
    assertEquals(row0.get(1).doubleValue(), 10.0, 0);
    assertEquals(row0.get(2).doubleValue(), 20, 0);

    row1 = x.get(1);
    assertEquals(row1.get(0).doubleValue(), 10, 0);
    assertEquals(row1.get(1).doubleValue(), 0, 0);
    assertEquals(row1.get(2).doubleValue(), 10, 0);

    row2 = x.get(2);
    assertEquals(row2.get(0).doubleValue(), 20, 0);
    assertEquals(row2.get(1).doubleValue(), 10, 0);
    assertEquals(row2.get(2).doubleValue(), 0, 0);
  }

  @Test
  public void testReverse() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", c=col(a, max(price_f)), tuple(reverse=rev(c)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> reverse = (List<Number>)tuples.get(0).get("reverse");
    assertTrue(reverse.size() == 4);
    assertTrue(reverse.get(0).doubleValue() == 400D);
    assertTrue(reverse.get(1).doubleValue() == 300D);
    assertTrue(reverse.get(2).doubleValue() == 500D);
    assertTrue(reverse.get(3).doubleValue() == 100D);
  }

  @Test
  public void testCopyOf() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", c=col(a, max(price_f)), tuple(copy1=copyOf(c, 10), copy2=copyOf(c), copy3=copyOf(c, 2) ))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> copy1 = (List<Number>)tuples.get(0).get("copy1");
    assertTrue(copy1.size() == 4);
    assertTrue(copy1.get(0).doubleValue() == 100D);
    assertTrue(copy1.get(1).doubleValue() == 500D);
    assertTrue(copy1.get(2).doubleValue() == 300D);
    assertTrue(copy1.get(3).doubleValue() == 400D);

    List<Number> copy2 = (List<Number>)tuples.get(0).get("copy2");
    assertTrue(copy2.size() == 4);
    assertTrue(copy2.get(0).doubleValue() == 100D);
    assertTrue(copy2.get(1).doubleValue() == 500D);
    assertTrue(copy2.get(2).doubleValue() == 300D);
    assertTrue(copy2.get(3).doubleValue() == 400D);

    List<Number> copy3 = (List<Number>)tuples.get(0).get("copy3");
    assertTrue(copy3.size() == 2);
    assertTrue(copy3.get(0).doubleValue() == 100D);
    assertTrue(copy3.get(1).doubleValue() == 500D);
  }

  @Test
  public void testCopyOfRange() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", c=col(a, max(price_f)), tuple(copy=copyOfRange(c, 1, 3), copy2=copyOfRange(c, 2, 4), l=length(c)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> copy1 = (List<Number>)tuples.get(0).get("copy");
    assertTrue(copy1.size() == 2);
    assertTrue(copy1.get(0).doubleValue() == 500D);
    assertTrue(copy1.get(1).doubleValue() == 300D);

    List<Number> copy2 = (List<Number>)tuples.get(0).get("copy2");
    assertTrue(copy2.size() == 2);
    assertTrue(copy2.get(0).doubleValue() == 300D);
    assertTrue(copy2.get(1).doubleValue() == 400D);

    long l = tuples.get(0).getLong("l");
    assertTrue(l == 4);

  }

  @Test
  public void testPercentile() throws Exception {
    String cexpr = "percentile(array(1,2,3,4,5,6,7,8,9,10,11), 50)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    double p = tuple.getDouble("return-value");
    assertEquals(p, 6, 0.0);


    cexpr = "percentile(array(11,10,3,4,5,6,7,8,9,2,1), 50)";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    tuple = tuples.get(0);
    p = tuple.getDouble("return-value");
    assertEquals(p, 6, 0.0);

    cexpr = "percentile(array(11,10,3,4,5,6,7,8,9,2,1), 20)";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    tuple = tuples.get(0);
    p = tuple.getDouble("return-value");
    assertEquals(p, 2.4, 0.001);
  }

  @Test
  public void testPrimes() throws Exception {
    String cexpr = "primes(10, 0)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    List<Number> asort = (List<Number>)tuple.get("return-value");
    assertEquals(asort.size(), 10);
    assertEquals(asort.get(0).intValue(), 2);
    assertEquals(asort.get(1).intValue(), 3);
    assertEquals(asort.get(2).intValue(), 5);
    assertEquals(asort.get(3).intValue(), 7);
    assertEquals(asort.get(4).intValue(), 11);
    assertEquals(asort.get(5).intValue(), 13);
    assertEquals(asort.get(6).intValue(), 17);
    assertEquals(asort.get(7).intValue(), 19);
    assertEquals(asort.get(8).intValue(), 23);
    assertEquals(asort.get(9).intValue(), 29);
  }

  @Test
  public void testBinomialCoefficient() throws Exception {
    String cexpr = "binomialCoefficient(8,3)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    long binomialCoefficient = (long) tuple.get("return-value");
    assertEquals(binomialCoefficient, 56);
  }

  @Test
  public void testAscend() throws Exception {
    String cexpr = "asc(array(11.5, 12.3, 4, 3, 1, 0))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    List<Number> asort = (List<Number>)tuple.get("return-value");
    assertEquals(asort.size(), 6);
    assertEquals(asort.get(0).doubleValue(), 0, 0.0);
    assertEquals(asort.get(1).doubleValue(), 1, 0.0);
    assertEquals(asort.get(2).doubleValue(), 3, 0.0);
    assertEquals(asort.get(3).doubleValue(), 4, 0.0);
    assertEquals(asort.get(4).doubleValue(), 11.5, 0.0);
    assertEquals(asort.get(5).doubleValue(), 12.3, 0.0);
  }

  @Test
  public void testRankTransform() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", c=col(a, max(price_f)), tuple(reverse=rev(c), ranked=rank(c)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> reverse = (List<Number>)tuples.get(0).get("reverse");
    assertTrue(reverse.size() == 4);
    assertTrue(reverse.get(0).doubleValue() == 400D);
    assertTrue(reverse.get(1).doubleValue() == 300D);
    assertTrue(reverse.get(2).doubleValue() == 500D);
    assertTrue(reverse.get(3).doubleValue() == 100D);

    List<Number> ranked = (List<Number>)tuples.get(0).get("ranked");
    assertTrue(ranked.size() == 4);
    assertTrue(ranked.get(0).doubleValue() == 1D);
    assertTrue(ranked.get(1).doubleValue() == 4D);
    assertTrue(ranked.get(2).doubleValue() == 2D);
    assertTrue(ranked.get(3).doubleValue() == 3D);
  }

  @Test
  public void testArray() throws Exception {
    String cexpr = "array(1, 2, 3, 300, 2, 500)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 6);
    assertTrue(out.get(0).intValue() == 1);
    assertTrue(out.get(1).intValue() == 2);
    assertTrue(out.get(2).intValue() == 3);
    assertTrue(out.get(3).intValue() == 300);
    assertTrue(out.get(4).intValue() == 2);
    assertTrue(out.get(5).intValue() == 500);

    cexpr = "array(1.122, 2.222, 3.333, 300.1, 2.13, 500.23)";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    solrStream = new SolrStream(url, paramsLoc);
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 6);
    assertTrue(out.get(0).doubleValue() == 1.122D);
    assertTrue(out.get(1).doubleValue() == 2.222D);
    assertTrue(out.get(2).doubleValue() == 3.333D);
    assertTrue(out.get(3).doubleValue() == 300.1D);
    assertTrue(out.get(4).doubleValue() == 2.13D);
    assertTrue(out.get(5).doubleValue() == 500.23D);
  }

  @Test
  public void testMatrix() throws Exception {
    String cexpr = "let(echo=true," +
        "               a=setColumnLabels(matrix(array(1, 2, 3), " +
        "                                        rev(array(4,5,6)))," +
        "                                        array(col1, col2, col3))," +
        "               b=rowAt(a, 1)," +
        "               c=colAt(a, 2)," +
        "               d=getColumnLabels(a)," +
        "               e=topFeatures(a, 1)," +
        "               f=rowCount(a)," +
        "               g=columnCount(a)," +
        "               h=indexOf(d, \"col2\")," +
        "               i=indexOf(d, col3))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> out = (List<List<Number>>)tuples.get(0).get("a");

    List<Number> array1 = out.get(0);
    assertEquals(array1.size(), 3);
    assertEquals(array1.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(array1.get(1).doubleValue(), 2.0, 0.0);
    assertEquals(array1.get(2).doubleValue(), 3.0, 0.0);

    List<Number> array2 = out.get(1);
    assertEquals(array2.size(), 3);
    assertEquals(array2.get(0).doubleValue(), 6.0, 0.0);
    assertEquals(array2.get(1).doubleValue(), 5.0, 0.0);
    assertEquals(array2.get(2).doubleValue(), 4.0, 0.0);

    List<Number> row = (List<Number>)tuples.get(0).get("b");

    assertEquals(row.size(), 3);
    assertEquals(array2.get(0).doubleValue(), 6.0, 0.0);
    assertEquals(array2.get(1).doubleValue(), 5.0, 0.0);
    assertEquals(array2.get(2).doubleValue(), 4.0, 0.0);

    List<Number> col = (List<Number>)tuples.get(0).get("c");
    assertEquals(col.size(), 2);
    assertEquals(col.get(0).doubleValue(), 3.0, 0.0);
    assertEquals(col.get(1).doubleValue(), 4.0, 0.0);

    List<String> colLabels = (List<String>)tuples.get(0).get("d");
    assertEquals(colLabels.size(), 3);
    assertEquals(colLabels.get(0), "col1");
    assertEquals(colLabels.get(1), "col2");
    assertEquals(colLabels.get(2), "col3");

    List<List<String>> features  = (List<List<String>>)tuples.get(0).get("e");
    assertEquals(features.size(), 2);
    assertEquals(features.get(0).size(), 1);
    assertEquals(features.get(1).size(), 1);
    assertEquals(features.get(0).get(0), "col3");
    assertEquals(features.get(1).get(0), "col1");

    assertTrue(tuples.get(0).getLong("f") == 2);
    assertTrue(tuples.get(0).getLong("g")== 3);
    assertTrue(tuples.get(0).getLong("h")== 1);
    assertTrue(tuples.get(0).getLong("i")== 2);
  }

  @Test
  public void testMatrixMath() throws Exception {
    String cexpr = "let(echo=true, a=matrix(array(1.5, 2.5, 3.5), array(4.5,5.5,6.5)), " +
                                  "b=grandSum(a), " +
                                  "c=sumRows(a), " +
                                  "d=sumColumns(a), " +
                                  "e=scalarAdd(1, a)," +
                                  "f=scalarSubtract(1, a)," +
                                  "g=scalarMultiply(1.5, a)," +
                                  "h=scalarDivide(1.5, a)," +
                                  "i=scalarAdd(1.5, array(1.5, 2.5, 3.5))," +
                                  "j=scalarSubtract(1.5, array(1.5, 2.5, 3.5))," +
                                  "k=scalarMultiply(1.5, array(1.5, 2.5, 3.5))," +
                                  "l=scalarDivide(1.5, array(1.5, 2.5, 3.5)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);

    double grandSum = tuples.get(0).getDouble("b");
    assertEquals(grandSum, 24, 0.0);

    List<Number> sumRows = (List<Number>)tuples.get(0).get("c");
    assertEquals(sumRows.size(), 2);
    assertEquals(sumRows.get(0).doubleValue(), 7.5, 0.0);
    assertEquals(sumRows.get(1).doubleValue(), 16.5, 0.0);

    List<Number> sumCols = (List<Number>)tuples.get(0).get("d");
    assertEquals(sumCols.size(), 3);
    assertEquals(sumCols.get(0).doubleValue(), 6.0, 0.0);
    assertEquals(sumCols.get(1).doubleValue(), 8.0, 0.0);
    assertEquals(sumCols.get(2).doubleValue(), 10, 0.0);

    List<List<Number>> scalarAdd = (List<List<Number>>)tuples.get(0).get("e");
    List<Number> row1 = scalarAdd.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 2.5, 0.0);
    assertEquals(row1.get(1).doubleValue(), 3.5, 0.0);
    assertEquals(row1.get(2).doubleValue(), 4.5, 0.0);

    List<Number> row2 = scalarAdd.get(1);
    assertEquals(row2.get(0).doubleValue(), 5.5, 0.0);
    assertEquals(row2.get(1).doubleValue(), 6.5, 0.0);
    assertEquals(row2.get(2).doubleValue(), 7.5, 0.0);

    List<List<Number>> scalarSubtract = (List<List<Number>>)tuples.get(0).get("f");
    row1 = scalarSubtract.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 0.5, 0.0);
    assertEquals(row1.get(1).doubleValue(), 1.5, 0.0);
    assertEquals(row1.get(2).doubleValue(), 2.5, 0.0);

    row2 = scalarSubtract.get(1);
    assertEquals(row2.get(0).doubleValue(), 3.5, 0.0);
    assertEquals(row2.get(1).doubleValue(), 4.5, 0.0);
    assertEquals(row2.get(2).doubleValue(), 5.5, 0.0);

    List<List<Number>> scalarMultiply = (List<List<Number>>)tuples.get(0).get("g");
    row1 = scalarMultiply.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 2.25, 0.0);
    assertEquals(row1.get(1).doubleValue(), 3.75, 0.0);
    assertEquals(row1.get(2).doubleValue(), 5.25, 0.0);

    row2 = scalarMultiply.get(1);
    assertEquals(row2.get(0).doubleValue(), 6.75, 0.0);
    assertEquals(row2.get(1).doubleValue(), 8.25, 0.0);
    assertEquals(row2.get(2).doubleValue(), 9.75, 0.0);

    List<List<Number>> scalarDivide = (List<List<Number>>)tuples.get(0).get("h");
    row1 = scalarDivide.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(row1.get(1).doubleValue(), 1.66666666666667, 0.001);
    assertEquals(row1.get(2).doubleValue(), 2.33333333333333, 0.001);

    row2 = scalarDivide.get(1);
    assertEquals(row2.get(0).doubleValue(), 3, 0.0);
    assertEquals(row2.get(1).doubleValue(), 3.66666666666667, 0.001);
    assertEquals(row2.get(2).doubleValue(), 4.33333333333333, 0.001);

    List<Number> rowA = (List<Number>)tuples.get(0).get("i");
    assertEquals(rowA.size(), 3);
    assertEquals(rowA.get(0).doubleValue(), 3.0, 0.0);
    assertEquals(rowA.get(1).doubleValue(), 4.0, 0.0);
    assertEquals(rowA.get(2).doubleValue(), 5.0, 0.0);

    rowA = (List<Number>)tuples.get(0).get("j");
    assertEquals(rowA.size(), 3);
    assertEquals(rowA.get(0).doubleValue(), 0, 0.0);
    assertEquals(rowA.get(1).doubleValue(), 1.0, 0.0);
    assertEquals(rowA.get(2).doubleValue(), 2.0, 0.0);

    rowA = (List<Number>)tuples.get(0).get("k");
    assertEquals(rowA.size(), 3);
    assertEquals(rowA.get(0).doubleValue(), 2.25, 0.0);
    assertEquals(rowA.get(1).doubleValue(), 3.75, 0.0);
    assertEquals(rowA.get(2).doubleValue(), 5.25, 0.0);

    rowA = (List<Number>)tuples.get(0).get("l");
    assertEquals(rowA.size(), 3);
    assertEquals(rowA.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(rowA.get(1).doubleValue(), 1.66666666666667, 0.001);
    assertEquals(rowA.get(2).doubleValue(), 2.33333333333333, 0.001);
  }

  @Test
  public void testTranspose() throws Exception {
    String cexpr = "let(a=matrix(array(1,2,3), array(4,5,6)), b=transpose(a))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> out = (List<List<Number>>)tuples.get(0).get("b");
    assertEquals(out.size(), 3);
    List<Number> array1 = out.get(0);
    assertEquals(array1.size(), 2);
    assertEquals(array1.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(array1.get(1).doubleValue(), 4.0, 0.0);

    List<Number> array2 = out.get(1);
    assertEquals(array2.size(), 2);
    assertEquals(array2.get(0).doubleValue(), 2.0, 0.0);
    assertEquals(array2.get(1).doubleValue(), 5.0, 0.0);

    List<Number> array3 = out.get(2);
    assertEquals(array3.size(), 2);
    assertEquals(array3.get(0).doubleValue(), 3.0, 0.0);
    assertEquals(array3.get(1).doubleValue(), 6.0, 0.0);
  }

  @Test
  public void testUnitize() throws Exception {
    String cexpr = "let(echo=true, a=unitize(matrix(array(1,2,3), array(4,5,6))), b=unitize(array(4,5,6)))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> out = (List<List<Number>>)tuples.get(0).get("a");
    assertEquals(out.size(), 2);
    List<Number> array1 = out.get(0);
    assertEquals(array1.size(), 3);
    assertEquals(array1.get(0).doubleValue(), 0.2672612419124244, 0.0);
    assertEquals(array1.get(1).doubleValue(), 0.5345224838248488, 0.0);
    assertEquals(array1.get(2).doubleValue(), 0.8017837257372732, 0.0);

    List<Number> array2 = out.get(1);
    assertEquals(array2.size(), 3);
    assertEquals(array2.get(0).doubleValue(), 0.4558423058385518, 0.0);
    assertEquals(array2.get(1).doubleValue(), 0.5698028822981898, 0.0);
    assertEquals(array2.get(2).doubleValue(), 0.6837634587578276, 0.0);

    List<Number> array3 = (List<Number>)tuples.get(0).get("b");
    assertEquals(array3.size(), 3);
    assertEquals(array3.get(0).doubleValue(), 0.4558423058385518, 0.0);
    assertEquals(array3.get(1).doubleValue(), 0.5698028822981898, 0.0);
    assertEquals(array3.get(2).doubleValue(), 0.6837634587578276, 0.0);
  }

  @Test
  public void testNormalizeSum() throws Exception {
    String cexpr = "let(echo=true, " +
                       "a=normalizeSum(matrix(array(1,2,3), array(4,5,6))), " +
                       "b=normalizeSum(array(1,2,3))," +
                       "c=normalizeSum(array(1,2,3), 100))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> out = (List<List<Number>>)tuples.get(0).get("a");
    assertEquals(out.size(), 2);
    List<Number> array1 = out.get(0);
    assertEquals(array1.size(), 3);
    assertEquals(array1.get(0).doubleValue(), 0.16666666666666666, 0.0001);
    assertEquals(array1.get(1).doubleValue(), 0.3333333333333333, 0.00001);
    assertEquals(array1.get(2).doubleValue(), 0.5, 0.0001);

    List<Number> array2 = out.get(1);
    assertEquals(array2.size(), 3);
    assertEquals(array2.get(0).doubleValue(), 0.26666666666666666, 0.0001);
    assertEquals(array2.get(1).doubleValue(), 0.3333333333333333, 0.0001);
    assertEquals(array2.get(2).doubleValue(), 0.4, 0.0001);

    List<Number> array3 = (List<Number>)tuples.get(0).get("b");
    assertEquals(array3.size(), 3);
    assertEquals(array3.get(0).doubleValue(), 0.16666666666666666, 0.0001);
    assertEquals(array3.get(1).doubleValue(), 0.3333333333333333, 0.0001);
    assertEquals(array3.get(2).doubleValue(), 0.5, 0.0001);

    List<Number> array4 = (List<Number>)tuples.get(0).get("c");
    assertEquals(array4.size(), 3);
    assertEquals(array4.get(0).doubleValue(), 16.666666666666666, 0.0001);
    assertEquals(array4.get(1).doubleValue(), 33.33333333333333, 0.00001);
    assertEquals(array4.get(2).doubleValue(), 50, 0.0001);
  }

  @Test
  public void testStandardize() throws Exception {
    String cexpr = "let(echo=true, a=standardize(matrix(array(1,2,3), array(4,5,6))), b=standardize(array(4,5,6)))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> out = (List<List<Number>>)tuples.get(0).get("a");
    assertEquals(out.size(), 2);
    List<Number> array1 = out.get(0);
    assertEquals(array1.size(), 3);
    assertEquals(array1.get(0).doubleValue(), -1, 0.0);
    assertEquals(array1.get(1).doubleValue(), 0, 0.0);
    assertEquals(array1.get(2).doubleValue(), 1, 0.0);

    List<Number> array2 = out.get(1);
    assertEquals(array2.size(), 3);
    assertEquals(array2.get(0).doubleValue(), -1, 0.0);
    assertEquals(array2.get(1).doubleValue(), 0, 0.0);
    assertEquals(array2.get(2).doubleValue(), 1, 0.0);

    List<Number> array3 = (List<Number>)tuples.get(0).get("b");
    assertEquals(array3.size(), 3);
    assertEquals(array2.get(0).doubleValue(), -1, 0.0);
    assertEquals(array2.get(1).doubleValue(), 0, 0.0);
    assertEquals(array2.get(2).doubleValue(), 1, 0.0);
  }

  @Test
  public void testMarkovChain() throws Exception {
    String cexpr = "let(state0=array(.5,.5),\n" +
                   "    state1=array(.5,.5),\n" +
                   "    states=matrix(state0, state1),\n" +
                   "    m=markovChain(states, 0),\n" +
                   "    s=sample(m, 50000),\n" +
                   "    f=freqTable(s))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Map<String, Number>> out = (List<Map<String, Number>>)tuples.get(0).get("f");
    assertEquals(out.size(), 2);
    Map<String, Number> bin0 = out.get(0);
    double state0Pct = bin0.get("pct").doubleValue();
    assertEquals(state0Pct, .5, .015);
    Map<String, Number> bin1 = out.get(1);
    double state1Pct = bin1.get("pct").doubleValue();
    assertEquals(state1Pct, .5, .015);
  }

  @Test
  public void testAddAll() throws Exception {
    String cexpr = "addAll(array(1, 2, 3), array(4.5, 5.5, 6.5), array(7,8,9))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 9);
    assertTrue(out.get(0).intValue() == 1);
    assertTrue(out.get(1).intValue() == 2);
    assertTrue(out.get(2).intValue() == 3);
    assertTrue(out.get(3).doubleValue() == 4.5D);
    assertTrue(out.get(4).doubleValue() == 5.5D);
    assertTrue(out.get(5).doubleValue() == 6.5D);
    assertTrue(out.get(6).intValue() == 7);
    assertTrue(out.get(7).intValue() == 8);
    assertTrue(out.get(8).intValue() == 9);
  }

  @Test
  public void testProbabilityRange() throws Exception {
    String cexpr = "let(a=normalDistribution(500, 20), " +
                       "b=probability(a, 520, 530))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number prob = (Number)tuples.get(0).get("b");
    assertEquals(prob.doubleValue(),  0.09184805266259899, 0.0);
  }

  @Test
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void testDistributions() throws Exception {
    String cexpr = "let(a=normalDistribution(10, 2), " +
                       "b=sample(a, 250), " +
                       "c=normalDistribution(100, 6), " +
                       "d=sample(c, 250), " +
                       "u=uniformDistribution(1, 6),"+
                       "t=sample(u, 250),"+
                       "e=empiricalDistribution(d),"+
                       "f=sample(e, 250),"+
                       "tuple(sample=b, ks=ks(a,b), ks2=ks(a, d), ks3=ks(u, t)))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    try {
      TupleStream solrStream = new SolrStream(url, paramsLoc);
      StreamContext context = new StreamContext();
      solrStream.setStreamContext(context);
      List<Tuple> tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 1);
      List<Number> out = (List<Number>) tuples.get(0).get("sample");

      Map ks = (Map) tuples.get(0).get("ks");
      Map ks2 = (Map) tuples.get(0).get("ks2");
      Map ks3 = (Map) tuples.get(0).get("ks3");

      assertTrue(out.size() == 250);
      Number pvalue = (Number) ks.get("p-value");
      Number pvalue2 = (Number) ks2.get("p-value");
      Number pvalue3 = (Number) ks3.get("p-value");

      assertTrue(pvalue.doubleValue() > .05D);
      assertTrue(pvalue2.doubleValue() == 0);
      assertTrue(pvalue3.doubleValue() > .05D);

    } catch(AssertionError e) {

      //This test will have random failures do to the random sampling. So if it fails try it again.
      //If it fails twice in a row, we probably broke some code.

      TupleStream solrStream = new SolrStream(url, paramsLoc);
      StreamContext context = new StreamContext();
      solrStream.setStreamContext(context);
      List<Tuple> tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 1);
      List<Number> out = (List<Number>) tuples.get(0).get("sample");

      Map ks = (Map) tuples.get(0).get("ks");
      Map ks2 = (Map) tuples.get(0).get("ks2");
      Map ks3 = (Map) tuples.get(0).get("ks3");

      assertTrue(out.size() == 250);
      Number pvalue = (Number) ks.get("p-value");
      Number pvalue2 = (Number) ks2.get("p-value");
      Number pvalue3 = (Number) ks3.get("p-value");

      assertTrue(pvalue.doubleValue() > .05D);
      assertTrue(pvalue2.doubleValue() == 0);
      assertTrue(pvalue3.doubleValue() > .05D);
    }
  }

  @Test
  public void testSumDifference() throws Exception {
    String cexpr = "sumDifference(array(2,4,6,8,10,12),array(1,2,3,4,5,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    double sd = tuples.get(0).getDouble("return-value");
    assertEquals(sd, 21.0D, 0.0);
  }

  @Test
  public void testMeanDifference() throws Exception {
    String cexpr = "meanDifference(array(2,4,6,8,10,12),array(1,2,3,4,5,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    double sd = tuples.get(0).getDouble("return-value");
    assertEquals(sd, 3.5, 0.0);
  }

  @Test
  public void testTermVectors() throws Exception {
    // Test termVectors with only documents and default termVector settings
    String cexpr = "let(echo=true," +
                       "a=select(list(tuple(id=\"1\", text=\"hello world\"), " +
                                     "tuple(id=\"2\", text=\"hello steve\"), " +
                                     "tuple(id=\"3\", text=\"hello jim jim\"), " +
                                     "tuple(id=\"4\", text=\"hello jack\")), id, analyze(text, test_t) as terms)," +
                   "    b=termVectors(a, minDocFreq=0, maxDocFreq=1)," +
        "               c=getRowLabels(b)," +
        "               d=getColumnLabels(b)," +
        "               e=getAttribute(b, docFreqs))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> termVectors  = (List<List<Number>>)tuples.get(0).get("b");

    assertEquals(termVectors.size(), 4);
    List<Number> termVector = termVectors.get(0);
    assertEquals(termVector.size(), 5);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(termVector.get(1).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(2).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(3).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(4).doubleValue(), 1.916290731874155, 0.0);

    termVector = termVectors.get(1);
    assertEquals(termVector.size(), 5);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(termVector.get(1).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(2).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(3).doubleValue(), 1.916290731874155, 0.0);
    assertEquals(termVector.get(4).doubleValue(), 0.0, 0.0);

    termVector = termVectors.get(2);
    assertEquals(termVector.size(), 5);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(termVector.get(1).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(2).doubleValue(), 2.7100443424662948, 0.0);
    assertEquals(termVector.get(3).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(4).doubleValue(), 0.0, 0.0);

    termVector = termVectors.get(3);
    assertEquals(termVector.size(), 5);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(termVector.get(1).doubleValue(), 1.916290731874155, 0.0);
    assertEquals(termVector.get(2).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(3).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(4).doubleValue(), 0.0, 0.0);

    List<String> rowLabels  = (List<String>)tuples.get(0).get("c");
    assertEquals(rowLabels.size(), 4);
    assertEquals(rowLabels.get(0), "1");
    assertEquals(rowLabels.get(1), "2");
    assertEquals(rowLabels.get(2), "3");
    assertEquals(rowLabels.get(3), "4");

    List<String> columnLabels  = (List<String>)tuples.get(0).get("d");
    assertEquals(columnLabels.size(), 5);
    assertEquals(columnLabels.get(0), "hello");
    assertEquals(columnLabels.get(1), "jack");
    assertEquals(columnLabels.get(2), "jim");
    assertEquals(columnLabels.get(3), "steve");
    assertEquals(columnLabels.get(4), "world");

    Map<String, Number> docFreqs  = (Map<String, Number>)tuples.get(0).get("e");

    assertEquals(docFreqs.size(), 5);
    assertEquals(docFreqs.get("hello").intValue(), 4);
    assertEquals(docFreqs.get("jack").intValue(), 1);
    assertEquals(docFreqs.get("jim").intValue(), 1);
    assertEquals(docFreqs.get("steve").intValue(), 1);
    assertEquals(docFreqs.get("world").intValue(), 1);

    //Test minTermLength. This should drop off the term jim

    cexpr = "let(echo=true," +
                 "a=select(list(tuple(id=\"1\", text=\"hello world\"), " +
                               "tuple(id=\"2\", text=\"hello steve\"), " +
                               "tuple(id=\"3\", text=\"hello jim jim\"), " +
                               "tuple(id=\"4\", text=\"hello jack\")), id, analyze(text, test_t) as terms)," +
            "    b=termVectors(a, minTermLength=4, minDocFreq=0, maxDocFreq=1)," +
            "    c=getRowLabels(b)," +
            "    d=getColumnLabels(b)," +
            "    e=getAttribute(b, docFreqs))";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    termVectors  = (List<List<Number>>)tuples.get(0).get("b");
    assertEquals(termVectors.size(), 4);
    termVector = termVectors.get(0);
    assertEquals(termVector.size(), 4);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(termVector.get(1).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(2).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(3).doubleValue(), 1.916290731874155, 0.0);

    termVector = termVectors.get(1);
    assertEquals(termVector.size(), 4);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(termVector.get(1).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(2).doubleValue(), 1.916290731874155, 0.0);
    assertEquals(termVector.get(3).doubleValue(), 0.0, 0.0);

    termVector = termVectors.get(2);
    assertEquals(termVector.size(), 4);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(termVector.get(1).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(2).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(3).doubleValue(), 0.0, 0.0);

    termVector = termVectors.get(3);
    assertEquals(termVector.size(), 4);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(termVector.get(1).doubleValue(), 1.916290731874155, 0.0);
    assertEquals(termVector.get(2).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(3).doubleValue(), 0.0, 0.0);

    rowLabels  = (List<String>)tuples.get(0).get("c");
    assertEquals(rowLabels.size(), 4);
    assertEquals(rowLabels.get(0), "1");
    assertEquals(rowLabels.get(1), "2");
    assertEquals(rowLabels.get(2), "3");
    assertEquals(rowLabels.get(3), "4");

    columnLabels  = (List<String>)tuples.get(0).get("d");
    assertEquals(columnLabels.size(), 4);
    assertEquals(columnLabels.get(0), "hello");
    assertEquals(columnLabels.get(1), "jack");
    assertEquals(columnLabels.get(2), "steve");
    assertEquals(columnLabels.get(3), "world");

    docFreqs  = (Map<String, Number>)tuples.get(0).get("e");

    assertEquals(docFreqs.size(), 4);
    assertEquals(docFreqs.get("hello").intValue(), 4);
    assertEquals(docFreqs.get("jack").intValue(), 1);
    assertEquals(docFreqs.get("steve").intValue(), 1);
    assertEquals(docFreqs.get("world").intValue(), 1);


    //Test exclude. This should drop off the term jim

    cexpr = "let(echo=true," +
        "        a=select(list(tuple(id=\"1\", text=\"hello world\"), " +
        "                      tuple(id=\"2\", text=\"hello steve\"), " +
        "                      tuple(id=\"3\", text=\"hello jim jim\"), " +
        "                      tuple(id=\"4\", text=\"hello jack\")), id, analyze(text, test_t) as terms)," +
        "        b=termVectors(a, exclude=jim, minDocFreq=0, maxDocFreq=1)," +
        "        c=getRowLabels(b)," +
        "        d=getColumnLabels(b)," +
        "        e=getAttribute(b, docFreqs))";

    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    termVectors  = (List<List<Number>>)tuples.get(0).get("b");
    assertEquals(termVectors.size(), 4);
    termVector = termVectors.get(0);
    assertEquals(termVector.size(), 4);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(termVector.get(1).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(2).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(3).doubleValue(), 1.916290731874155, 0.0);

    termVector = termVectors.get(1);
    assertEquals(termVector.size(), 4);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(termVector.get(1).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(2).doubleValue(), 1.916290731874155, 0.0);
    assertEquals(termVector.get(3).doubleValue(), 0.0, 0.0);

    termVector = termVectors.get(2);
    assertEquals(termVector.size(), 4);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(termVector.get(1).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(2).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(3).doubleValue(), 0.0, 0.0);

    termVector = termVectors.get(3);
    assertEquals(termVector.size(), 4);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(termVector.get(1).doubleValue(), 1.916290731874155, 0.0);
    assertEquals(termVector.get(2).doubleValue(), 0.0, 0.0);
    assertEquals(termVector.get(3).doubleValue(), 0.0, 0.0);

    rowLabels  = (List<String>)tuples.get(0).get("c");
    assertEquals(rowLabels.size(), 4);
    assertEquals(rowLabels.get(0), "1");
    assertEquals(rowLabels.get(1), "2");
    assertEquals(rowLabels.get(2), "3");
    assertEquals(rowLabels.get(3), "4");

    columnLabels  = (List<String>)tuples.get(0).get("d");
    assertEquals(columnLabels.size(), 4);
    assertEquals(columnLabels.get(0), "hello");
    assertEquals(columnLabels.get(1), "jack");
    assertEquals(columnLabels.get(2), "steve");
    assertEquals(columnLabels.get(3), "world");

    docFreqs  = (Map<String, Number>)tuples.get(0).get("e");

    assertEquals(docFreqs.size(), 4);
    assertEquals(docFreqs.get("hello").intValue(), 4);
    assertEquals(docFreqs.get("jack").intValue(), 1);
    assertEquals(docFreqs.get("steve").intValue(), 1);
    assertEquals(docFreqs.get("world").intValue(), 1);

    //Test minDocFreq attribute at .5. This should eliminate all but the term hello

    cexpr = "let(echo=true," +
        "a=select(list(tuple(id=\"1\", text=\"hello world\"), " +
        "tuple(id=\"2\", text=\"hello steve\"), " +
        "tuple(id=\"3\", text=\"hello jim jim\"), " +
        "tuple(id=\"4\", text=\"hello jack\")), id, analyze(text, test_t) as terms)," +
        "    b=termVectors(a, minDocFreq=.5, maxDocFreq=1)," +
        "    c=getRowLabels(b)," +
        "    d=getColumnLabels(b)," +
        "    e=getAttribute(b, docFreqs))";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    termVectors  = (List<List<Number>>)tuples.get(0).get("b");

    assertEquals(termVectors.size(), 4);
    termVector = termVectors.get(0);
    assertEquals(termVector.size(), 1);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);

    termVector = termVectors.get(1);
    assertEquals(termVector.size(), 1);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);

    termVector = termVectors.get(2);
    assertEquals(termVector.size(), 1);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);

    termVector = termVectors.get(3);
    assertEquals(termVector.size(), 1);
    assertEquals(termVector.get(0).doubleValue(), 1.0, 0.0);

    rowLabels  = (List<String>)tuples.get(0).get("c");
    assertEquals(rowLabels.size(), 4);
    assertEquals(rowLabels.get(0), "1");
    assertEquals(rowLabels.get(1), "2");
    assertEquals(rowLabels.get(2), "3");
    assertEquals(rowLabels.get(3), "4");

    columnLabels  = (List<String>)tuples.get(0).get("d");
    assertEquals(columnLabels.size(), 1);
    assertEquals(columnLabels.get(0), "hello");

    docFreqs  = (Map<String, Number>)tuples.get(0).get("e");

    assertEquals(docFreqs.size(), 1);
    assertEquals(docFreqs.get("hello").intValue(), 4);

    //Test maxDocFreq attribute at 0. This should eliminate all terms

    cexpr = "let(echo=true," +
        "a=select(list(tuple(id=\"1\", text=\"hello world\"), " +
        "tuple(id=\"2\", text=\"hello steve\"), " +
        "tuple(id=\"3\", text=\"hello jim jim\"), " +
        "tuple(id=\"4\", text=\"hello jack\")), id, analyze(text, test_t) as terms)," +
        "    b=termVectors(a, maxDocFreq=0)," +
        "    c=getRowLabels(b)," +
        "    d=getColumnLabels(b)," +
        "    e=getAttribute(b, docFreqs))";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    termVectors  = (List<List<Number>>)tuples.get(0).get("b");
    assertEquals(termVectors.size(), 4);
    assertEquals(termVectors.get(0).size(), 0);
  }

  @Test
  public void testEbeSubtract() throws Exception {
    String cexpr = "let(echo=true," +
        "               a=array(2, 4, 6, 8, 10, 12)," +
        "               b=array(1, 2, 3, 4, 5, 6)," +
        "               c=ebeSubtract(a,b)," +
        "               d=array(10, 11, 12, 13, 14, 15)," +
        "               e=array(100, 200, 300, 400, 500, 600)," +
        "               f=matrix(a, b)," +
        "               g=matrix(d, e)," +
        "               h=ebeSubtract(f, g))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("c");
    assertEquals(out.size(), 6);
    assertEquals(out.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(out.get(1).doubleValue(), 2.0, 0.0);
    assertEquals(out.get(2).doubleValue(), 3.0, 0.0);
    assertEquals(out.get(3).doubleValue(), 4.0, 0.0);
    assertEquals(out.get(4).doubleValue(), 5.0, 0.0);
    assertEquals(out.get(5).doubleValue(), 6.0, 0.0);

    List<List<Number>> mout = (List<List<Number>>)tuples.get(0).get("h");
    assertEquals(mout.size(), 2);
    List<Number> row1 = mout.get(0);
    assertEquals(row1.size(), 6);
    assertEquals(row1.get(0).doubleValue(), -8.0, 0.0);
    assertEquals(row1.get(1).doubleValue(), -7.0, 0.0);
    assertEquals(row1.get(2).doubleValue(), -6.0, 0.0);
    assertEquals(row1.get(3).doubleValue(), -5.0, 0.0);
    assertEquals(row1.get(4).doubleValue(), -4.0, 0.0);
    assertEquals(row1.get(5).doubleValue(), -3.0, 0.0);

    List<Number> row2 = mout.get(1);
    assertEquals(row2.size(), 6);
    assertEquals(row2.get(0).doubleValue(), -99.0, 0.0);
    assertEquals(row2.get(1).doubleValue(), -198.0, 0.0);
    assertEquals(row2.get(2).doubleValue(), -297.0, 0.0);
    assertEquals(row2.get(3).doubleValue(), -396.0, 0.0);
    assertEquals(row2.get(4).doubleValue(), -495.0, 0.0);
    assertEquals(row2.get(5).doubleValue(), -594.0, 0.0);
  }

  @Test
  public void testMatrixMult() throws Exception {
    String cexpr = "let(echo=true," +
        "               a=array(1,2,3)," +
        "               b=matrix(array(4), array(5), array(6))," +
        "               c=matrixMult(a, b)," +
        "               d=matrix(array(3, 4), array(10,11), array(30, 40))," +
        "               e=matrixMult(a, d)," +
        "               f=array(4,8,10)," +
        "               g=matrix(a, f)," +
        "               h=matrixMult(d, g)," +
        "               i=matrixMult(b, a))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> matrix = (List<List<Number>>)tuples.get(0).get("c");
    assertEquals(matrix.size(), 1);
    List<Number> row = matrix.get(0);
    assertEquals(row.size(), 1);
    assertEquals(row.get(0).doubleValue(), 32.0, 0.0);

    matrix = (List<List<Number>>)tuples.get(0).get("e");
    assertEquals(matrix.size(), 1);
    row = matrix.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).doubleValue(), 113.0, 0.0);
    assertEquals(row.get(1).doubleValue(), 146.0, 0.0);

    matrix = (List<List<Number>>)tuples.get(0).get("h");
    assertEquals(matrix.size(), 3);
    row = matrix.get(0);
    assertEquals(row.size(), 3);
    assertEquals(row.get(0).doubleValue(), 19.0, 0.0);
    assertEquals(row.get(1).doubleValue(), 38.0, 0.0);
    assertEquals(row.get(2).doubleValue(), 49.0, 0.0);

    row = matrix.get(1);
    assertEquals(row.size(), 3);
    assertEquals(row.get(0).doubleValue(), 54.0, 0.0);
    assertEquals(row.get(1).doubleValue(), 108.0, 0.0);
    assertEquals(row.get(2).doubleValue(), 140.0, 0.0);

    row = matrix.get(2);
    assertEquals(row.size(), 3);
    assertEquals(row.get(0).doubleValue(), 190.0, 0.0);
    assertEquals(row.get(1).doubleValue(), 380.0, 0.0);
    assertEquals(row.get(2).doubleValue(), 490.0, 0.0);

    matrix = (List<List<Number>>)tuples.get(0).get("i");

    assertEquals(matrix.size(), 3);
    row = matrix.get(0);
    assertEquals(row.size(), 3);
    assertEquals(row.get(0).doubleValue(), 4.0, 0.0);
    assertEquals(row.get(1).doubleValue(), 8.0, 0.0);
    assertEquals(row.get(2).doubleValue(), 12.0, 0.0);

    row = matrix.get(1);
    assertEquals(row.size(), 3);
    assertEquals(row.get(0).doubleValue(), 5.0, 0.0);
    assertEquals(row.get(1).doubleValue(), 10.0, 0.0);
    assertEquals(row.get(2).doubleValue(), 15.0, 0.0);

    row = matrix.get(2);
    assertEquals(row.size(), 3);
    assertEquals(row.get(0).doubleValue(), 6.0, 0.0);
    assertEquals(row.get(1).doubleValue(), 12.0, 0.0);
    assertEquals(row.get(2).doubleValue(), 18.0, 0.0);
  }

  @Test
  public void testKmeans() throws Exception {
    String cexpr = "let(echo=true," +
        "               a=array(1,1,1,0,0,0)," +
        "               b=array(1,1,1,0,0,0)," +
        "               c=array(0,0,0,1,1,1)," +
        "               d=array(0,0,0,1,1,1)," +
        "               e=setRowLabels(matrix(a,b,c,d), " +
        "                              array(doc1, doc2, doc3, doc4))," +
        "               f=kmeans(e, 2)," +
        "               g=getCluster(f, 0)," +
        "               h=getCluster(f, 1)," +
        "               i=getCentroids(f)," +
        "               j=getRowLabels(g)," +
        "               k=getRowLabels(h))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> cluster1 = (List<List<Number>>)tuples.get(0).get("g");
    List<List<Number>> cluster2 = (List<List<Number>>)tuples.get(0).get("h");
    List<List<Number>> centroids = (List<List<Number>>)tuples.get(0).get("i");
    List<String> labels1 = (List<String>)tuples.get(0).get("j");
    List<String> labels2 = (List<String>)tuples.get(0).get("k");

    assertEquals(cluster1.size(), 2);
    assertEquals(cluster2.size(), 2);
    assertEquals(centroids.size(), 2);

    //Assert that the docs are not in both clusters
    assertTrue(!(labels1.contains("doc1") && labels2.contains("doc1")));
    assertTrue(!(labels1.contains("doc2") && labels2.contains("doc2")));
    assertTrue(!(labels1.contains("doc3") && labels2.contains("doc3")));
    assertTrue(!(labels1.contains("doc4") && labels2.contains("doc4")));

    //Assert that (doc1 and doc2) or (doc3 and doc4) are in labels1
    assertTrue((labels1.contains("doc1") && labels1.contains("doc2")) ||
        ((labels1.contains("doc3") && labels1.contains("doc4"))));

    //Assert that (doc1 and doc2) or (doc3 and doc4) are in labels2
    assertTrue((labels2.contains("doc1") && labels2.contains("doc2")) ||
        ((labels2.contains("doc3") && labels2.contains("doc4"))));

    if(labels1.contains("doc1")) {
      assertEquals(centroids.get(0).get(0).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(0).get(1).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(0).get(2).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(0).get(3).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(0).get(4).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(0).get(5).doubleValue(), 0.0, 0.0);

      assertEquals(centroids.get(1).get(0).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(1).get(1).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(1).get(2).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(1).get(3).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(1).get(4).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(1).get(5).doubleValue(), 1.0, 0.0);
    } else {
      assertEquals(centroids.get(0).get(0).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(0).get(1).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(0).get(2).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(0).get(3).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(0).get(4).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(0).get(5).doubleValue(), 1.0, 0.0);

      assertEquals(centroids.get(1).get(0).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(1).get(1).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(1).get(2).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(1).get(3).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(1).get(4).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(1).get(5).doubleValue(), 0.0, 0.0);
    }
  }

  @Test
  public void testMultiKmeans() throws Exception {
    String cexpr = "let(echo=true," +
        "               a=array(1,1,1,0,0,0)," +
        "               b=array(1,1,1,0,0,0)," +
        "               c=array(0,0,0,1,1,1)," +
        "               d=array(0,0,0,1,1,1)," +
        "               e=setRowLabels(matrix(a,b,c,d), " +
        "                              array(doc1, doc2, doc3, doc4))," +
        "               f=multiKmeans(e, 2, 5)," +
        "               g=getCluster(f, 0)," +
        "               h=getCluster(f, 1)," +
        "               i=getCentroids(f)," +
        "               j=getRowLabels(g)," +
        "               k=getRowLabels(h))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> cluster1 = (List<List<Number>>)tuples.get(0).get("g");
    List<List<Number>> cluster2 = (List<List<Number>>)tuples.get(0).get("h");
    List<List<Number>> centroids = (List<List<Number>>)tuples.get(0).get("i");
    List<String> labels1 = (List<String>)tuples.get(0).get("j");
    List<String> labels2 = (List<String>)tuples.get(0).get("k");

    assertEquals(cluster1.size(), 2);
    assertEquals(cluster2.size(), 2);
    assertEquals(centroids.size(), 2);

    //Assert that the docs are not in both clusters
    assertTrue(!(labels1.contains("doc1") && labels2.contains("doc1")));
    assertTrue(!(labels1.contains("doc2") && labels2.contains("doc2")));
    assertTrue(!(labels1.contains("doc3") && labels2.contains("doc3")));
    assertTrue(!(labels1.contains("doc4") && labels2.contains("doc4")));

    //Assert that (doc1 and doc2) or (doc3 and doc4) are in labels1
    assertTrue((labels1.contains("doc1") && labels1.contains("doc2")) ||
        ((labels1.contains("doc3") && labels1.contains("doc4"))));

    //Assert that (doc1 and doc2) or (doc3 and doc4) are in labels2
    assertTrue((labels2.contains("doc1") && labels2.contains("doc2")) ||
        ((labels2.contains("doc3") && labels2.contains("doc4"))));

    if(labels1.contains("doc1")) {
      assertEquals(centroids.get(0).get(0).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(0).get(1).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(0).get(2).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(0).get(3).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(0).get(4).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(0).get(5).doubleValue(), 0.0, 0.0);

      assertEquals(centroids.get(1).get(0).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(1).get(1).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(1).get(2).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(1).get(3).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(1).get(4).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(1).get(5).doubleValue(), 1.0, 0.0);
    } else {
      assertEquals(centroids.get(0).get(0).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(0).get(1).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(0).get(2).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(0).get(3).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(0).get(4).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(0).get(5).doubleValue(), 1.0, 0.0);

      assertEquals(centroids.get(1).get(0).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(1).get(1).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(1).get(2).doubleValue(), 1.0, 0.0);
      assertEquals(centroids.get(1).get(3).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(1).get(4).doubleValue(), 0.0, 0.0);
      assertEquals(centroids.get(1).get(5).doubleValue(), 0.0, 0.0);
    }
  }

  @Test
  public void testFuzzyKmeans() throws Exception {
    String cexpr = "let(echo=true," +
        "               a=array(1,1,1,0,0,0)," +
        "               b=array(1,1,1,0,0,0)," +
        "               c=array(0,0,0,1,1,1)," +
        "               d=array(0,0,0,1,1,1)," +
        "               e=setRowLabels(matrix(a,b,c,d), " +
        "                              array(doc1, doc2, doc3, doc4))," +
        "               f=fuzzyKmeans(e, 2)," +
        "               g=getCluster(f, 0)," +
        "               h=getCluster(f, 1)," +
        "               i=getCentroids(f)," +
        "               j=getRowLabels(g)," +
        "               k=getRowLabels(h)," +
        "               l=getMembershipMatrix(f))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> cluster1 = (List<List<Number>>)tuples.get(0).get("g");
    List<List<Number>> cluster2 = (List<List<Number>>)tuples.get(0).get("h");
    List<List<Number>> centroids = (List<List<Number>>)tuples.get(0).get("i");
    List<List<Number>> membership = (List<List<Number>>)tuples.get(0).get("l");

    List<String> labels1 = (List<String>)tuples.get(0).get("j");
    List<String> labels2 = (List<String>)tuples.get(0).get("k");

    assertEquals(cluster1.size(), 2);
    assertEquals(cluster2.size(), 2);
    assertEquals(centroids.size(), 2);

    //Assert that the docs are not in both clusters
    assertTrue(!(labels1.contains("doc1") && labels2.contains("doc1")));
    assertTrue(!(labels1.contains("doc2") && labels2.contains("doc2")));
    assertTrue(!(labels1.contains("doc3") && labels2.contains("doc3")));
    assertTrue(!(labels1.contains("doc4") && labels2.contains("doc4")));

    //Assert that (doc1 and doc2) or (doc3 and doc4) are in labels1
    assertTrue((labels1.contains("doc1") && labels1.contains("doc2")) ||
        ((labels1.contains("doc3") && labels1.contains("doc4"))));

    //Assert that (doc1 and doc2) or (doc3 and doc4) are in labels2
    assertTrue((labels2.contains("doc1") && labels2.contains("doc2")) ||
        ((labels2.contains("doc3") && labels2.contains("doc4"))));


    if(labels1.contains("doc1")) {
      assertEquals(centroids.get(0).get(0).doubleValue(), 1.0, 0.001);
      assertEquals(centroids.get(0).get(1).doubleValue(), 1.0, 0.001);
      assertEquals(centroids.get(0).get(2).doubleValue(), 1.0, 0.001);
      assertEquals(centroids.get(0).get(3).doubleValue(), 0.0, 0.001);
      assertEquals(centroids.get(0).get(4).doubleValue(), 0.0, 0.001);
      assertEquals(centroids.get(0).get(5).doubleValue(), 0.0, 0.001);

      assertEquals(centroids.get(1).get(0).doubleValue(), 0.0, 0.001);
      assertEquals(centroids.get(1).get(1).doubleValue(), 0.0, 0.001);
      assertEquals(centroids.get(1).get(2).doubleValue(), 0.0, 0.001);
      assertEquals(centroids.get(1).get(3).doubleValue(), 1.0, 0.001);
      assertEquals(centroids.get(1).get(4).doubleValue(), 1.0, 0.001);
      assertEquals(centroids.get(1).get(5).doubleValue(), 1.0, 0.001);

      //Assert the membership matrix
      assertEquals(membership.get(0).get(0).doubleValue(), 1.0, 0.001);
      assertEquals(membership.get(0).get(1).doubleValue(), 0.0, 0.001);
      assertEquals(membership.get(1).get(0).doubleValue(), 1.0, 0.001);
      assertEquals(membership.get(1).get(1).doubleValue(), 0.0, 0.001);
      assertEquals(membership.get(2).get(0).doubleValue(), 0.0, 0.001);
      assertEquals(membership.get(2).get(1).doubleValue(), 1.0, 0.001);
      assertEquals(membership.get(3).get(0).doubleValue(), 0.0, 0.001);
      assertEquals(membership.get(3).get(1).doubleValue(), 1.0, 0.001);

    } else {
      assertEquals(centroids.get(0).get(0).doubleValue(), 0.0, 0.001);
      assertEquals(centroids.get(0).get(1).doubleValue(), 0.0, 0.001);
      assertEquals(centroids.get(0).get(2).doubleValue(), 0.0, 0.001);
      assertEquals(centroids.get(0).get(3).doubleValue(), 1.0, 0.001);
      assertEquals(centroids.get(0).get(4).doubleValue(), 1.0, 0.001);
      assertEquals(centroids.get(0).get(5).doubleValue(), 1.0, 0.001);

      assertEquals(centroids.get(1).get(0).doubleValue(), 1.0, 0.001);
      assertEquals(centroids.get(1).get(1).doubleValue(), 1.0, 0.001);
      assertEquals(centroids.get(1).get(2).doubleValue(), 1.0, 0.001);
      assertEquals(centroids.get(1).get(3).doubleValue(), 0.0, 0.001);
      assertEquals(centroids.get(1).get(4).doubleValue(), 0.0, 0.001);
      assertEquals(centroids.get(1).get(5).doubleValue(), 0.0, 0.001);

      //Assert the membership matrix
      assertEquals(membership.get(0).get(0).doubleValue(), 0.0, 0.001);
      assertEquals(membership.get(0).get(1).doubleValue(), 1.0, 0.001);
      assertEquals(membership.get(1).get(0).doubleValue(), 0.0, 0.001);
      assertEquals(membership.get(1).get(1).doubleValue(), 1.0, 0.001);
      assertEquals(membership.get(2).get(0).doubleValue(), 1.0, 0.001);
      assertEquals(membership.get(2).get(1).doubleValue(), 0.0, 0.001);
      assertEquals(membership.get(3).get(0).doubleValue(), 1.0, 0.001);
      assertEquals(membership.get(3).get(1).doubleValue(), 0.0, 0.001);
    }
  }

  @Test
  public void testEbeMultiply() throws Exception {
    String cexpr = "ebeMultiply(array(2,4,6,8,10,12),array(1,2,3,4,5,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 6);
    assertTrue(out.get(0).intValue() == 2);
    assertTrue(out.get(1).intValue() == 8);
    assertTrue(out.get(2).intValue() == 18);
    assertTrue(out.get(3).intValue() == 32);
    assertTrue(out.get(4).intValue() == 50);
    assertTrue(out.get(5).intValue() == 72);
  }

  @Test
  public void testEbeAdd() throws Exception {
    String cexpr = "let(echo=true," +
        "               a=array(2, 4, 6, 8, 10, 12)," +
        "               b=array(1, 2, 3, 4, 5, 6)," +
        "               c=ebeAdd(a,b)," +
        "               d=array(10, 11, 12, 13, 14, 15)," +
        "               e=array(100, 200, 300, 400, 500, 600)," +
        "               f=matrix(a, b)," +
        "               g=matrix(d, e)," +
        "               h=ebeAdd(f, g))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("c");
    assertEquals(out.size(), 6);
    assertEquals(out.get(0).doubleValue(), 3.0, 0.0);
    assertEquals(out.get(1).doubleValue(), 6.0, 0.0);
    assertEquals(out.get(2).doubleValue(), 9.0, 0.0);
    assertEquals(out.get(3).doubleValue(), 12.0, 0.0);
    assertEquals(out.get(4).doubleValue(), 15.0, 0.0);
    assertEquals(out.get(5).doubleValue(), 18.0, 0.0);

    List<List<Number>> mout = (List<List<Number>>)tuples.get(0).get("h");
    assertEquals(mout.size(), 2);
    List<Number> row1 = mout.get(0);
    assertEquals(row1.size(), 6);
    assertEquals(row1.get(0).doubleValue(), 12.0, 0.0);
    assertEquals(row1.get(1).doubleValue(), 15.0, 0.0);
    assertEquals(row1.get(2).doubleValue(), 18.0, 0.0);
    assertEquals(row1.get(3).doubleValue(), 21.0, 0.0);
    assertEquals(row1.get(4).doubleValue(), 24.0, 0.0);
    assertEquals(row1.get(5).doubleValue(), 27.0, 0.0);

    List<Number> row2 = mout.get(1);
    assertEquals(row2.size(), 6);
    assertEquals(row2.get(0).doubleValue(), 101.0, 0.0);
    assertEquals(row2.get(1).doubleValue(), 202.0, 0.0);
    assertEquals(row2.get(2).doubleValue(), 303.0, 0.0);
    assertEquals(row2.get(3).doubleValue(), 404.0, 0.0);
    assertEquals(row2.get(4).doubleValue(), 505.0, 0.0);
    assertEquals(row2.get(5).doubleValue(), 606.0, 0.0);
  }

  @Test
  public void testEbeDivide() throws Exception {
    String cexpr = "ebeDivide(array(2,4,6,8,10,12),array(1,2,3,4,5,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 6);
    assertTrue(out.get(0).intValue() == 2);
    assertTrue(out.get(1).intValue() == 2);
    assertTrue(out.get(2).intValue() == 2);
    assertTrue(out.get(3).intValue() == 2);
    assertTrue(out.get(4).intValue() == 2);
    assertTrue(out.get(5).intValue() == 2);
  }

  @Test
  public void testFreqTable() throws Exception {
    String cexpr = "freqTable(array(2,4,6,8,10,12,12,4,8,8,8,2))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Map<String,Number>> out = (List<Map<String, Number>>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 6);
    Map<String, Number> bucket = out.get(0);
    assertEquals(bucket.get("value").longValue(), 2);
    assertEquals(bucket.get("count").longValue(), 2);

    bucket = out.get(1);
    assertEquals(bucket.get("value").longValue(), 4);
    assertEquals(bucket.get("count").longValue(), 2);

    bucket = out.get(2);
    assertEquals(bucket.get("value").longValue(), 6);
    assertEquals(bucket.get("count").longValue(), 1);

    bucket = out.get(3);
    assertEquals(bucket.get("value").longValue(), 8);
    assertEquals(bucket.get("count").longValue(), 4);

    bucket = out.get(4);
    assertEquals(bucket.get("value").longValue(), 10);
    assertEquals(bucket.get("count").longValue(), 1);

    bucket = out.get(5);
    assertEquals(bucket.get("value").longValue(), 12);
    assertEquals(bucket.get("count").longValue(), 2);
  }

  @Test
  public void testCosineSimilarity() throws Exception {
    String cexpr = "cosineSimilarity(array(2,4,6,8),array(1,1,3,4))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number cs = (Number)tuples.get(0).get("return-value");
    assertTrue(cs.doubleValue() == 0.9838197164968291);
  }

  @Test
  public void testPoissonDistribution() throws Exception {
    String cexpr = "let(a=poissonDistribution(100)," +
        "               b=sample(a, 10000)," +
        "               tuple(d=describe(b), " +
        "                     p=probability(a, 100), " +
        "                     c=cumulativeProbability(a, 100)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map map = (Map)tuples.get(0).get("d");
    Number mean = (Number)map.get("mean");
    Number var = (Number)map.get("var");
    //The mean and variance should be almost the same for poisson distribution
    assertEquals(mean.doubleValue(), var.doubleValue(), 7.0);
    Number prob = (Number)tuples.get(0).get("p");
    assertEquals(prob.doubleValue(), 0.03986099680914713, 0.0);
    Number cprob = (Number)tuples.get(0).get("c");
    assertEquals(cprob.doubleValue(), 0.5265621985303708, 0.0);
  }

  @Test
  public void testGeometricDistribution() throws Exception {
    String cexpr = "let(a=geometricDistribution(.2)," +
        "               b=geometricDistribution(.5)," +
        "               c=geometricDistribution(.8)," +
        "               d=sample(a, 10000)," +
        "               e=sample(b, 10000)," +
        "               f=sample(c, 10000)," +
        "               g=freqTable(d)," +
        "               h=freqTable(e)," +
        "               i=freqTable(f)," +
        "               tuple(g=g, h=h, i=i))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);

    List<Map> listg = (List<Map>)tuples.get(0).get("g");
    Map mapg = listg.get(0);
    double pctg = (double) mapg.get("pct");
    assertEquals(pctg, .2, .02);

    List<Map> listh = (List<Map>)tuples.get(0).get("h");
    Map maph = listh.get(0);
    double pcth = (double)maph.get("pct");
    assertEquals(pcth, .5, .02);

    List<Map> listi = (List<Map>)tuples.get(0).get("i");
    Map mapi = listi.get(0);
    double pcti = (double)mapi.get("pct");
    assertEquals(pcti, .8, .02);
  }

  @Test
  public void testBinomialDistribution() throws Exception {
    String cexpr = "let(a=binomialDistribution(100, .50)," +
        "               b=sample(a, 10000)," +
        "               tuple(d=describe(b), " +
        "                     p=probability(a, 50), " +
        "                     c=cumulativeProbability(a, 50)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number prob = (Number)tuples.get(0).get("p");
    assertEquals(prob.doubleValue(),0.07958923738717877, 0.0);
    Number cprob = (Number)tuples.get(0).get("c");
    assertEquals(cprob.doubleValue(), 0.5397946186935851, 0.0);
  }

  @Test
  public void testUniformIntegerDistribution() throws Exception {
    String cexpr = "let(a=uniformIntegerDistribution(1, 10)," +
        "               b=sample(a, 10000)," +
        "               tuple(d=describe(b), " +
        "                     p=probability(a, 5), " +
        "                     c=cumulativeProbability(a, 5)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map map = (Map)tuples.get(0).get("d");
    Number N = (Number)map.get("N");
    assertEquals(N.intValue(), 10000);
    Number prob = (Number)tuples.get(0).get("p");
    assertEquals(prob.doubleValue(), 0.1, 0.0);
    Number cprob = (Number)tuples.get(0).get("c");
    assertEquals(cprob.doubleValue(), 0.5, 0.0);
  }

  @Test
  public void testZipFDistribution() throws Exception {
    String cexpr = "let(a=sample(zipFDistribution(10, 1), 50000), b=freqTable(a), c=col(b, count))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> counts = (List<Number>)tuples.get(0).get("c");

    assertTrue(counts.size() == 10);

    int lastCount = Integer.MAX_VALUE;
    for(Number number : counts) {
      int current = number.intValue();
      if(current > lastCount) {
        throw new Exception("Zipf distribution not descending!!!");
      } else {
        lastCount = current;
      }
    }
  }

  @Test
  public void testBetaDistribution() throws Exception {
    String cexpr = "let(a=sample(betaDistribution(1, 5), 50000), b=hist(a, 11), c=col(b, N))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> counts = (List<Number>)tuples.get(0).get("c");

    int lastCount = Integer.MAX_VALUE;
    for(Number number : counts) {
      int current = number.intValue();
      if(current > lastCount) {
        throw new Exception("This beta distribution should be descending");
      } else {
        lastCount = current;
      }
    }

    cexpr = "let(a=sample(betaDistribution(5, 1), 50000), b=hist(a, 11), c=col(b, N))";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    counts = (List<Number>)tuples.get(0).get("c");

    lastCount = Integer.MIN_VALUE;
    for(Number number : counts) {
      int current = number.intValue();
      if(current < lastCount) {
        throw new Exception("This beta distribution should be ascending");
      } else {
        lastCount = current;
      }
    }
  }

  @Test
  public void testEnumeratedDistribution() throws Exception {
    String cexpr = "let(a=uniformIntegerDistribution(1, 10)," +
        "               b=sample(a, 10000)," +
        "               c=enumeratedDistribution(b),"+
        "               tuple(d=describe(b), " +
        "                     p=probability(c, 5), " +
        "                     c=cumulativeProbability(c, 5)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map map = (Map)tuples.get(0).get("d");
    Number N = (Number)map.get("N");
    assertEquals(N.intValue(), 10000);
    Number prob = (Number)tuples.get(0).get("p");
    assertEquals(prob.doubleValue(), 0.1, 0.07);
    Number cprob = (Number)tuples.get(0).get("c");
    assertEquals(cprob.doubleValue(), 0.5, 0.07);


    cexpr = "let(a=sample(enumeratedDistribution(array(1,2,3,4), array(40, 30, 20, 10)), 50000),"+
                "b=freqTable(a),"+
                "y=col(b, pct))";

    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> freqs = (List<Number>)tuples.get(0).get("y");
    assertEquals(freqs.get(0).doubleValue(), .40, .03);
    assertEquals(freqs.get(1).doubleValue(), .30, .03);
    assertEquals(freqs.get(2).doubleValue(), .20, .03);
    assertEquals(freqs.get(3).doubleValue(), .10, .03);
  }

  @Test
  public void testDotProduct() throws Exception {
    String cexpr = "dotProduct(array(2,4,6,8,10,12),array(1,2,3,4,5,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number dotProduct = (Number)tuples.get(0).get("return-value");
    assertTrue(dotProduct.doubleValue() == 182);
  }

  @Test
  public void testExponentialMovingAverage() throws Exception {
    String cexpr = "expMovingAvg(array(22.27, 22.19, 22.08, 22.17, 22.18, 22.13, 22.23, 22.43, 22.24, 22.29, " +
                   "22.15, 22.39, 22.38, 22.61, 23.36, 24.05, 23.75, 23.83, 23.95, 23.63, 23.82, 23.87, 23.65, 23.19,"+
                   "23.10, 23.33, 22.68, 23.10, 22.40, 22.17), 10)";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 21);
    assertEquals((double) out.get(0), 22.22, 0.009);
    assertEquals((double) out.get(1), 22.21, 0.009);
    assertEquals((double)out.get(2), 22.24, 0.009);
    assertEquals((double)out.get(3), 22.27, 0.009);
    assertEquals((double)out.get(4), 22.33, 0.009);
    assertEquals((double)out.get(5), 22.52, 0.009);
    assertEquals((double)out.get(6), 22.80, 0.009);
    assertEquals((double)out.get(7), 22.97, 0.009);
    assertEquals((double)out.get(8), 23.13, 0.009);
    assertEquals((double)out.get(9), 23.28, 0.009);
    assertEquals((double)out.get(10), 23.34, 0.009);
    assertEquals((double)out.get(11), 23.43, 0.009);
    assertEquals((double)out.get(12), 23.51, 0.009);
    assertEquals((double)out.get(13), 23.54, 0.009);
    assertEquals((double)out.get(14), 23.47, 0.009);
    assertEquals((double)out.get(15), 23.40, 0.009);
    assertEquals((double)out.get(16), 23.39, 0.009);
    assertEquals((double)out.get(17), 23.26, 0.009);
    assertEquals((double)out.get(18), 23.23, 0.009);
    assertEquals((double)out.get(19), 23.08, 0.009);
    assertEquals((double)out.get(20), 22.92, 0.009);
  }

  @Test
  public void testTimeDifferencingDefaultLag() throws Exception {
    String cexpr = "diff(array(1709.0, 1621.0, 1973.0, 1812.0, 1975.0, 1862.0, 1940.0, 2013.0, 1596.0, 1725.0, 1676.0, 1814.0, 1615.0, 1557.0, 1891.0, 1956.0, 1885.0, 1623.0, 1903.0, 1997.0))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 19);
    assertEquals(out.get(0).doubleValue(),-88.0, 0.01);
    assertEquals(out.get(1).doubleValue(),352.0, 0.01);
    assertEquals(out.get(2).doubleValue(),-161.0, 0.01);
    assertEquals(out.get(3).doubleValue(),163.0, 0.01);
    assertEquals(out.get(4).doubleValue(),-113.0, 0.01);
    assertEquals(out.get(5).doubleValue(),78.0, 0.01);
    assertEquals(out.get(6).doubleValue(),73.0, 0.01);
    assertEquals(out.get(7).doubleValue(),-417.0, 0.01);
    assertEquals(out.get(8).doubleValue(),129.0, 0.01);
    assertEquals(out.get(9).doubleValue(),-49.0, 0.01);
    assertEquals(out.get(10).doubleValue(),138.0, 0.01);
    assertEquals(out.get(11).doubleValue(),-199.0, 0.01);
    assertEquals(out.get(12).doubleValue(),-58.0, 0.01);
    assertEquals(out.get(13).doubleValue(),334.0, 0.01);
    assertEquals(out.get(14).doubleValue(),65.0, 0.01);
    assertEquals(out.get(15).doubleValue(),-71.0, 0.01);
    assertEquals(out.get(16).doubleValue(),-262.0, 0.01);
    assertEquals(out.get(17).doubleValue(),280.0, 0.01);
    assertEquals(out.get(18).doubleValue(),94.0, 0.01);
  }

  @Test
  public void testTimeDifferencingDefinedLag() throws Exception {
    String cexpr = "diff(array(1709.0, 1621.0, 1973.0, 1812.0, 1975.0, 1862.0, 1940.0, 2013.0, 1596.0, 1725.0, 1676.0, 1814.0, 1615.0, 1557.0, 1891.0, 1956.0, 1885.0, 1623.0, 1903.0, 1997.0), 12)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 8);
    assertEquals(out.get(0).doubleValue(), -94.0, 0.01);
    assertEquals(out.get(1).doubleValue(),-64.0, 0.01);
    assertEquals(out.get(2).doubleValue(),-82.0, 0.01);
    assertEquals(out.get(3).doubleValue(),144.0, 0.01);
    assertEquals(out.get(4).doubleValue(),-90.0, 0.01);
    assertEquals(out.get(5).doubleValue(),-239.0, 0.01);
    assertEquals(out.get(6).doubleValue(),-37.0, 0.01);
    assertEquals(out.get(7).doubleValue(),-16.0, 0.01);
  }

  @Test
  public void testNestedDoubleTimeDifference() throws Exception {
    String cexpr = "diff(diff(array(1709.0, 1621.0, 1973.0, 1812.0, 1975.0, 1862.0, 1940.0, 2013.0, 1596.0, 1725.0, 1676.0, 1814.0, 1615.0, 1557.0, 1891.0, 1956.0, 1885.0, 1623.0, 1903.0, 1997.0)), 12)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 7);
    assertEquals(out.get(0).doubleValue(), 30.0, 0.01);
    assertEquals(out.get(1).doubleValue(), -18.0, 0.01);
    assertEquals(out.get(2).doubleValue(), 226.0, 0.01);
    assertEquals(out.get(3).doubleValue(), -234.0, 0.01);
    assertEquals(out.get(4).doubleValue(), -149.0, 0.01);
    assertEquals(out.get(5).doubleValue(), 202.0, 0.01);
    assertEquals(out.get(6).doubleValue(), 21.0, 0.01);
  }

  @Test
  public void testPolyfit() throws Exception {
    String cexpr = "let(echo=true," +
                   "    a=array(0,1,2,3,4,5,6,7)," +
                   "    fit=polyfit(a, 1))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("fit");
    assertTrue(out.size() == 8);
    assertTrue(out.get(0).intValue() == 0);
    assertTrue(out.get(1).intValue() == 1);
    assertTrue(out.get(2).intValue() == 2);
    assertTrue(out.get(3).intValue() == 3);
    assertTrue(out.get(4).intValue() == 4);
    assertTrue(out.get(5).intValue() == 5);
    assertTrue(out.get(6).intValue() == 6);
    assertTrue(out.get(7).intValue() == 7);
  }

  @Test
  public void testTtest() throws Exception {
    String cexpr = "let(echo=true," +
                       "a=array(0,1,2,3,4,5,6,7,9,10,11,12), " +
                       "b=array(0,1,2,3,4,5,6,7,1,1,1,1), " +
                       "ttest=ttest(a, b)," +
                       "sample2Mean=mean(b),"+
                       "onesamplettest=ttest(sample2Mean, b)," +
                       "pairedttest=pairedTtest(a,b))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map testResult = (Map)tuples.get(0).get("ttest");
    Number tstat = (Number)testResult.get("t-statistic");
    Number pval = (Number)testResult.get("p-value");
    assertEquals(tstat.doubleValue(), 2.3666107120397575, .0001);
    assertEquals(pval.doubleValue(), 0.029680704317867967, .0001);

    Map testResult2 = (Map)tuples.get(0).get("onesamplettest");
    Number tstat2 = (Number)testResult2.get("t-statistic");
    Number pval2 = (Number)testResult2.get("p-value");
    assertEquals(tstat2.doubleValue(), 0, .0001);
    assertEquals(pval2.doubleValue(), 1, .0001);

    Map testResult3 = (Map)tuples.get(0).get("pairedttest");
    Number tstat3 = (Number)testResult3.get("t-statistic");
    Number pval3 = (Number)testResult3.get("p-value");
    assertEquals(tstat3.doubleValue(), 2.321219442769799, .0001);
    assertEquals(pval3.doubleValue(), 0.0404907407662755, .0001);
  }

  @Test
  public void testChiSquareDataSet() throws Exception {
    String cexpr = "let(echo=true," +
                   "    a=array(1,1,2,3,4,5,6,7,9,10,11,12), " +
                   "    b=array(1,1,2,3,4,5,6,7,1,1,1,1), " +
                   "    chisquare=chiSquareDataSet(a, b))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map testResult = (Map)tuples.get(0).get("chisquare");
    Number tstat = (Number)testResult.get("chisquare-statistic");
    Number pval = (Number)testResult.get("p-value");
    assertEquals(tstat.doubleValue(), 20.219464814599252, .0001);
    assertEquals(pval.doubleValue(), 0.04242018685334226, .0001);
  }

  @Test
  public void testGtestDataSet() throws Exception {
    String cexpr = "let(echo=true," +
        "    a=array(1,1,2,3,4,5,6,7,9,10,11,12), " +
        "    b=array(1,1,2,3,4,5,6,7,1,1,1,1), " +
        "    gtest=gtestDataSet(a, b))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map testResult = (Map)tuples.get(0).get("gtest");
    Number gstat = (Number)testResult.get("G-statistic");
    Number pval = (Number)testResult.get("p-value");
    assertEquals(gstat.doubleValue(), 22.41955157784917, .0001);
    assertEquals(pval.doubleValue(), 0.021317102314826752, .0001);
  }

  @Test
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") //2018-03-10
  public void testMultiVariateNormalDistribution() throws Exception {
    String cexpr = "let(echo=true," +
        "     a=array(1,2,3,4,5,6,7)," +
        "     b=array(100, 110, 120, 130,140,150,180)," +
        "     c=transpose(matrix(a, b))," +
        "     d=array(mean(a), mean(b))," +
        "     e=cov(c)," +
        "     f=multiVariateNormalDistribution(d, e)," +
        "     g=sample(f, 10000)," +
        "     h=cov(g)," +
        "     i=sample(f)," +
        "     j=density(f, array(4.016093243274465, 138.7283428008585)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> cov = (List<List<Number>>)tuples.get(0).get("h");
    assertEquals(cov.size(), 2);
    List<Number> row1 = cov.get(0);
    assertEquals(row1.size(), 2);

    double a = row1.get(0).doubleValue();
    double b = row1.get(1).doubleValue();
    assertEquals(a, 4.666666666666667, 2.5);
    assertEquals(b, 56.66666666666667, 7);

    List<Number> row2 = cov.get(1);

    double c = row2.get(0).doubleValue();
    double d = row2.get(1).doubleValue();
    assertEquals(c, 56.66666666666667, 7);
    assertEquals(d, 723.8095238095239, 50);

    List<Number> sample = (List<Number>)tuples.get(0).get("i");
    assertEquals(sample.size(), 2);
    Number sample1 = sample.get(0);
    Number sample2 = sample.get(1);
    assertTrue(sample1.doubleValue() > -30 && sample1.doubleValue() < 30);
    assertTrue(sample2.doubleValue() > 50 && sample2.doubleValue() < 250);

    Number density = (Number)tuples.get(0).get("j");
    assertEquals(density.doubleValue(), 0.007852638121596995, .00001);
  }

  @Test
  public void testKnn() throws Exception {
    String cexpr = "let(echo=true," +
        "               a=setRowLabels(matrix(array(1,1,1,0,0,0),"+
        "                                     array(1,0,0,0,1,1),"+
        "                                     array(0,0,0,1,1,1)), array(row1,row2,row3)),"+
        "               b=array(0,0,0,1,1,1),"+
        "               c=knn(a, b, 2),"+
        "               d=getRowLabels(c),"+
        "               e=getAttributes(c)," +
        "               f=knn(a, b, 2, distance=manhattan)," +
        "               g=getAttributes(f))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);

    List<List<Number>> knnMatrix = (List<List<Number>>)tuples.get(0).get("c");
    assertEquals(knnMatrix.size(), 2);

    List<Number> row1 = knnMatrix.get(0);
    assertEquals(row1.size(), 6);
    assertEquals(row1.get(0).doubleValue(), 0.0, 0.0);
    assertEquals(row1.get(1).doubleValue(), 0.0, 0.0);
    assertEquals(row1.get(2).doubleValue(), 0.0, 0.0);
    assertEquals(row1.get(3).doubleValue(), 1.0, 0.0);
    assertEquals(row1.get(4).doubleValue(), 1.0, 0.0);
    assertEquals(row1.get(5).doubleValue(), 1.0, 0.0);

    List<Number> row2 = knnMatrix.get(1);
    assertEquals(row2.size(), 6);

    assertEquals(row2.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(row2.get(1).doubleValue(), 0.0, 0.0);
    assertEquals(row2.get(2).doubleValue(), 0.0, 0.0);
    assertEquals(row2.get(3).doubleValue(), 0.0, 0.0);
    assertEquals(row2.get(4).doubleValue(), 1.0, 0.0);
    assertEquals(row2.get(5).doubleValue(), 1.0, 0.0);

    Map atts = (Map)tuples.get(0).get("e");
    List<Number> dists = (List<Number>)atts.get("distances");
    assertEquals(dists.size(), 2);
    assertEquals(dists.get(0).doubleValue(), 0.0, 0.0);
    assertEquals(dists.get(1).doubleValue(), 1.4142135623730951, 0.0);

    List<String> rowLabels = (List<String>)tuples.get(0).get("d");
    assertEquals(rowLabels.size(), 2);
    assertEquals(rowLabels.get(0), "row3");
    assertEquals(rowLabels.get(1), "row2");

    atts = (Map)tuples.get(0).get("g");
    dists = (List<Number>)atts.get("distances");
    assertEquals(dists.size(), 2);
    assertEquals(dists.get(0).doubleValue(), 0.0, 0.0);
    assertEquals(dists.get(1).doubleValue(), 2.0, 0.0);
  }

  @Test
  public void testIntegrate() throws Exception {
    String cexpr = "let(echo=true, " +
                       "a=sequence(50, 1, 0), " +
                       "b=spline(a), " +
                       "c=integrate(b, 0, 49), " +
                       "d=integrate(b, 0, 20), " +
                       "e=integrate(b, 20, 49))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number integral = (Number)tuples.get(0).get("c");
    assertEquals(integral.doubleValue(), 49, 0.0);
    integral = (Number)tuples.get(0).get("d");
    assertEquals(integral.doubleValue(), 20, 0.0);
    integral = (Number)tuples.get(0).get("e");
    assertEquals(integral.doubleValue(), 29, 0.0);
  }

  @Test
  public void testLoess() throws Exception {
    String cexpr = "let(echo=true," +
                   "    a=array(0,1,2,3,4,5,6,7)," +
                   "    fit=loess(a), " +
                   "    der=derivative(fit))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("fit");
    assertTrue(out.size() == 8);
    assertEquals(out.get(0).doubleValue(), 0.0, 0.0);
    assertEquals(out.get(1).doubleValue(), 1.0, 0.0);
    assertEquals(out.get(2).doubleValue(), 2.0, 0.0);
    assertEquals(out.get(3).doubleValue(), 3.0, 0.0);
    assertEquals(out.get(4).doubleValue(), 4.0, 0.0);
    assertEquals(out.get(5).doubleValue(), 5.0, 0.0);
    assertEquals(out.get(6).doubleValue(), 6.0, 0.0);
    assertEquals(out.get(7).doubleValue(), 7.0, 0.0);

    List<Number> out1 = (List<Number>)tuples.get(0).get("der");
    assertTrue(out1.size() == 8);
    assertEquals(out1.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(1).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(2).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(3).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(4).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(5).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(6).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(7).doubleValue(), 1.0, 0.0);

  }

  @Test
  public void testSpline() throws Exception {
    String cexpr = "let(echo=true," +
                   "    a=array(0,1,2,3,4,5,6,7), " +
                   "    b=array(1,70,90,10,78, 100, 1, 9)," +
                   "    fit=spline(a, b), " +
                   "    der=derivative(fit))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("fit");
    assertTrue(out.size() == 8);
    assertEquals(out.get(0).doubleValue(), 1.0, 0.0001);
    assertEquals(out.get(1).doubleValue(), 70.0, 0.0001);
    assertEquals(out.get(2).doubleValue(), 90.0, 0.0001);
    assertEquals(out.get(3).doubleValue(), 10.0, 0.0001);
    assertEquals(out.get(4).doubleValue(), 78.0, 0.0001);
    assertEquals(out.get(5).doubleValue(), 100.0, 0.0001);
    assertEquals(out.get(6).doubleValue(), 1.0, 0.0001);
    assertEquals(out.get(7).doubleValue(), 9.0, 0.0001);

    List<Number> out1 = (List<Number>)tuples.get(0).get("der");

    assertTrue(out1.size() == 8);
    assertEquals(out1.get(0).doubleValue(), 72.06870491240123, 0.0001);
    assertEquals(out1.get(1).doubleValue(), 62.86259017519753, 0.0001);
    assertEquals(out1.get(2).doubleValue(),-56.519065613191344, 0.0001);
    assertEquals(out1.get(3).doubleValue(), -16.786327722432148, 0.0001);
    assertEquals(out1.get(4).doubleValue(), 87.66437650291996, 0.0001);
    assertEquals(out1.get(5).doubleValue(), -63.87117828924769, 0.0001);
    assertEquals(out1.get(6).doubleValue(), -63.17966334592923, 0.0001);
    assertEquals(out1.get(7).doubleValue(), 43.58983167296462, 0.0001);
  }

  @Test
  public void testBicubicSpline() throws Exception {
    String cexpr = "let(echo=true," +
        "               a=array(300, 400, 500, 600, 700), " +
        "               b=array(340, 410, 495, 590, 640)," +
        "               c=array(600, 395, 550, 510, 705),"+
        "               d=array(500, 420, 510, 601, 690),"+
        "               e=array(420, 411, 511, 611, 711),"+
        "               f=matrix(a, b, c, d, e),"+
        "               x=array(1,2,3,4,5),"+
        "               y=array(100, 200, 300, 400, 500),"+
        "               bspline=bicubicSpline(x, y, f), " +
        "               p1=predict(bspline, 1.5, 250)," +
        "               p2=predict(bspline, 3.5, 350)," +
        "               p3=predict(bspline, 4.5, 450)," +
        "               p4=predict(bspline,matrix(array(1.5, 250), array(3.5, 350), array(4.5, 450))))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number p1 = (Number)tuples.get(0).get("p1");
    assertEquals(p1.doubleValue(), 449.7837701612903, 0.0);
    Number p2 = (Number)tuples.get(0).get("p2");
    assertEquals(p2.doubleValue(), 536.8916383774491, 0.0);
    Number p3 = (Number)tuples.get(0).get("p3");
    assertEquals(p3.doubleValue(), 659.921875, 0.0);
    List<Number> p4 = (List<Number>)tuples.get(0).get("p4");
    assertEquals(p4.get(0).doubleValue(), 449.7837701612903, 0.0);
    assertEquals(p4.get(1).doubleValue(), 536.8916383774491, 0.0);
    assertEquals(p4.get(2).doubleValue(), 659.921875, 0.0);
  }

  @Test
  public void testAkima() throws Exception {
    String cexpr = "let(echo=true," +
        "    a=array(0,1,2,3,4,5,6,7), " +
        "    b=array(1,70,90,10,78, 100, 1, 9)," +
        "    fit=akima(a, b), " +
        "    der=derivative(fit))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("fit");
    assertTrue(out.size() == 8);
    assertEquals(out.get(0).doubleValue(), 1.0, 0.0001);
    assertEquals(out.get(1).doubleValue(), 70.0, 0.0001);
    assertEquals(out.get(2).doubleValue(), 90.0, 0.0001);
    assertEquals(out.get(3).doubleValue(), 10.0, 0.0001);
    assertEquals(out.get(4).doubleValue(), 78.0, 0.0001);
    assertEquals(out.get(5).doubleValue(), 100.0, 0.0001);
    assertEquals(out.get(6).doubleValue(), 1.0, 0.0001);
    assertEquals(out.get(7).doubleValue(), 9.0, 0.0001);

    List<Number> out1 = (List<Number>)tuples.get(0).get("der");
    assertTrue(out1.size() == 8);
    assertEquals(out1.get(0).doubleValue(), 93.5, 0.0001);
    assertEquals(out1.get(1).doubleValue(), 44.5, 0.0001);
    assertEquals(out1.get(2).doubleValue(), -4.873096446700508, 0.0001);
    assertEquals(out1.get(3).doubleValue(), 21.36986301369863, 0.0001);
    assertEquals(out1.get(4).doubleValue(), 42.69144981412639, 0.0001);
    assertEquals(out1.get(5).doubleValue(), -14.379084967320262, 0.0001);
    assertEquals(out1.get(6).doubleValue(), -45.5, 0.0001);
    assertEquals(out1.get(7).doubleValue(), 61.5, 0.0001);
  }

  @Test
  public void testLerp() throws Exception {
    String cexpr = "let(echo=true," +
        "    a=array(0,1,2,3,4,5,6,7), " +
        "    b=array(1,70,90,10,78, 100, 1, 9)," +
        "    fit=lerp(a, b), " +
        "    der=derivative(fit))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("fit");
    assertTrue(out.size() == 8);
    assertEquals(out.get(0).doubleValue(), 1.0, 0.0001);
    assertEquals(out.get(1).doubleValue(), 70.0, 0.0001);
    assertEquals(out.get(2).doubleValue(), 90.0, 0.0001);
    assertEquals(out.get(3).doubleValue(), 10.0, 0.0001);
    assertEquals(out.get(4).doubleValue(), 78.0, 0.0001);
    assertEquals(out.get(5).doubleValue(), 100.0, 0.0001);
    assertEquals(out.get(6).doubleValue(), 1.0, 0.0001);
    assertEquals(out.get(7).doubleValue(), 9.0, 0.0001);

    List<Number> out1 = (List<Number>)tuples.get(0).get("der");
    assertTrue(out1.size() == 8);
    assertEquals(out1.get(0).doubleValue(), 69.0, 0.0001);
    assertEquals(out1.get(1).doubleValue(), 20.0, 0.0001);
    assertEquals(out1.get(2).doubleValue(),-80.0, 0.0001);
    assertEquals(out1.get(3).doubleValue(), 68, 0.0001);
    assertEquals(out1.get(4).doubleValue(), 22.0, 0.0001);
    assertEquals(out1.get(5).doubleValue(),-99.0, 0.0001);
    assertEquals(out1.get(6).doubleValue(), 8.0, 0.0001);
    assertEquals(out1.get(7).doubleValue(), 8.0, 0.0001);
  }

  @Test
  public void testHarmonicFit() throws Exception {
    String cexpr = "let(a=sin(sequence(100, 1, 6)), b=harmonicFit(a), s=ebeSubtract(a, b))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("s");
    assertTrue(out.size() == 100);
    for(Number n : out) {
      assertEquals(n.doubleValue(), 0.0, .01);
    }
  }

  @Test
  public void testAnova() throws Exception {
    String cexpr = "anova(array(1,2,3,5,4,6), array(5,2,3,5,4,6), array(1,2,7,5,4,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map out = (Map)tuples.get(0).get("return-value");
    assertEquals((double) out.get("p-value"), 0.788298D, .0001);
    assertEquals((double) out.get("f-ratio"), 0.24169D, .0001);
  }

  @Test
  public void testOlsRegress() throws Exception {
    String cexpr = "let(echo=true, a=array(8.5, 12.89999962, 5.199999809, 10.69999981, 3.099999905, 3.5, 9.199999809, 9, 15.10000038, 10.19999981), " +
                       "b=array(5.099999905, 5.800000191, 2.099999905, 8.399998665, 2.900000095, 1.200000048, 3.700000048, 7.599999905, 7.699999809, 4.5)," +
                       "c=array(4.699999809, 8.800000191, 15.10000038, 12.19999981, 10.60000038, 3.5, 9.699999809, 5.900000095, 20.79999924, 7.900000095)," +
                       "d=array(85.09999847, 106.3000031, 50.20000076, 130.6000061, 54.79999924, 30.29999924, 79.40000153, 91, 135.3999939, 89.30000305)," +
                       "e=transpose(matrix(a, b, c))," +
                       "f=olsRegress(e, d)," +
                       "g=predict(f, e))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map regression = (Map)tuples.get(0).get("f");

    Number rsquared = (Number)regression.get("RSquared");

    assertEquals(rsquared.doubleValue(), 0.9667887860584002, .000001);

    List<Number> regressionParameters = (List<Number>)regression.get("regressionParameters");

    assertEquals(regressionParameters.get(0).doubleValue(), 7.676028542255028, .0001);
    assertEquals(regressionParameters.get(1).doubleValue(), 3.661604009261836, .0001);
    assertEquals(regressionParameters.get(2).doubleValue(), 7.621051256504592, .0001);
    assertEquals(regressionParameters.get(3).doubleValue(), 0.8284680662898674, .0001);

    List<Number> predictions = (List<Number>)tuples.get(0).get("g");

    assertEquals(predictions.get(0).doubleValue(), 81.56082305847914, .0001);
    assertEquals(predictions.get(1).doubleValue(), 106.40333675525883, .0001);
    assertEquals(predictions.get(2).doubleValue(), 55.23044372150484, .0001);
    assertEquals(predictions.get(3).doubleValue(), 120.97932137751451, .0001);
    assertEquals(predictions.get(4).doubleValue(), 49.90981180846799, .0001);
    assertEquals(predictions.get(5).doubleValue(), 32.53654268030196, .0001);
    assertEquals(predictions.get(6).doubleValue(), 77.59681482774931, .0001);
    assertEquals(predictions.get(7).doubleValue(), 103.43841512086125, .0001);
    assertEquals(predictions.get(8).doubleValue(), 138.88047884217636, .0001);
    assertEquals(predictions.get(9).doubleValue(), 85.86401719768607, .0001);
  }

  @Test
  public void testPlot() throws Exception {
    String cexpr = "let(a=array(3,2,3), plot(type=scatter, x=a, y=array(5,6,3)))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    String plot = tuples.get(0).getString("plot");
    assertTrue(plot.equals("scatter"));
    List<List<Number>> data = (List<List<Number>>)tuples.get(0).get("data");
    assertTrue(data.size() == 3);
    List<Number> pair1 = data.get(0);
    assertTrue(pair1.get(0).intValue() == 3);
    assertTrue(pair1.get(1).intValue() == 5);
    List<Number> pair2 = data.get(1);
    assertTrue(pair2.get(0).intValue() == 2);
    assertTrue(pair2.get(1).intValue() == 6);
    List<Number> pair3 = data.get(2);
    assertTrue(pair3.get(0).intValue() == 3);
    assertTrue(pair3.get(1).intValue() == 3);
  }

  @Test
  public void testMovingAverage() throws Exception {
    String cexpr = "movingAvg(array(1,2,3,4,5,6,7), 4)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size()==4);
    assertEquals((double) out.get(0), 2.5, .0);
    assertEquals((double) out.get(1), 3.5, .0);
    assertEquals((double) out.get(2), 4.5, .0);
    assertEquals((double) out.get(3), 5.5, .0);
  }

  @Test
  public void testMannWhitney() throws Exception {
    String cexpr = "mannWhitney(array(0.15,0.10,0.11,0.24,0.08,0.08,0.10,0.10,0.10,0.12,0.04,0.07), " +
                               "array(0.10,0.20,0.30,0.10,0.10,0.02,0.05,0.07))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map out = (Map)tuples.get(0).get("return-value");
    assertEquals((double) out.get("u-statistic"), 52.5, .1);
    assertEquals((double) out.get("p-value"), 0.7284, .001);
  }

  @Test
  public void testMovingMedian() throws Exception {
    String cexpr = "movingMedian(array(1,2,6,9,10,12,15), 5)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 3);
    assertEquals(out.get(0).doubleValue(), 6.0, .0);
    assertEquals(out.get(1).doubleValue(), 9.0, .0);
    assertEquals(out.get(2).doubleValue(), 10.0, .0);
  }

  @Test
  public void testSumSq() throws Exception {
    String cexpr = "sumSq(array(-3,-2.5, 10))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number sumSq = (Number)tuples.get(0).get("return-value");
    assertEquals(sumSq.doubleValue(), 115.25D, 0.0D);
  }

  @Test
  public void testMonteCarlo() throws Exception {
    String cexpr = "let(a=constantDistribution(10), b=constantDistribution(20), c=monteCarlo(add(sample(a), sample(b)), 10))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("c");
    assertTrue(out.size()==10);
    assertEquals(out.get(0).doubleValue(), 30.0, .0);
    assertEquals(out.get(1).doubleValue(), 30.0, .0);
    assertEquals(out.get(2).doubleValue(), 30.0, .0);
    assertEquals(out.get(3).doubleValue(), 30.0, .0);
    assertEquals(out.get(4).doubleValue(), 30.0, .0);
    assertEquals(out.get(5).doubleValue(), 30.0, .0);
    assertEquals(out.get(6).doubleValue(), 30.0, .0);
    assertEquals(out.get(7).doubleValue(), 30.0, .0);
    assertEquals(out.get(8).doubleValue(), 30.0, .0);
    assertEquals(out.get(9).doubleValue(), 30.0, .0);
  }

  @Test
  public void testWeibullDistribution() throws Exception {
    String cexpr = "let(echo=true, " +
                       "a=describe(sample(weibullDistribution(.1, 10),10000)), " +
                       "b=describe(sample(weibullDistribution(.5, 10),10000)), " +
                       "c=describe(sample(weibullDistribution(1, 10),10000))," +
                       "d=describe(sample(weibullDistribution(6, 10),10000))," +
                       "e=mean(sample(weibullDistribution(1, 10),10000))," +
                       "f=mean(sample(weibullDistribution(1, 20),10000))," +
                       "g=mean(sample(weibullDistribution(1, 30),10000)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map a = (Map)tuples.get(0).get("a");
    Map b = (Map)tuples.get(0).get("b");
    Map c = (Map)tuples.get(0).get("c");
    Map d = (Map)tuples.get(0).get("d");

    Number sa = (Number)a.get("skewness");
    Number sb = (Number)b.get("skewness");
    Number sc = (Number)c.get("skewness");
    Number sd = (Number)d.get("skewness");

    //Test shape change
    assertTrue(sa.doubleValue() > sb.doubleValue());
    assertTrue(sb.doubleValue() > sc.doubleValue());
    assertTrue(sc.doubleValue() > sd.doubleValue());
    assertTrue(sd.doubleValue() < 0.0);

    //Test scale change

    Number e = (Number)tuples.get(0).get("e");
    Number f = (Number)tuples.get(0).get("f");
    Number g = (Number)tuples.get(0).get("g");

    assertTrue(e.doubleValue() < f.doubleValue());
    assertTrue(f.doubleValue() < g.doubleValue());
  }

  @Test
  public void testGammaDistribution() throws Exception {
    String cexpr = "#comment\nlet(echo=true, " +
        "a=describe(sample(gammaDistribution(1, 10),10000)), " +
        "\n# commment\n"+
        "b=describe(sample(gammaDistribution(3, 10),10000)), " +
        "c=describe(sample(gammaDistribution(5, 10),10000))," +
        "d=describe(sample(gammaDistribution(7, 10),10000))," +
        "e=mean(sample(gammaDistribution(1, 10),10000))," +
        "f=mean(sample(gammaDistribution(1, 20),10000))," +
        "g=mean(sample(gammaDistribution(1, 30),10000)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map a = (Map)tuples.get(0).get("a");
    Map b = (Map)tuples.get(0).get("b");
    Map c = (Map)tuples.get(0).get("c");
    Map d = (Map)tuples.get(0).get("d");

    Number sa = (Number)a.get("skewness");
    Number sb = (Number)b.get("skewness");
    Number sc = (Number)c.get("skewness");
    Number sd = (Number)d.get("skewness");

    //Test shape change
    assertTrue(sa.doubleValue() > sb.doubleValue());
    assertTrue(sb.doubleValue() > sc.doubleValue());
    assertTrue(sc.doubleValue() > sd.doubleValue());

    //Test scale change

    Number e = (Number)tuples.get(0).get("e");
    Number f = (Number)tuples.get(0).get("f");
    Number g = (Number)tuples.get(0).get("g");

    assertTrue(e.doubleValue() < f.doubleValue());
    assertTrue(f.doubleValue() < g.doubleValue());
  }

  @Test
  public void testLogNormalDistribution() throws Exception {
    String cexpr = "let(echo=true, " +
        "a=describe(sample(logNormalDistribution(.1, 0),10000)), " +
        "b=describe(sample(logNormalDistribution(.3, 0),10000)), " +
        "c=describe(sample(logNormalDistribution(.6, 0),10000))," +
        "d=mean(sample(logNormalDistribution(.3, 0),10000)), " +
        "e=mean(sample(logNormalDistribution(.3, 2),10000)), " +
        ")";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map a = (Map)tuples.get(0).get("a");
    Map b = (Map)tuples.get(0).get("b");
    Map c = (Map)tuples.get(0).get("c");

    Number sa = (Number)a.get("skewness");
    Number sb = (Number)b.get("skewness");
    Number sc = (Number)c.get("skewness");

    assertTrue(sa.doubleValue() < sb.doubleValue());
    assertTrue(sb.doubleValue() < sc.doubleValue());

    Number d = (Number)tuples.get(0).get("d");
    Number e = (Number)tuples.get(0).get("e");

    assertTrue(d.doubleValue() < e.doubleValue());

  }

  @Test
  public void testTriangularDistribution() throws Exception {
    String cexpr = "let(echo=true, " +
        "a=describe(sample(triangularDistribution(10, 15, 30),10000)), " +
        "b=describe(sample(triangularDistribution(10, 25, 30),10000)), " +
        ")";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map a = (Map)tuples.get(0).get("a");
    Map b = (Map)tuples.get(0).get("b");

    Number sa = (Number)a.get("skewness");
    Number sb = (Number)b.get("skewness");

    Number mina = (Number)a.get("min");
    Number maxa = (Number)a.get("max");

    assertTrue(sa.doubleValue() > 0);
    assertTrue(sb.doubleValue() < 0);
    assertEquals(mina.doubleValue(), 10, .5);
    assertEquals(maxa.doubleValue(), 30, .5);
  }

  @Test
  public void testCovMatrix() throws Exception {
    String cexpr = "let(a=array(1,2,3), b=array(2,4,6), c=array(4, 8, 12), d=transpose(matrix(a, b, c)), f=cov(d))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> cm = (List<List<Number>>)tuples.get(0).get("f");
    assertEquals(cm.size(), 3);
    List<Number> row1 = cm.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).longValue(), 1);
    assertEquals(row1.get(1).longValue(), 2);
    assertEquals(row1.get(2).longValue(), 4);

    List<Number> row2 = cm.get(1);
    assertEquals(row2.size(), 3);
    assertEquals(row2.get(0).longValue(), 2);
    assertEquals(row2.get(1).longValue(), 4);
    assertEquals(row2.get(2).longValue(), 8);

    List<Number> row3 = cm.get(2);
    assertEquals(row3.size(), 3);
    assertEquals(row3.get(0).longValue(), 4);
    assertEquals(row3.get(1).longValue(), 8);
    assertEquals(row3.get(2).longValue(), 16);
  }

  @Test
  public void testCorrMatrix() throws Exception {
    String cexpr = "let(echo=true," +
                       "a=array(1,2,3), " +
                       "b=array(2,4,6), " +
                       "c=array(4, 8, 52), " +
                       "d=transpose(matrix(a, b, c)), " +
                       "f=corr(d), " +
                       "g=corr(d, type=kendalls), " +
                       "h=corr(d, type=spearmans)," +
                       "i=corrPValues(f))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> cm = (List<List<Number>>)tuples.get(0).get("f");
    assertEquals(cm.size(), 3);
    List<Number> row1 = cm.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 1, 0);
    assertEquals(row1.get(1).doubleValue(), 1, 0);
    assertEquals(row1.get(2).doubleValue(), 0.901127113779166, 0);

    List<Number> row2 = cm.get(1);
    assertEquals(row2.size(), 3);
    assertEquals(row2.get(0).doubleValue(), 1, 0);
    assertEquals(row2.get(1).doubleValue(), 1, 0);
    assertEquals(row2.get(2).doubleValue(), 0.901127113779166, 0);

    List<Number> row3 = cm.get(2);
    assertEquals(row3.size(), 3);
    assertEquals(row3.get(0).doubleValue(), 0.901127113779166, 0);
    assertEquals(row3.get(1).doubleValue(), 0.901127113779166, 0);
    assertEquals(row3.get(2).doubleValue(), 1, 0);

    cm = (List<List<Number>>)tuples.get(0).get("g");
    assertEquals(cm.size(), 3);
    row1 = cm.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 1, 0);
    assertEquals(row1.get(1).doubleValue(), 1, 0);
    assertEquals(row1.get(2).doubleValue(), 1, 0);

    row2 = cm.get(1);
    assertEquals(row2.size(), 3);
    assertEquals(row2.get(0).doubleValue(), 1, 0);
    assertEquals(row2.get(1).doubleValue(), 1, 0);
    assertEquals(row2.get(2).doubleValue(), 1, 0);

    row3 = cm.get(2);
    assertEquals(row3.size(), 3);
    assertEquals(row3.get(0).doubleValue(), 1, 0);
    assertEquals(row3.get(1).doubleValue(), 1, 0);
    assertEquals(row3.get(2).doubleValue(), 1, 0);

    cm = (List<List<Number>>)tuples.get(0).get("h");
    assertEquals(cm.size(), 3);
    row1 = cm.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 1, 0);
    assertEquals(row1.get(1).doubleValue(), 1, 0);
    assertEquals(row1.get(2).doubleValue(), 1, 0);

    row2 = cm.get(1);
    assertEquals(row2.size(), 3);
    assertEquals(row2.get(0).doubleValue(), 1, 0);
    assertEquals(row2.get(1).doubleValue(), 1, 0);
    assertEquals(row2.get(2).doubleValue(), 1, 0);

    row3 = cm.get(2);
    assertEquals(row3.size(), 3);
    assertEquals(row3.get(0).doubleValue(), 1, 0);
    assertEquals(row3.get(1).doubleValue(), 1, 0);
    assertEquals(row3.get(2).doubleValue(), 1, 0);

    cm = (List<List<Number>>)tuples.get(0).get("i");
    assertEquals(cm.size(), 3);
    row1 = cm.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 0, 0);
    assertEquals(row1.get(1).doubleValue(), 0, 0);
    assertEquals(row1.get(2).doubleValue(), 0.28548201004998375, 0);

    row2 = cm.get(1);
    assertEquals(row2.size(), 3);
    assertEquals(row2.get(0).doubleValue(), 0, 0);
    assertEquals(row2.get(1).doubleValue(), 0, 0);
    assertEquals(row2.get(2).doubleValue(), 0.28548201004998375, 0);

    row3 = cm.get(2);
    assertEquals(row3.size(), 3);
    assertEquals(row3.get(0).doubleValue(), 0.28548201004998375, 0);
    assertEquals(row3.get(1).doubleValue(), 0.28548201004998375, 0);
    assertEquals(row3.get(2).doubleValue(), 0, 0);
  }

  @Test
  public void testPrecision() throws Exception {
    String cexpr = "let(echo=true, a=precision(array(1.44445, 1, 2.00006), 4), b=precision(1.44445, 4))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);

    List<Number> nums = (List<Number>)tuples.get(0).get("a");
    assertTrue(nums.size() == 3);
    assertEquals(nums.get(0).doubleValue(), 1.4445, 0.0);
    assertEquals(nums.get(1).doubleValue(), 1, 0.0);
    assertEquals(nums.get(2).doubleValue(), 2.0001, 0.0);

    double num = tuples.get(0).getDouble("b");
    assertEquals(num, 1.4445, 0.0);
  }

  @Test
  public void testMinMaxScale() throws Exception {
    String cexpr = "let(echo=true, a=minMaxScale(matrix(array(1,2,3,4,5), array(10,20,30,40,50))), " +
                                  "b=minMaxScale(matrix(array(1,2,3,4,5), array(10,20,30,40,50)), 0, 100)," +
                                  "c=minMaxScale(array(1,2,3,4,5))," +
                                  "d=minMaxScale(array(1,2,3,4,5), 0, 100))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);

    List<List<Number>> matrix = (List<List<Number>>)tuples.get(0).get("a");
    List<Number> row1 = matrix.get(0);
    assertEquals(row1.get(0).doubleValue(), 0,0);
    assertEquals(row1.get(1).doubleValue(), .25,0);
    assertEquals(row1.get(2).doubleValue(), .5,0);
    assertEquals(row1.get(3).doubleValue(), .75, 0);
    assertEquals(row1.get(4).doubleValue(), 1, 0);

    List<Number> row2 = matrix.get(1);
    assertEquals(row2.get(0).doubleValue(), 0,0);
    assertEquals(row2.get(1).doubleValue(), .25,0);
    assertEquals(row2.get(2).doubleValue(), .5,0);
    assertEquals(row2.get(3).doubleValue(), .75,0);
    assertEquals(row2.get(4).doubleValue(), 1,0);

    matrix = (List<List<Number>>)tuples.get(0).get("b");
    row1 = matrix.get(0);
    assertEquals(row1.get(0).doubleValue(), 0,0);
    assertEquals(row1.get(1).doubleValue(), 25,0);
    assertEquals(row1.get(2).doubleValue(), 50,0);
    assertEquals(row1.get(3).doubleValue(), 75,0);
    assertEquals(row1.get(4).doubleValue(), 100,0);

    row2 = matrix.get(1);
    assertEquals(row2.get(0).doubleValue(), 0,0);
    assertEquals(row2.get(1).doubleValue(), 25,0);
    assertEquals(row2.get(2).doubleValue(), 50,0);
    assertEquals(row2.get(3).doubleValue(), 75,0);
    assertEquals(row2.get(4).doubleValue(), 100,0);

    List<Number> row3= (List<Number>)tuples.get(0).get("c");
    assertEquals(row3.get(0).doubleValue(), 0,0);
    assertEquals(row3.get(1).doubleValue(), .25,0);
    assertEquals(row3.get(2).doubleValue(), .5,0);
    assertEquals(row3.get(3).doubleValue(), .75,0);
    assertEquals(row3.get(4).doubleValue(), 1,0);

    List<Number> row4= (List<Number>)tuples.get(0).get("d");
    assertEquals(row4.get(0).doubleValue(), 0,0);
    assertEquals(row4.get(1).doubleValue(), 25,0);
    assertEquals(row4.get(2).doubleValue(), 50,0);
    assertEquals(row4.get(3).doubleValue(), 75,0);
    assertEquals(row4.get(4).doubleValue(), 100,0);
  }

  @Test
  public void testMean() throws Exception {
    String cexpr = "mean(array(1,2,3,4,5))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number mean = (Number)tuples.get(0).get("return-value");
    assertEquals(mean.doubleValue(), 3.0D, 0.0D);
  }

  @Test
  public void testNorms() throws Exception {
    String cexpr = "let(echo=true, " +
        "               a=array(1,2,3,4,5,6), " +
        "               b=l1norm(a), " +
        "               c=l2norm(a), " +
        "               d=linfnorm(a), " +
        "               e=sqrt(add(pow(a, 2)))," +
        "               f=add(abs(a)))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number l1norm = (Number)tuples.get(0).get("b");
    assertEquals(l1norm.doubleValue(), 21.0D, 0.0D);

    Number norm = (Number)tuples.get(0).get("c");
    assertEquals(norm.doubleValue(), 9.5393920141695, 0.0001D);

    Number inorm = (Number)tuples.get(0).get("d");
    assertEquals(inorm.doubleValue(), 6.0, 0.0);

    Number norm2 = (Number)tuples.get(0).get("e");
    assertEquals(norm.doubleValue(), norm2.doubleValue(), 0.0);

    Number l1norm2 = (Number)tuples.get(0).get("f");
    assertEquals(l1norm.doubleValue(), l1norm2.doubleValue(), 0.0);
  }

  @Test
  public void testScale() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", c=col(a, max(price_f)), tuple(reverse=rev(c), scaled=scale(2, c)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> reverse = (List<Number>)tuples.get(0).get("reverse");
    assertTrue(reverse.size() == 4);
    assertTrue(reverse.get(0).doubleValue() == 400D);
    assertTrue(reverse.get(1).doubleValue() == 300D);
    assertTrue(reverse.get(2).doubleValue() == 500D);
    assertTrue(reverse.get(3).doubleValue() == 100D);

    List<Number> ranked = (List<Number>)tuples.get(0).get("scaled");
    assertTrue(ranked.size() == 4);
    assertTrue(ranked.get(0).doubleValue() == 200D);
    assertTrue(ranked.get(1).doubleValue() == 1000D);
    assertTrue(ranked.get(2).doubleValue() == 600D);
    assertTrue(ranked.get(3).doubleValue() == 800D);
  }

  @Test
  public void testConvolution() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", b=select("+expr+",mult(2, count(*)) as nvalue), c=col(a, count(*)), d=col(b, nvalue), tuple(colc=c, cold=d, conv=conv(c,d)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> convolution = (List<Number>)(tuples.get(0)).get("conv");
    assertTrue(convolution.size() == 7);
    assertTrue(convolution.get(0).equals(20000L));
    assertTrue(convolution.get(1).equals(20000L));
    assertTrue(convolution.get(2).equals(25000L));
    assertTrue(convolution.get(3).equals(30000L));
    assertTrue(convolution.get(4).equals(15000L));
    assertTrue(convolution.get(5).equals(10000L));
    assertTrue(convolution.get(6).equals(5000L));
  }

  @Test
  public void testRegressAndPredict() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    updateRequest.add(id, "1", "price_f", "100.0", "col_s", "a", "order_i", "1");
    updateRequest.add(id, "2", "price_f", "200.0", "col_s", "a", "order_i", "2");
    updateRequest.add(id, "3", "price_f", "300.0", "col_s", "a", "order_i", "3");
    updateRequest.add(id, "4", "price_f", "100.0", "col_s", "a", "order_i", "4");
    updateRequest.add(id, "5", "price_f", "200.0", "col_s", "a", "order_i", "5");
    updateRequest.add(id, "6", "price_f", "400.0", "col_s", "a", "order_i", "6");
    updateRequest.add(id, "7", "price_f", "600.0", "col_s", "a", "order_i", "7");

    updateRequest.add(id, "8", "price_f", "200.0", "col_s", "b", "order_i", "1");
    updateRequest.add(id, "9", "price_f", "400.0", "col_s", "b", "order_i", "2");
    updateRequest.add(id, "10", "price_f", "600.0", "col_s", "b", "order_i", "3");
    updateRequest.add(id, "11", "price_f", "200.0", "col_s", "b", "order_i", "4");
    updateRequest.add(id, "12", "price_f", "400.0", "col_s", "b", "order_i", "5");
    updateRequest.add(id, "13", "price_f", "800.0", "col_s", "b", "order_i", "6");
    updateRequest.add(id, "14", "price_f", "1200.0", "col_s", "b", "order_i", "7");
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr1 = "search("+COLLECTIONORALIAS+", q=\"col_s:a\", fl=\"price_f, order_i\", sort=\"order_i asc\")";
    String expr2 = "search("+COLLECTIONORALIAS+", q=\"col_s:b\", fl=\"price_f, order_i\", sort=\"order_i asc\")";

    String cexpr = "let(a="+expr1+", b="+expr2+", c=col(a, price_f), d=col(b, price_f), e=regress(c, d), tuple(regress=e, p=predict(e, 300), pl=predict(e, c)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    Map regression = (Map)tuple.get("regress");
    double slope = (double)regression.get("slope");
    double intercept= (double) regression.get("intercept");
    double rSquare= (double) regression.get("RSquared");
    assertTrue(slope == 2.0D);
    assertTrue(intercept == 0.0D);
    assertTrue(rSquare == 1.0D);
    double prediction = tuple.getDouble("p");
    assertTrue(prediction == 600.0D);
    List<Number> predictions = (List<Number>)tuple.get("pl");
    assertList(predictions, 200L, 400L, 600L, 200L, 400L, 800L, 1200L);
  }

  @Test
  public void testFinddelay() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    //Pad column 1 with three zeros.
    updateRequest.add(id, "10", "price_f", "0.0", "col_s", "a", "order_i", "0");
    updateRequest.add(id, "11", "price_f", "0.0", "col_s", "a", "order_i", "0");
    updateRequest.add(id, "12", "price_f", "0.0", "col_s", "a", "order_i", "0");
    updateRequest.add(id, "1", "price_f", "100.0", "col_s", "a", "order_i", "1");
    updateRequest.add(id, "2", "price_f", "200.0", "col_s", "a", "order_i", "2");
    updateRequest.add(id, "3", "price_f", "300.0", "col_s", "a", "order_i", "3");
    updateRequest.add(id, "4", "price_f", "100.0", "col_s", "a", "order_i", "4");
    updateRequest.add(id, "5", "price_f", "200.0", "col_s", "a", "order_i", "5");
    updateRequest.add(id, "6", "price_f", "400.0", "col_s", "a", "order_i", "6");
    updateRequest.add(id, "7", "price_f", "600.0", "col_s", "a", "order_i", "7");

    updateRequest.add(id, "100", "price_f", "200.0", "col_s", "b", "order_i", "1");
    updateRequest.add(id, "101", "price_f", "400.0", "col_s", "b", "order_i", "2");
    updateRequest.add(id, "102", "price_f", "600.0", "col_s", "b", "order_i", "3");
    updateRequest.add(id, "103", "price_f", "200.0", "col_s", "b", "order_i", "4");
    updateRequest.add(id, "104", "price_f", "400.0", "col_s", "b", "order_i", "5");
    updateRequest.add(id, "105", "price_f", "800.0", "col_s", "b", "order_i", "6");
    updateRequest.add(id, "106", "price_f", "1200.0", "col_s", "b", "order_i", "7");


    updateRequest.add(id, "200", "price_f", "-200.0", "col_s", "c", "order_i", "1");
    updateRequest.add(id, "301", "price_f", "-400.0", "col_s", "c", "order_i", "2");
    updateRequest.add(id, "402", "price_f", "-600.0", "col_s", "c", "order_i", "3");
    updateRequest.add(id, "503", "price_f", "-200.0", "col_s", "c", "order_i", "4");
    updateRequest.add(id, "604", "price_f", "-400.0", "col_s", "c", "order_i", "5");
    updateRequest.add(id, "705", "price_f", "-800.0", "col_s", "c", "order_i", "6");
    updateRequest.add(id, "806", "price_f", "-1200.0", "col_s", "c", "order_i", "7");
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr1 = "search("+COLLECTIONORALIAS+", q=\"col_s:a\", fl=\"price_f, order_i\", sort=\"order_i asc\")";
    String expr2 = "search("+COLLECTIONORALIAS+", q=\"col_s:b\", fl=\"price_f, order_i\", sort=\"order_i asc\")";

    String cexpr = "let(a="+expr1+", b="+expr2+", c=col(a, price_f), d=col(b, price_f), tuple(delay=finddelay(c, d)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    long delay = tuple.getLong("delay");
    assert(delay == 3);

    expr1 = "search("+COLLECTIONORALIAS+", q=\"col_s:a\", fq=\"id:(1 2 3 4 5 6 7)\", fl=\"price_f, order_i\", sort=\"order_i asc\")";
    expr2 = "search("+COLLECTIONORALIAS+", q=\"col_s:b\", fl=\"price_f, order_i\", sort=\"order_i asc\")";

    cexpr = "let(a="+expr1+", b="+expr2+", c=col(a, price_f), d=col(b, price_f), tuple(delay=finddelay(c, d)))";

    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    tuple = tuples.get(0);
    delay = tuple.getLong("delay");
    assert(delay == 0);

    //Test negative correlation.
    expr1 = "search("+COLLECTIONORALIAS+", q=\"col_s:a\", fq=\"id:(1 2 3 4 5 6 7 11 12)\",fl=\"price_f, order_i\", sort=\"order_i asc\")";
    expr2 = "search("+COLLECTIONORALIAS+", q=\"col_s:c\", fl=\"price_f, order_i\", sort=\"order_i asc\")";

    cexpr = "let(a="+expr1+", b="+expr2+", c=col(a, price_f), d=col(b, price_f), tuple(delay=finddelay(c, d)))";

    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    tuple = tuples.get(0);
    delay = tuple.getLong("delay");
    assert(delay == 2);
  }

  @Test
  public void testDescribe() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    updateRequest.add(id, "1", "price_f", "100.0", "col_s", "a", "order_i", "1");
    updateRequest.add(id, "2", "price_f", "200.0", "col_s", "a", "order_i", "2");
    updateRequest.add(id, "3", "price_f", "300.0", "col_s", "a", "order_i", "3");
    updateRequest.add(id, "4", "price_f", "100.0", "col_s", "a", "order_i", "4");
    updateRequest.add(id, "5", "price_f", "200.0", "col_s", "a", "order_i", "5");
    updateRequest.add(id, "6", "price_f", "400.0", "col_s", "a", "order_i", "6");
    updateRequest.add(id, "7", "price_f", "600.0", "col_s", "a", "order_i", "7");

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr1 = "search("+COLLECTIONORALIAS+", q=\"col_s:a\", fl=\"price_f, order_i\", sort=\"order_i asc\")";

    String cexpr = "let(a="+expr1+", b=col(a, price_f),  tuple(stats=describe(b)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    Map stats = (Map)tuple.get("stats");
    Number min = (Number)stats.get("min");
    Number max = (Number)stats.get("max");
    Number mean = (Number)stats.get("mean");
    Number stdev = (Number)stats.get("stdev");
    Number popVar = (Number)stats.get("popVar");
    Number skewness = (Number)stats.get("skewness");
    Number kurtosis = (Number)stats.get("kurtosis");
    Number var = (Number)stats.get("var");
    Number geometricMean = (Number)stats.get("geometricMean");
    Number N = (Number)stats.get("N");
    assertEquals(min.doubleValue(), 100.0D, 0.0);
    assertEquals(max.doubleValue(), 600.0D, 0.0);
    assertEquals(N.doubleValue(), 7.0D, 0.0);
    assertEquals(mean.doubleValue(), 271.42D, 0.5);
    assertEquals(popVar.doubleValue(), 27755.10, 0.5);
    assertEquals(kurtosis.doubleValue(), .70D, 0.5);
    assertEquals(skewness.doubleValue(), 1.07D, 0.5);
    assertEquals(var.doubleValue(), 32380.95D, 0.5);
    assertEquals(geometricMean.doubleValue(), 224.56D, 0.5);
    assertEquals(stdev.doubleValue(), 179.94D, 0.5);
  }

  @Test
  public void testLength() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "1", "price_f", "100.0", "col_s", "a", "order_i", "1");
    updateRequest.add(id, "2", "price_f", "200.0", "col_s", "a", "order_i", "2");
    updateRequest.add(id, "3", "price_f", "300.0", "col_s", "a", "order_i", "3");
    updateRequest.add(id, "4", "price_f", "100.0", "col_s", "a", "order_i", "4");
    updateRequest.add(id, "5", "price_f", "200.0", "col_s", "a", "order_i", "5");
    updateRequest.add(id, "6", "price_f", "400.0", "col_s", "a", "order_i", "6");
    updateRequest.add(id, "7", "price_f", "600.0", "col_s", "a", "order_i", "7");

    updateRequest.add(id, "8", "price_f", "200.0", "col_s", "b", "order_i", "1");
    updateRequest.add(id, "9", "price_f", "400.0", "col_s", "b", "order_i", "2");
    updateRequest.add(id, "10", "price_f", "600.0", "col_s", "b", "order_i", "3");
    updateRequest.add(id, "11", "price_f", "200.0", "col_s", "b", "order_i", "4");
    updateRequest.add(id, "12", "price_f", "400.0", "col_s", "b", "order_i", "5");
    updateRequest.add(id, "13", "price_f", "800.0", "col_s", "b", "order_i", "6");
    updateRequest.add(id, "14", "price_f", "1200.0", "col_s", "b", "order_i", "7");
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr1 = "search("+COLLECTIONORALIAS+", q=\"col_s:a\", fl=\"price_f, order_i\", sort=\"order_i asc\")";
    String expr2 = "search("+COLLECTIONORALIAS+", q=\"col_s:b\", fl=\"price_f, order_i\", sort=\"order_i asc\")";

    String cexpr = "let(a="+expr1+", b="+expr2+", c=col(a, price_f), d=col(b, price_f), e=regress(c, d), tuple(regress=e, p=predict(e, 300), l=length(d)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    Map regression = (Map)tuple.get("regress");
    double slope = (double)regression.get("slope");
    double intercept= (double) regression.get("intercept");
    double length = tuple.getDouble("l");

    assertTrue(slope == 2.0D);
    assertTrue(intercept == 0.0D);
    double prediction = tuple.getDouble("p");
    assertTrue(prediction == 600.0D);
    assertTrue(length == 7);
  }

  @Test
  public void testConvertEvaluator() throws Exception {

    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "1", "miles_i", "50");
    updateRequest.add(id, "2", "miles_i", "70");

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    //Test annotating tuple
    String expr = "select(calc(), convert(miles, kilometers, 10) as kilometers)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    double d = (double)tuples.get(0).get("kilometers");
    assertTrue(d == (double)(10*1.61));


    expr = "select(search("+COLLECTIONORALIAS+", q=\"*:*\", sort=\"miles_i asc\", fl=\"miles_i\"), convert(miles, kilometers, miles_i) as kilometers)";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 2);
    d = (double)tuples.get(0).get("kilometers");
    assertTrue(d == (double)(50*1.61));
    d = (double)tuples.get(1).get("kilometers");
    assertTrue(d == (double)(70*1.61));

    expr = "parallel("+COLLECTIONORALIAS+", workers=2, sort=\"miles_i asc\", select(search("+COLLECTIONORALIAS+", q=\"*:*\", partitionKeys=miles_i, sort=\"miles_i asc\", fl=\"miles_i\"), convert(miles, kilometers, miles_i) as kilometers))";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");
    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 2);
    d = (double)tuples.get(0).get("kilometers");
    assertTrue(d == (double)(50*1.61));
    d = (double)tuples.get(1).get("kilometers");
    assertTrue(d == (double)(70*1.61));

    expr = "select(stats("+COLLECTIONORALIAS+", q=\"*:*\", sum(miles_i)), convert(miles, kilometers, sum(miles_i)) as kilometers)";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");
    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    d = (double)tuples.get(0).get("kilometers");
    assertTrue(d == (double)(120*1.61));
  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    List<Tuple> tuples = new ArrayList<Tuple>();

    try {
      tupleStream.open();
      for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
        tuples.add(t);
      }
    } finally {
      tupleStream.close();
    }
    return tuples;
  }

  public boolean assertLong(Tuple tuple, String fieldName, long l) throws Exception {
    long lv = (long)tuple.get(fieldName);
    if(lv != l) {
      throw new Exception("Longs not equal:"+l+" : "+lv);
    }

    return true;
  }
  
  public boolean assertString(Tuple tuple, String fieldName, String expected) throws Exception {
    String actual = (String)tuple.get(fieldName);
    
    if( (null == expected && null != actual) ||
        (null != expected && null == actual) ||
        (null != expected && !expected.equals(actual))){
      throw new Exception("Longs not equal:"+expected+" : "+actual);
    }

    return true;
  }

  private boolean assertList(List list, Object... vals) throws Exception {

    if(list.size() != vals.length) {
      throw new Exception("Lists are not the same size:"+list.size() +" : "+vals.length);
    }

    for(int i=0; i<list.size(); i++) {
      Object a = list.get(i);
      Object b = vals[i];
      if(!a.equals(b)) {
        throw new Exception("List items not equals:"+a+" : "+b);
      }
    }

    return true;
  }
}
