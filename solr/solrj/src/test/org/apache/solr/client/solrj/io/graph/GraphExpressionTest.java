package org.apache.solr.client.solrj.io.graph;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *  All base tests will be done with CloudSolrStream. Under the covers CloudSolrStream uses SolrStream so
 *  SolrStream will get fully exercised through these tests.
 *
 **/

@Slow
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class GraphExpressionTest extends AbstractFullDistribZkTestBase {

  private static final String SOLR_HOME = getFile("solrj" + File.separator + "solr").getAbsolutePath();

  static {
    schemaString = "schema-streaming.xml";
  }

  @BeforeClass
  public static void beforeSuperClass() {
    AbstractZkTestCase.SOLRHOME = new File(SOLR_HOME());
  }

  @AfterClass
  public static void afterSuperClass() {

  }

  protected String getCloudSolrConfig() {
    return "solrconfig-streaming.xml";
  }


  @Override
  public String getSolrHome() {
    return SOLR_HOME;
  }

  public static String SOLR_HOME() {
    return SOLR_HOME;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // we expect this time of exception as shards go up and down...
    //ignoreException(".*");

    System.setProperty("numShards", Integer.toString(sliceCount));
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    resetExceptionIgnores();
  }

  public GraphExpressionTest() {
    super();
    sliceCount = 2;
  }

  @Test
  public void testAll() throws Exception{
    assertNotNull(cloudClient);

    handle.clear();
    handle.put("timestamp", SKIPVAL);

    waitForRecoveriesToFinish(false);

    del("*:*");
    commit();

    testShortestPathStream();
  }

  private void testShortestPathStream() throws Exception {

    indexr(id, "0", "from_s", "jim", "to_s", "mike", "predicate_s", "knows");
    indexr(id, "1", "from_s", "jim", "to_s", "dave", "predicate_s", "knows");
    indexr(id, "2", "from_s", "jim", "to_s", "stan", "predicate_s", "knows");
    indexr(id, "3", "from_s", "dave", "to_s", "stan", "predicate_s", "knows");
    indexr(id, "4", "from_s", "dave", "to_s", "bill", "predicate_s", "knows");
    indexr(id, "5", "from_s", "dave", "to_s", "mike", "predicate_s", "knows");
    indexr(id, "20", "from_s", "dave", "to_s", "alex", "predicate_s", "knows");
    indexr(id, "21", "from_s", "alex", "to_s", "steve", "predicate_s", "knows");
    indexr(id, "6", "from_s", "stan", "to_s", "alice", "predicate_s", "knows");
    indexr(id, "7", "from_s", "stan", "to_s", "mary", "predicate_s", "knows");
    indexr(id, "8", "from_s", "stan", "to_s", "dave", "predicate_s", "knows");
    indexr(id, "10", "from_s", "mary", "to_s", "mike", "predicate_s", "knows");
    indexr(id, "11", "from_s", "mary", "to_s", "max", "predicate_s", "knows");
    indexr(id, "12", "from_s", "mary", "to_s", "jim", "predicate_s", "knows");
    indexr(id, "13", "from_s", "mary", "to_s", "steve", "predicate_s", "knows");

    commit();

    List<Tuple> tuples = null;
    Set<String> paths = null;
    ShortestPathStream stream = null;
    StreamContext context = new StreamContext();
    SolrClientCache cache = new SolrClientCache();
    context.setSolrClientCache(cache);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", zkServer.getZkAddress())
        .withFunctionName("shortestPath", ShortestPathStream.class);

    Map params = new HashMap();
    params.put("fq", "predicate_s:knows");

    stream = (ShortestPathStream)factory.constructStream("shortestPath(collection1, " +
        "from=\"jim\", " +
        "to=\"steve\"," +
        "edge=\"from_s=to_s\"," +
        "fq=\"predicate_s:knows\","+
        "threads=\"3\","+
        "partitionSize=\"3\","+
        "maxDepth=\"6\")");

    stream.setStreamContext(context);
    paths = new HashSet();
    tuples = getTuples(stream);

    assertTrue(tuples.size() == 2);

    for(Tuple tuple : tuples) {
      paths.add(tuple.getStrings("path").toString());
    }

    assertTrue(paths.contains("[jim, dave, alex, steve]"));
    assertTrue(paths.contains("[jim, stan, mary, steve]"));

    //Test with batch size of 1

    params.put("fq", "predicate_s:knows");

    stream = (ShortestPathStream)factory.constructStream("shortestPath(collection1, " +
        "from=\"jim\", " +
        "to=\"steve\"," +
        "edge=\"from_s=to_s\"," +
        "fq=\"predicate_s:knows\","+
        "threads=\"3\","+
        "partitionSize=\"1\","+
        "maxDepth=\"6\")");

    stream.setStreamContext(context);
    paths = new HashSet();
    tuples = getTuples(stream);

    assertTrue(tuples.size() == 2);

    for(Tuple tuple : tuples) {
      paths.add(tuple.getStrings("path").toString());
    }

    assertTrue(paths.contains("[jim, dave, alex, steve]"));
    assertTrue(paths.contains("[jim, stan, mary, steve]"));

    //Test with bad predicate


    stream = (ShortestPathStream)factory.constructStream("shortestPath(collection1, " +
        "from=\"jim\", " +
        "to=\"steve\"," +
        "edge=\"from_s=to_s\"," +
        "fq=\"predicate_s:crap\","+
        "threads=\"3\","+
        "partitionSize=\"3\","+
        "maxDepth=\"6\")");

    stream.setStreamContext(context);
    paths = new HashSet();
    tuples = getTuples(stream);

    assertTrue(tuples.size() == 0);

    //Test with depth 2

    stream = (ShortestPathStream)factory.constructStream("shortestPath(collection1, " +
        "from=\"jim\", " +
        "to=\"steve\"," +
        "edge=\"from_s=to_s\"," +
        "fq=\"predicate_s:knows\","+
        "threads=\"3\","+
        "partitionSize=\"3\","+
        "maxDepth=\"2\")");


    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assertTrue(tuples.size() == 0);

    //Take out alex
    params.put("fq", "predicate_s:knows NOT to_s:alex");

    stream = (ShortestPathStream)factory.constructStream("shortestPath(collection1, " +
        "from=\"jim\", " +
        "to=\"steve\"," +
        "edge=\"from_s=to_s\"," +
        "fq=\" predicate_s:knows NOT to_s:alex\","+
        "threads=\"3\","+
        "partitionSize=\"3\","+
        "maxDepth=\"6\")");


    stream.setStreamContext(context);
    paths = new HashSet();
    tuples = getTuples(stream);
    assertTrue(tuples.size() == 1);

    for(Tuple tuple : tuples) {
      paths.add(tuple.getStrings("path").toString());
    }

    assertTrue(paths.contains("[jim, stan, mary, steve]"));

    cache.close();
    del("*:*");
    commit();
  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList<Tuple>();
    for(Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
      tuples.add(t);
    }
    tupleStream.close();
    return tuples;
  }
  protected boolean assertOrder(List<Tuple> tuples, int... ids) throws Exception {
    return assertOrderOf(tuples, "id", ids);
  }
  protected boolean assertOrderOf(List<Tuple> tuples, String fieldName, int... ids) throws Exception {
    int i = 0;
    for(int val : ids) {
      Tuple t = tuples.get(i);
      Long tip = (Long)t.get(fieldName);
      if(tip.intValue() != val) {
        throw new Exception("Found value:"+tip.intValue()+" expecting:"+val);
      }
      ++i;
    }
    return true;
  }

  protected boolean assertMapOrder(List<Tuple> tuples, int... ids) throws Exception {
    int i = 0;
    for(int val : ids) {
      Tuple t = tuples.get(i);
      List<Map> tip = t.getMaps("group");
      int id = (int)tip.get(0).get("id");
      if(id != val) {
        throw new Exception("Found value:"+id+" expecting:"+val);
      }
      ++i;
    }
    return true;
  }


  protected boolean assertFields(List<Tuple> tuples, String ... fields) throws Exception{
    for(Tuple tuple : tuples){
      for(String field : fields){
        if(!tuple.fields.containsKey(field)){
          throw new Exception(String.format(Locale.ROOT, "Expected field '%s' not found", field));
        }
      }
    }
    return true;
  }
  protected boolean assertNotFields(List<Tuple> tuples, String ... fields) throws Exception{
    for(Tuple tuple : tuples){
      for(String field : fields){
        if(tuple.fields.containsKey(field)){
          throw new Exception(String.format(Locale.ROOT, "Unexpected field '%s' found", field));
        }
      }
    }
    return true;
  }

  protected boolean assertGroupOrder(Tuple tuple, int... ids) throws Exception {
    List<?> group = (List<?>)tuple.get("tuples");
    int i=0;
    for(int val : ids) {
      Map<?,?> t = (Map<?,?>)group.get(i);
      Long tip = (Long)t.get("id");
      if(tip.intValue() != val) {
        throw new Exception("Found value:"+tip.intValue()+" expecting:"+val);
      }
      ++i;
    }
    return true;
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

  protected boolean assertMaps(List<Map> maps, int... ids) throws Exception {
    if(maps.size() != ids.length) {
      throw new Exception("Expected id count != actual map count:"+ids.length+":"+maps.size());
    }

    int i=0;
    for(int val : ids) {
      Map t = maps.get(i);
      Long tip = (Long)t.get("id");
      if(tip.intValue() != val) {
        throw new Exception("Found value:"+tip.intValue()+" expecting:"+val);
      }
      ++i;
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


  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }
}
