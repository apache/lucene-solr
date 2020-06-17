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
package org.apache.solr.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.junit.Assert;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_DEF;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.NUM_SHARDS_PROP;
import static org.apache.solr.common.util.Utils.fromJSONString;

/**
 *
 */
public class TestUtils extends SolrTestCaseJ4 {
  
  public void testJoin() {
    assertEquals("a|b|c",   StrUtils.join(asList("a","b","c"), '|'));
    assertEquals("a,b,c",   StrUtils.join(asList("a","b","c"), ','));
    assertEquals("a\\,b,c", StrUtils.join(asList("a,b","c"), ','));
    assertEquals("a,b|c",   StrUtils.join(asList("a,b","c"), '|'));

    assertEquals("a\\\\b|c",   StrUtils.join(asList("a\\b","c"), '|'));
  }

  public void testEscapeTextWithSeparator() {
    assertEquals("a",  StrUtils.escapeTextWithSeparator("a", '|'));
    assertEquals("a",  StrUtils.escapeTextWithSeparator("a", ','));
                              
    assertEquals("a\\|b",  StrUtils.escapeTextWithSeparator("a|b", '|'));
    assertEquals("a|b",    StrUtils.escapeTextWithSeparator("a|b", ','));
    assertEquals("a,b",    StrUtils.escapeTextWithSeparator("a,b", '|'));
    assertEquals("a\\,b",  StrUtils.escapeTextWithSeparator("a,b", ','));
    assertEquals("a\\\\b", StrUtils.escapeTextWithSeparator("a\\b", ','));

    assertEquals("a\\\\\\,b", StrUtils.escapeTextWithSeparator("a\\,b", ','));
  }

  public void testSplitEscaping() {
    List<String> arr = StrUtils.splitSmart("\\r\\n:\\t\\f\\b", ":", true);
    assertEquals(2,arr.size());
    assertEquals("\r\n",arr.get(0));
    assertEquals("\t\f\b",arr.get(1));

    arr = StrUtils.splitSmart("\\r\\n:\\t\\f\\b", ":", false);
    assertEquals(2,arr.size());
    assertEquals("\\r\\n",arr.get(0));
    assertEquals("\\t\\f\\b",arr.get(1));

    arr = StrUtils.splitWS("\\r\\n \\t\\f\\b", true);
    assertEquals(2,arr.size());
    assertEquals("\r\n",arr.get(0));
    assertEquals("\t\f\b",arr.get(1));

    arr = StrUtils.splitWS("\\r\\n \\t\\f\\b", false);
    assertEquals(2,arr.size());
    assertEquals("\\r\\n",arr.get(0));
    assertEquals("\\t\\f\\b",arr.get(1));

    arr = StrUtils.splitSmart("\\:foo\\::\\:bar\\:", ":", true);
    assertEquals(2,arr.size());
    assertEquals(":foo:",arr.get(0));
    assertEquals(":bar:",arr.get(1));

    arr = StrUtils.splitWS("\\ foo\\  \\ bar\\ ", true);
    assertEquals(2,arr.size());
    assertEquals(" foo ",arr.get(0));
    assertEquals(" bar ",arr.get(1));
    
    arr = StrUtils.splitFileNames("/h/s,/h/\\,s,");
    assertEquals(2,arr.size());
    assertEquals("/h/s",arr.get(0));
    assertEquals("/h/,s",arr.get(1));

    arr = StrUtils.splitFileNames("/h/s");
    assertEquals(1,arr.size());
    assertEquals("/h/s",arr.get(0));
  }

  public void testNamedLists()
  {
    SimpleOrderedMap<Integer> map = new SimpleOrderedMap<>();
    map.add( "test", 10 );
    SimpleOrderedMap<Integer> clone = map.clone();
    assertEquals( map.toString(), clone.toString() );
    assertEquals(Integer.valueOf(10), clone.get( "test" ) );
  
    Map<String,Integer> realMap = new HashMap<>();
    realMap.put( "one", 1 );
    realMap.put( "two", 2 );
    realMap.put( "three", 3 );
    map = new SimpleOrderedMap<>();
    map.addAll( realMap );
    assertEquals( 3, map.size() );
    map = new SimpleOrderedMap<>();
    map.add( "one", 1 );
    map.add( "two", 2 );
    map.add( "three", 3 );
    map.add( "one", 100 );
    map.add( null, null );
    
    assertEquals( "one", map.getName(0) );
    map.setName( 0, "ONE" );
    assertEquals( "ONE", map.getName(0) );
    assertEquals(Integer.valueOf(100), map.get( "one", 1 ) );
    assertEquals( 4, map.indexOf( null, 1 ) );
    assertEquals( null, map.get( null, 1 ) );

    map = new SimpleOrderedMap<>();
    map.add( "one", 1 );
    map.add( "two", 2 );
    Iterator<Map.Entry<String, Integer>> iter = map.iterator();
    while( iter.hasNext() ) {
      Map.Entry<String, Integer> v = iter.next();
      v.toString(); // coverage
      v.setValue( v.getValue()*10 );
      try {
        iter.remove();
        Assert.fail( "should be unsupported..." );
      } catch( UnsupportedOperationException ignored) {}
    }
    // the values should be bigger
    assertEquals(Integer.valueOf(10), map.get( "one" ) );
    assertEquals(Integer.valueOf(20), map.get( "two" ) );
  }
  
  public void testNumberUtils()
  {
    double number = 1.234;
    String sortable = NumberUtils.double2sortableStr( number );
    assertEquals( number, NumberUtils.SortableStr2double(sortable), 0.001);
    
    long num = System.nanoTime();
    sortable = NumberUtils.long2sortableStr( num );
    assertEquals( num, NumberUtils.SortableStr2long(sortable, 0, sortable.length() ) );
    assertEquals( Long.toString(num), NumberUtils.SortableStr2long(sortable) );
  }

  public void testNoggitFlags() throws IOException {
    String s = "a{b:c, d [{k1:v1}{k2:v2}]}";
    assertNoggitJsonValues((Map) Utils.fromJSON(s.getBytes(UTF_8)));
    assertNoggitJsonValues((Map) fromJSONString(s));
    List<CommandOperation> commands = CommandOperation.parse(new StringReader(s + s));
    assertEquals(2, commands.size());
    for (CommandOperation command : commands) {
      assertEquals("a", command.name);
      assertEquals( "v1" ,Utils.getObjectByPath(command.getDataMap(), true, "d[0]/k1"));command.getDataMap();
      assertEquals( "v2" ,Utils.getObjectByPath(command.getDataMap(), true, "d[1]/k2"));command.getDataMap();
    }
  }

  public void testBinaryCommands() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final JavaBinCodec jbc = new JavaBinCodec()) {
      jbc.marshal((MapWriter) ew -> {
        ew.put("set-user", fromJSONString("{x:y}"));
        ew.put("set-user", fromJSONString("{x:y,x1:y1}"));
        ew.put("single", asList(fromJSONString("[{x:y,x1:y1},{x2:y2}]"), fromJSONString( "{x2:y2}")));
        ew.put("multi", asList(fromJSONString("{x:y,x1:y1}"), fromJSONString( "{x2:y2}")));
      }, baos);
    }

    ContentStream stream = new ContentStreamBase.ByteArrayStream(baos.toByteArray(),null, "application/javabin");
    @SuppressWarnings({"rawtypes"})
    List<CommandOperation> commands = CommandOperation.readCommands(Collections.singletonList(stream), new NamedList(), Collections.singleton("single"));

    assertEquals(5, commands.size());
  }

  private void assertNoggitJsonValues(@SuppressWarnings({"rawtypes"})Map m) {
    assertEquals( "c" ,Utils.getObjectByPath(m, true, "/a/b"));
    assertEquals( "v1" ,Utils.getObjectByPath(m, true, "/a/d[0]/k1"));
    assertEquals( "v2" ,Utils.getObjectByPath(m, true, "/a/d[1]/k2"));
  }
  public void testSetObjectByPath(){
    String json = "{\n" +
        "  'authorization':{\n" +
        "    'class':'solr.RuleBasedAuthorizationPlugin',\n" +
        "    'user-role':{\n" +
        "      'solr':'admin',\n" +
        "      'harry':'admin'},\n" +
        "    'permissions':[{\n" +
        "        'name':'security-edit',\n" +
        "        'role':['admin']},\n" +
        "      {\n" +
        "        'name':'x-update',\n" +
        "        'collection':'x',\n" +
        "        'path':'/update/*',\n" +
        "        'role':'dev'}],\n" +
        "    '':{'v':4}}}";
    @SuppressWarnings({"rawtypes"})
    Map m = (Map) fromJSONString(json);
    Utils.setObjectByPath(m,"authorization/permissions[1]/role","guest");
    Utils.setObjectByPath(m,"authorization/permissions[0]/role[-1]","dev");
    assertEquals("guest", Utils.getObjectByPath(m,true,"authorization/permissions[1]/role"));
    assertEquals("dev", Utils.getObjectByPath(m,true,"authorization/permissions[0]/role[1]"));

  }

  public void testUtilsJSPath(){

    String json = "{\n" +
        "  'authorization':{\n" +
        "    'class':'solr.RuleBasedAuthorizationPlugin',\n" +
        "    'user-role':{\n" +
        "      'solr':'admin',\n" +
        "      'harry':'admin'},\n" +
        "    'permissions':[{\n" +
        "        'name':'security-edit',\n" +
        "        'role':'admin'},\n" +
        "      {\n" +
        "        'name':'x-update',\n" +
        "        'collection':'x',\n" +
        "        'path':'/update/*',\n" +
        "        'role':'dev'}],\n" +
        "    '':{'v':4}}}";
    @SuppressWarnings({"rawtypes"})
    Map m = (Map) fromJSONString(json);
    assertEquals("x-update", Utils.getObjectByPath(m,false, "authorization/permissions[1]/name"));

  }
  
  @SuppressWarnings({"unchecked"})
  public void testMapWriterIdx(){
    String json = "{" +
        "  'responseHeader':{" +
        "    'status':0," +
        "    'QTime':6752}," +
        "  'success':{" +
        "    '127.0.0.1:56443_solr':{" +
        "      'responseHeader':{" +
        "        'status':0," +
        "        'QTime':4276}," +
        "      'core':'corestatus_test_shard2_replica_n5'}," +
        "    '127.0.0.1:56445_solr':{" +
        "      'responseHeader':{" +
        "        'status':0," +
        "        'QTime':4271}," +
        "      'core':'corestatus_test_shard1_replica_n1'}," +
        "    '127.0.0.1:56446_solr':{" +
        "      'responseHeader':{" +
        "        'status':0," +
        "        'QTime':5015}," +
        "      'core':'corestatus_test_shard1_replica_n2'}," +
        "    '127.0.0.1:56444_solr':{" +
        "      'responseHeader':{" +
        "        'status':0," +
        "        'QTime':5033}," +
        "      'core':'corestatus_test_shard2_replica_n3'}}}";
    @SuppressWarnings({"rawtypes"})
    Map m = (Map) fromJSONString(json);

    assertEquals("127.0.0.1:56443_solr", Utils.getObjectByPath(m,false, "success[0]/key"));
    assertEquals("corestatus_test_shard2_replica_n5", Utils.getObjectByPath(m, false,asList("success[0]", "value", "core") ));
    assertEquals(4276L, Utils.getObjectByPath(m, false,asList("success[0]", "value", "responseHeader", "QTime") ));

    assertEquals("127.0.0.1:56444_solr", Utils.getObjectByPath(m,false, "success[3]/key"));
    assertEquals("corestatus_test_shard2_replica_n3", Utils.getObjectByPath(m, false,asList("success[3]", "value", "core") ));
    assertEquals(5033L, Utils.getObjectByPath(m, false,asList("success[3]", "value", "responseHeader", "QTime") ));

    @SuppressWarnings({"rawtypes"})
    Map nodes = (Map) m.get("success");
    m.put("success", (MapWriter) ew -> nodes.forEach((o, o2) -> ew.putNoEx((String) o,o2)));
    assertEquals("127.0.0.1:56443_solr", Utils.getObjectByPath(m,false, "success[0]/key"));
    assertEquals("corestatus_test_shard2_replica_n5", Utils.getObjectByPath(m, false,asList("success[0]", "value", "core") ));
    assertEquals(4276L, Utils.getObjectByPath(m, false,asList("success[0]", "value", "responseHeader", "QTime") ));

    assertEquals("127.0.0.1:56444_solr", Utils.getObjectByPath(m,false, "success[3]/key"));
    assertEquals("corestatus_test_shard2_replica_n3", Utils.getObjectByPath(m, false,asList("success[3]", "value", "core") ));
    assertEquals(5033L, Utils.getObjectByPath(m, false,asList("success[3]", "value", "responseHeader", "QTime") ));
    final int[] count = {0};
    @SuppressWarnings({"unchecked", "rawtypes"})
    NamedList nl = new NamedList(m);
    nl._forEachEntry("success", (o, o2) -> count[0]++);
    assertEquals(count[0], 4);
  }

  @SuppressWarnings({"unchecked"})
  public void testMergeJson() {
    Map<String, Object> sink = (Map<String, Object>) Utils.fromJSONString("{k2:v2, k1: {a:b, p:r, k21:{xx:yy}}}");
    assertTrue(Utils.mergeJson(sink, (Map<String, Object>) Utils.fromJSONString("k1:{a:c, e:f, p :null, k11:{a1:b1}, k21:{pp : qq}}")));

    assertEquals("v2", Utils.getObjectByPath(sink, true, "k2"));
    assertEquals("c", Utils.getObjectByPath(sink, true, "k1/a"));
    assertEquals("yy", Utils.getObjectByPath(sink, true, "k1/k21/xx"));
    assertEquals("qq", Utils.getObjectByPath(sink, true, "k1/k21/pp"));
    assertEquals("f", Utils.getObjectByPath(sink, true, "k1/e"));
    assertEquals("b1", Utils.getObjectByPath(sink, true, "k1/k11/a1"));

    sink = new HashMap<>();
    sink.put("legacyCloud", "false");
    assertTrue(Utils.mergeJson(sink, (Map<String, Object>) Utils.fromJSONString("collectionDefaults:{numShards:3 , nrtReplicas:2}")));
    assertEquals(3L, Utils.getObjectByPath(sink, true, ImmutableList.of(COLLECTION_DEF, NUM_SHARDS_PROP)));
    assertEquals(2L, Utils.getObjectByPath(sink, true, ImmutableList.of(COLLECTION_DEF, NRT_REPLICAS)));
  }
}
