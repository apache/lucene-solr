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

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.util.SolrLogPostTool.LogRecordReader;

import org.junit.Test;

public class SolrLogPostToolTest extends SolrTestCaseJ4 {


  @Test
  public void testQueryRecord() throws Exception{
    String record = "2019-12-09 15:05:11.931 INFO  (qtp2103763750-21) [c:logs4 s:shard1 r:core_node2 x:logs4_shard1_replica_n1] o.a.s.c.S.Request [logs4_shard1_replica_n1]  webapp=/solr path=/select params={q=*:*&_=1575835181759&shards.purpose=36&isShard=true&wt=javabin&distrib=false} hits=234868 status=0 QTime=8\n";
    List<SolrInputDocument> docs = readDocs(record);
    assertEquals(docs.size(), 1);
    SolrInputDocument doc = docs.get(0);

    SolrInputField query = doc.getField("q_s");
    SolrInputField date = doc.getField("date_dt");
    SolrInputField time_minute = doc.getField("time_minute_s");
    SolrInputField time_ten_second = doc.getField("time_ten_second_s");
    SolrInputField collection = doc.getField("collection_s");
    SolrInputField path = doc.getField("path_s");
    SolrInputField hits = doc.getField("hits_l");
    SolrInputField type = doc.getField("type_s");
    SolrInputField status = doc.getField("status_s");
    SolrInputField shard = doc.getField("shard_s");
    SolrInputField replica = doc.getField("replica_s");
    SolrInputField core = doc.getField("core_s");
    SolrInputField wt = doc.getField("wt_s");
    SolrInputField distrib = doc.getField("distrib_s");
    SolrInputField isShard = doc.getField("isShard_s");
    SolrInputField ids = doc.getField("ids_s");
    SolrInputField shards = doc.getField("shards_s");
    SolrInputField purpose = doc.getField("purpose_ss");
    Object[] purposes = purpose.getValues().toArray();

    assertEquals(query.getValue(), "*:*");
    assertEquals(date.getValue(), "2019-12-09T15:05:11.931Z");
    assertEquals(time_minute.getValue(), "2019-12-09T15:05:00Z");
    assertEquals(time_ten_second.getValue(), "2019-12-09T15:05:10Z");
    assertEquals(collection.getValue(), "logs4");
    assertEquals(path.getValue(), "/select");
    assertEquals(hits.getValue(), "234868");
    assertEquals(type.getValue(), "query");
    assertEquals(status.getValue(), "0");
    assertEquals(shard.getValue(), "shard1");
    assertEquals(replica.getValue(), "core_node2");
    assertEquals(core.getValue(), "logs4_shard1_replica_n1");
    assertEquals(wt.getValue(), "javabin");
    assertEquals(distrib.getValue(), "false");
    assertEquals(isShard.getValue(), "true");
    assertEquals(ids.getValue(), "false");
    assertEquals(shards.getValue(), "false");
    assertEquals("GET_TOP_IDS", purposes[0].toString());
    assertEquals("REFINE_FACETS", purposes[1].toString());
  }

  // Requests which have multiple copies of the same param should be parsed so that the first param value only is
  // indexed, since the log schema expects many of these to be single-valued fields and will throw errors if multiple
  // values are received.
  @Test
  public void testRecordsFirstInstanceOfSingleValuedParams() throws Exception {
    final String record = "2019-12-09 15:05:01.931 INFO  (qtp2103763750-21) [c:logs4 s:shard1 r:core_node2 x:logs4_shard1_replica_n1] o.a.s.c.S.Request [logs4_shard1_replica_n1]  webapp=/solr path=/select params={q=*:*&q=inStock:true&_=1575835181759&shards.purpose=36&isShard=true&wt=javabin&wt=xml&distrib=false} hits=234868 status=0 QTime=8\n";

    List<SolrInputDocument> docs = readDocs(record);
    assertEquals(docs.size(), 1);
    SolrInputDocument doc = docs.get(0);

    assertEquals(doc.getFieldValues("q_s").size(), 1);
    assertEquals(doc.getFieldValue("q_s"), "*:*");

    assertEquals(doc.getFieldValues("wt_s").size(), 1);
    assertEquals(doc.getFieldValue("wt_s"), "javabin");
  }

  @Test
  public void testRTGRecord() throws Exception {
    final String record = "2020-03-19 20:00:30.845 INFO  (qtp1635378213-20354) [c:logs4 s:shard8 r:core_node63 x:logs4_shard8_replica_n60] o.a.s.c.S.Request [logs4_shard8_replica_n60]  webapp=/solr path=/get params={qt=/get&_stateVer_=logs4:104&ids=id1&ids=id2&ids=id3&wt=javabin&version=2} status=0 QTime=61";

    List<SolrInputDocument> docs = readDocs(record);
    assertEquals(docs.size(), 1);
    SolrInputDocument doc = docs.get(0);

    assertEquals(doc.getField("type_s").getValue(), "get");
    assertEquals(doc.getField("date_dt").getValue(), "2020-03-19T20:00:30.845Z");
    assertEquals(doc.getField("collection_s").getValue(), "logs4");
    assertEquals(doc.getField("path_s").getValue(), "/get");
    assertEquals(doc.getField("status_s").getValue(), "0");
    assertEquals(doc.getField("shard_s").getValue(), "shard8");
    assertEquals(doc.getField("replica_s").getValue(), "core_node63");
    assertEquals(doc.getField("core_s").getValue(), "logs4_shard8_replica_n60");
    assertEquals(doc.getField("wt_s").getValue(), "javabin");
    assertEquals(doc.getField("distrib_s").getValue(), "true");
    assertEquals(doc.getField("ids_s").getValue(), "false");
  }

  @Test
  public void testUpdateRecords() throws Exception{
    String record = "2019-12-25 20:38:23.498 INFO  (qtp2103763750-126) [c:logs3 s:shard1 r:core_node2 x:logs3_shard1_replica_n1] o.a.s.u.p.LogUpdateProcessorFactory [logs3_shard1_replica_n1]  webapp=/solr path=/update params={commitWithin=1000&overwrite=true&wt=json&_=1577306114481}{deleteByQuery=*:* (-1653925534487281664)} 0 11\n" +
                    "2019-12-25 20:42:13.411 INFO  (qtp2103763750-303) [c:logs5 s:shard1 r:core_node2 x:logs5_shard1_replica_n1] o.a.s.u.p.LogUpdateProcessorFactory [logs5_shard1_replica_n1]  webapp=/solr path=/update params={commitWithin=1000&overwrite=true&wt=json&_=1577306114481}{delete=[03bbe975-728a-4df8-aa25-fe25049dc0ef (-1653925775577972736)]} 0 1\n";
    List<SolrInputDocument> docs = readDocs(record);
    assertEquals(docs.size(), 2);
    SolrInputDocument doc = docs.get(0);
    SolrInputField date = doc.getField("date_dt");
    SolrInputField type = doc.getField("type_s");
    SolrInputField core = doc.getField("core_s");
    SolrInputField collection = doc.getField("collection_s");
    assertEquals(date.getValue(), "2019-12-25T20:38:23.498Z");
    assertEquals(type.getValue(), "deleteByQuery");
    assertEquals(collection.getValue(), "logs3");
    assertEquals(core.getValue(), "logs3_shard1_replica_n1");

    SolrInputDocument doc1 = docs.get(1);
    SolrInputField date1 = doc1.getField("date_dt");
    SolrInputField type1 = doc1.getField("type_s");
    SolrInputField core1 = doc1.getField("core_s");
    SolrInputField collection1= doc1.getField("collection_s");
    assertEquals(date1.getValue(), "2019-12-25T20:42:13.411Z");
    assertEquals(type1.getValue(), "delete");
    assertEquals(collection1.getValue(), "logs5");
    assertEquals(core1.getValue(), "logs5_shard1_replica_n1");
  }

  @Test
  public void testErrorRecord() throws Exception{
    String record = "2019-12-31 01:49:53.251 ERROR (qtp2103763750-240) [c:logs6 s:shard1 r:core_node2 x:logs6_shard1_replica_n1] o.a.s.h.RequestHandlerBase org.apache.solr.common.SolrException: org.apache.solr.search.SyntaxError: Cannot parse 'id:[* TO *': Encountered \"<EOF>\" at line 1, column 10.\n" +
        "Was expecting one of:\n" +
        "    \"]\" ...\n" +
        "    \"}\" ...\n" +
        "    \n" +
        "\tat org.apache.solr.handler.component.QueryComponent.prepare(QueryComponent.java:218)\n" +
        "\tat org.apache.solr.handler.component.SearchHandler.handleRequestBody(SearchHandler.java:302)\n" +
        "\tat org.apache.solr.handler.RequestHandlerBase.handleRequest(RequestHandlerBase.java:197)\n" +
        "\tat org.apache.solr.core.SolrCore.execute(SolrCore.java:2582)\n" +
        "\tat org.apache.solr.servlet.HttpSolrCall.execute(HttpSolrCall.java:799)\n" +
        "\tat org.apache.solr.servlet.HttpSolrCall.call(HttpSolrCall.java:578)\n" +
        "\tat org.apache.solr.servlet.SolrDispatchFilter.doFilter(SolrDispatchFilter.java:419)\n" +
        "\tat org.apache.solr.servlet.SolrDispatchFilter.doFilter(SolrDispatchFilter.java:351)\n" +
        "\tat org.eclipse.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1602)\n" +
        "\tat org.eclipse.jetty.servlet.ServletHandler.doHandle(ServletHandler.java:540)\n" +
        "\tat org.eclipse.jetty.server.handler.ScopedHandler.handle(ScopedHandler.java:146)\n" +
        "\tat org.eclipse.jetty.security.SecurityHandler.handle(SecurityHandler.java:548)\n" +
        "\tat org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:132)\n" +
        "\tat org.eclipse.jetty.server.handler.ScopedHandler.nextHandle(ScopedHandler.java:257)\n" +
        "\tat org.eclipse.jetty.server.session.SessionHandler.doHandle(SessionHandler.java:1711)\n" +
        "\tat org.eclipse.jetty.server.handler.ScopedHandler.nextHandle(ScopedHandler.java:255)\n" +
        "\tat org.eclipse.jetty.server.handler.ContextHandler.doHandle(ContextHandler.java:1347)\n" +
        "\tat org.eclipse.jetty.server.handler.ScopedHandler.nextScope(ScopedHandler.java:203)\n" +
        "\tat org.eclipse.jetty.servlet.ServletHandler.doScope(ServletHandler.java:480)\n" +
        "\tat org.eclipse.jetty.server.session.SessionHandler.doScope(SessionHandler.java:1678)\n" +
        "\tat org.eclipse.jetty.server.handler.ScopedHandler.nextScope(ScopedHandler.java:201)\n" +
        "\tat org.eclipse.jetty.server.handler.ContextHandler.doScope(ContextHandler.java:1249)\n" +
        "\tat org.eclipse.jetty.server.handler.ScopedHandler.handle(ScopedHandler.java:144)\n" +
        "\tat org.eclipse.jetty.server.handler.ContextHandlerCollection.handle(ContextHandlerCollection.java:220)\n" +
        "\tat org.eclipse.jetty.server.handler.HandlerCollection.handle(HandlerCollection.java:152)\n" +
        "\tat org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:132)\n" +
        "\tat org.eclipse.jetty.rewrite.handler.RewriteHandler.handle(RewriteHandler.java:335)\n" +
        "\tat org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:132)\n" +
        "\tat org.eclipse.jetty.server.Server.handle(Server.java:505)\n" +
        "\tat org.eclipse.jetty.server.HttpChannel.handle(HttpChannel.java:370)\n" +
        "\tat org.eclipse.jetty.server.HttpConnection.onFillable(HttpConnection.java:267)\n" +
        "\tat org.eclipse.jetty.io.AbstractConnection$ReadCallback.succeeded(AbstractConnection.java:305)\n" +
        "\tat org.eclipse.jetty.io.FillInterest.fillable(FillInterest.java:103)\n" +
        "\tat org.eclipse.jetty.io.ChannelEndPoint$2.run(ChannelEndPoint.java:117)\n" +
        "\tat org.eclipse.jetty.util.thread.strategy.EatWhatYouKill.runTask(EatWhatYouKill.java:333)\n" +
        "\tat org.eclipse.jetty.util.thread.strategy.EatWhatYouKill.doProduce(EatWhatYouKill.java:310)\n" +
        "\tat org.eclipse.jetty.util.thread.strategy.EatWhatYouKill.tryProduce(EatWhatYouKill.java:168)\n" +
        "\tat org.eclipse.jetty.util.thread.strategy.EatWhatYouKill.run(EatWhatYouKill.java:126)\n" +
        "\tat org.eclipse.jetty.util.thread.ReservedThreadExecutor$ReservedThread.run(ReservedThreadExecutor.java:366)\n" +
        "\tat org.eclipse.jetty.util.thread.QueuedThreadPool.runJob(QueuedThreadPool.java:781)\n" +
        "\tat org.eclipse.jetty.util.thread.QueuedThreadPool$Runner.run(QueuedThreadPool.java:917)\n" +
        "\tat java.base/java.lang.Thread.run(Thread.java:834)\n" +
        "Caused by: org.apache.solr.search.SyntaxError: Cannot parse 'id:[* TO *': Encountered \"<EOF>\" at line 1, column 10.\n" +
        "Was expecting one of:\n" +
        "    \"]\" ...\n" +
        "    \"}\" ...\n" +
        "    \n" +
        "\tat org.apache.solr.parser.SolrQueryParserBase.parse(SolrQueryParserBase.java:266)\n" +
        "\tat org.apache.solr.search.LuceneQParser.parse(LuceneQParser.java:49)\n" +
        "\tat org.apache.solr.search.QParser.getQuery(QParser.java:174)\n" +
        "\tat org.apache.solr.handler.component.QueryComponent.prepare(QueryComponent.java:160)\n" +
        "\t... 41 more\n" +
        "Caused by: org.apache.solr.parser.ParseException: Encountered \"<EOF>\" at line 1, column 10.\n" +
        "Was expecting one of:\n" +
        "    \"]\" ...\n" +
        "    \"}\" ...\n" +
        "    \n" +
        "\tat org.apache.solr.parser.QueryParser.generateParseException(QueryParser.java:885)\n" +
        "\tat org.apache.solr.parser.QueryParser.jj_consume_token(QueryParser.java:767)\n" +
        "\tat org.apache.solr.parser.QueryParser.Term(QueryParser.java:479)\n" +
        "\tat org.apache.solr.parser.QueryParser.Clause(QueryParser.java:278)\n" +
        "\tat org.apache.solr.parser.QueryParser.Query(QueryParser.java:162)\n" +
        "\tat org.apache.solr.parser.QueryParser.TopLevelQuery(QueryParser.java:131)\n" +
        "\tat org.apache.solr.parser.SolrQueryParserBase.parse(SolrQueryParserBase.java:262)\n" +
        "\t... 44 more\n" +
        "\n"+
        "2019-12-09 15:05:01.931 INFO  (qtp2103763750-21) [c:logs4 s:shard1 r:core_node2 x:logs4_shard1_replica_n1] o.a.s.c.S.Request [logs4_shard1_replica_n1]  webapp=/solr path=/select params={q=*:*&_=1575835181759&isShard=true&wt=javabin&distrib=false} hits=234868 status=0 QTime=8\n";
    List<SolrInputDocument> docs = readDocs(record);
    assertEquals(docs.size(), 2);
    SolrInputDocument doc = docs.get(0);
    SolrInputField date = doc.getField("date_dt");
    SolrInputField type = doc.getField("type_s");
    SolrInputField shard = doc.getField("shard_s");
    SolrInputField replica = doc.getField("replica_s");
    SolrInputField core = doc.getField("core_s");
    SolrInputField stack = doc.getField("stack_t");
    SolrInputField root = doc.getField("root_cause_t");
    SolrInputField collection = doc.getField("collection_s");


    assertEquals(date.getValue(), "2019-12-31T01:49:53.251Z");
    assertEquals(type.getValue(), "error");
    assertEquals(collection.getValue(), "logs6");


    assertEquals(shard.getValue(), "shard1");
    assertEquals(replica.getValue(), "core_node2");
    assertEquals(core.getValue(), "logs6_shard1_replica_n1");
    assertTrue(stack.getValue().toString().contains(root.getValue().toString()));

    SolrInputDocument doc1 = docs.get(1);
    SolrInputField date1 = doc1.getField("date_dt");
    SolrInputField type1 = doc1.getField("type_s");
    assertEquals(date1.getValue(), "2019-12-09T15:05:01.931Z");
    assertEquals(type1.getValue(), "query");

  }

  @Test
  public void testCommit() throws Exception{
    String record = "2019-12-16T14:20:19.708 INFO  (qtp812143047-22671) [c:production_201912 s:shard128 r:core_node7 x:production_201912_shard128_replica] o.a.s.u.DirectUpdateHandler2 start commit{_version_=1653086376121335808,optimize=false,openSearcher=true,waitSearcher=true,expungeDeletes=false,softCommit=false,prepareCommit=false}\n";
    List<SolrInputDocument> docs = readDocs(record);
    assertEquals(docs.size(), 1);
    SolrInputDocument doc = docs.get(0);

    SolrInputField date = doc.getField("date_dt");
    SolrInputField type = doc.getField("type_s");
    SolrInputField shard = doc.getField("shard_s");
    SolrInputField replica = doc.getField("replica_s");
    SolrInputField core = doc.getField("core_s");
    SolrInputField openSearcher = doc.getField("open_searcher_s");
    SolrInputField softCommit = doc.getField("soft_commit_s");
    SolrInputField collection = doc.getField("collection_s");

    assertEquals(date.getValue(), "2019-12-16T14:20:19.708Z");
    assertEquals(type.getValue(), "commit");
    assertEquals(shard.getValue(), "shard128");
    assertEquals(replica.getValue(), "core_node7");
    assertEquals(core.getValue(), "production_201912_shard128_replica");
    assertEquals(openSearcher.getValue(), "true");
    assertEquals(softCommit.getValue(), "false");
    assertEquals(collection.getValue(), "production_201912");
  }

  @Test
  public void testNewSearcher() throws Exception{
    String record = "2019-12-16 19:00:23.931 INFO  (searcherExecutor-66-thread-1) [   ] o.a.s.c.SolrCore [production_cv_month_201912_shard35_replica_n1] Registered new searcher Searcher@16ef5fac[production_cv_month_201912_shard35_replica_n1] ...";
    List<SolrInputDocument> docs = readDocs(record);
    assertEquals(docs.size(), 1);
    SolrInputDocument doc = docs.get(0);
    SolrInputField date = doc.getField("date_dt");
    SolrInputField type = doc.getField("type_s");
    SolrInputField core = doc.getField("core_s");
    assertEquals(date.getValue(), "2019-12-16T19:00:23.931Z");
    assertEquals(type.getValue(), "newSearcher");
    assertEquals(core.getValue(), "production_cv_month_201912_shard35_replica_n1");
  }

  // Ensure SolrLogPostTool parses _all_ log lines into searchable records
  @Test
  public void testOtherRecord() throws Exception {
    final String record = "2020-06-11 11:59:08.386 INFO  (main) [   ] o.a.s.c.c.ZkStateReader Updated live nodes from ZooKeeper... (0) -> (2)";
    final List<SolrInputDocument> docs = readDocs(record);
    assertEquals(docs.size(), 1);

    SolrInputDocument doc = docs.get(0);
    final Collection<String> fields = doc.getFieldNames();
    assertEquals(5, fields.size());
    assertEquals("2020-06-11T11:59:08.386Z", doc.getField("date_dt").getValue());
    assertEquals("other", doc.getField("type_s").getValue());
    assertEquals(record, doc.getField("line_t").getValue());
  }

  private List<SolrInputDocument> readDocs(String records) throws Exception {
    BufferedReader bufferedReader = new BufferedReader(new StringReader(records));
    ArrayList<SolrInputDocument> list = new ArrayList<>();

    try {
      LogRecordReader logRecordReader = new SolrLogPostTool.LogRecordReader(bufferedReader);
      SolrInputDocument doc = null;
      while ((doc = logRecordReader.readRecord()) != null) {
        list.add(doc);
      }
    } finally {
      bufferedReader.close();
    }
    return list;
  }

}