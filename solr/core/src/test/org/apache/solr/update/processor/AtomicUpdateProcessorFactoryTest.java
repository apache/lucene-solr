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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.junit.BeforeClass;

/**
 * test class for @see AtomicUpdateProcessorFactory
 */
public class AtomicUpdateProcessorFactoryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log","true");
    initCore("solrconfig.xml", "schema.xml");
  }

  public void testWrongAtomicOpPassed() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams()
        .add("processor", "Atomic")
        .add("atomic.cat", "delete")
        .add("commit", "true");
    try (SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params)) {
      AddUpdateCommand cmd = new AddUpdateCommand(req);
      AtomicUpdateProcessorFactory factory = new AtomicUpdateProcessorFactory();
      factory.inform(h.getCore());
      factory.getInstance(cmd.getReq(), new SolrQueryResponse(),
          null).processAdd(cmd);
    } catch (SolrException e) {
      assertEquals("Unexpected param(s) for AtomicUpdateProcessor, invalid atomic op passed: 'delete'",
          e.getMessage());
    }
  }

  public void testNoUniqueIdPassed() throws Exception { //TODO
    ModifiableSolrParams params = new ModifiableSolrParams()
        .add("processor", "atomic")
        .add("atomic.cat", "add")
        .add("commit", "true");
    try (SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params)) {
      AddUpdateCommand cmd = new AddUpdateCommand(req);
      cmd.solrDoc = new SolrInputDocument();
      cmd.solrDoc.addField("title", 1);
      AtomicUpdateProcessorFactory factory = new AtomicUpdateProcessorFactory();
      factory.inform(h.getCore());
      factory.getInstance(cmd.getReq(), new SolrQueryResponse(),
          null).processAdd(cmd);
    } catch (SolrException e) {
      assertEquals("Document passed with no unique field: 'id'", e.getMessage());
    }
  }

  public void testBasics() throws Exception {

    ModifiableSolrParams params = new ModifiableSolrParams()
        .add("processor", "atomic")
        .add("atomic.cat", "add")
        .add("atomic.title", "set")
        .add("atomic.count_i", "set")
        .add("atomic.name_s", "set")
        .add("atomic.multiDefault", "set")
        .add("commit", "true");
    try (SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params)) {
      AddUpdateCommand cmd = new AddUpdateCommand(req);

      cmd.solrDoc = new SolrInputDocument();
      cmd.solrDoc.addField("id", 1);
      cmd.solrDoc.addField("cat", "human");
      cmd.solrDoc.addField("title", "Mr");
      cmd.solrDoc.addField("count_i", 20);
      cmd.solrDoc.addField("name_s", "Virat");
      cmd.solrDoc.addField("multiDefault", "Delhi");

      h.getCore()
          .getUpdateProcessorChain(params)
          .createProcessor(cmd.getReq(), new SolrQueryResponse())
          .processAdd(cmd);
    }

    assertU(commit());

    assertQ("Check the total number of docs",
        req("q", "id:1")
        , "//result[@numFound=1]");

    assertQ("Check the total number of docs",
        req("q", "cat:human")
        , "//result[@numFound=1]");

    assertQ("Check the total number of docs",
        req("q", "title:Mr")
        , "//result[@numFound=1]");

    assertQ("Check the total number of docs",
        req("q", "count_i:20")
        , "//result[@numFound=1]");

    assertQ("Check the total number of docs",
        req("q", "name_s:Virat")
        , "//result[@numFound=1]");

    assertQ("Check the total number of docs",
        req("q", "multiDefault:Delhi")
        , "//result[@numFound=1]");

    params = new ModifiableSolrParams()
        .add("processor", "atomic")
        .add("atomic.cat", "add-distinct")
        .add("atomic.title", "set")
        .add("atomic.count_i", "inc")
        .add("atomic.name_s", "remove")
        .add("atomic.multiDefault", "removeregex")
        .add("commit", "true");
    try (SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params)) {
      AddUpdateCommand cmd = new AddUpdateCommand(req);

      cmd.solrDoc = new SolrInputDocument();
      cmd.solrDoc.addField("id", 1);
      cmd.solrDoc.addField("cat", Arrays.asList(new String[]{"human", "human", "animal", "animal"}));
      cmd.solrDoc.addField("title", "Dr");
      cmd.solrDoc.addField("count_i", 20);
      cmd.solrDoc.addField("name_s", "Virat");
      cmd.solrDoc.addField("multiDefault", ".elh.");
      h.getCore()
          .getUpdateProcessorChain(params)
          .createProcessor(cmd.getReq(), new SolrQueryResponse())
          .processAdd(cmd);
    }

    assertU(commit());

    assertQ("Check the total number of docs",
        req("q", "id:1")
        , "//result[@numFound=1]");

    assertQ("Check the total number of docs",
        req("q", "cat:human")
        , "//result[@numFound=1]");

    assertQ("Check the total number of docs",
        req("q", "cat:animal")
        , "//result[@numFound=1]");

    assertQ(req("q", "id:1", "indent", "true"),
        "//doc/arr[@name='cat'][count(str)=2]");

    assertQ("Check the total number of docs",
        req("q", "title:Mr")
        , "//result[@numFound=0]");

    assertQ("Check the total number of docs",
        req("q", "title:Dr")
        , "//result[@numFound=1]");

    assertQ("Check the total number of docs",
        req("q", "count_i:20")
        , "//result[@numFound=0]");

    assertQ("Check the total number of docs",
        req("q", "count_i:40")
        , "//result[@numFound=1]");

    assertQ("Check the total number of docs",
        req("q", "name_s:Virat")
        , "//result[@numFound=0]");

    assertQ("Check the total number of docs",
        req("q", "multiDefault:Delhi")
        , "//result[@numFound=0]");

  }

  public void testMultipleThreads() throws Exception {
    clearIndex();
    String[] strings = new String[5];
    for (int i=0; i<5; i++) {
      strings[i] = generateRandomString();
    }

    List<Thread> threads = new ArrayList<>(100);
    int finalCount = 0; //int_i

    AtomicUpdateProcessorFactory factory = new AtomicUpdateProcessorFactory();
    factory.inform(h.getCore());

    for (int i = 0; i < 10; i++) {
      int index = random().nextInt(5);
      Thread t = new Thread() {
        @Override
        public void run() {
          ModifiableSolrParams params = new ModifiableSolrParams()
              .add("processor", "atomic")
              .add("atomic.cat", "add")
              .add("atomic.int_i", "inc")
              .add("commit","true");
          try (SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params)) {
            AddUpdateCommand cmd = new AddUpdateCommand(req);
            cmd.solrDoc = new SolrInputDocument();
            cmd.solrDoc.addField("id", 10); //hardcoded id=10
            cmd.solrDoc.addField("cat", strings[index]);
            cmd.solrDoc.addField("int_i", index);
            SolrQueryResponse rsp = new SolrQueryResponse();
            factory.getInstance(cmd.getReq(), new SolrQueryResponse(),
                createDistributedUpdateProcessor(cmd.getReq(), rsp,
                    createRunUpdateProcessor(cmd.getReq(), rsp, null))).processAdd(cmd);
          } catch (IOException e) {
          }
        }
      };
      threads.add(t);
      t.start();
      finalCount += index; //int_i
    }

    for (Thread thread: threads) thread.join();

    assertU(commit());

    assertQ("Check the total number of docs",
        req("q", "id:10"), "//result[@numFound=1]");


    StringJoiner queryString = new StringJoiner(" ");
    for(String string: strings) {
      queryString.add(string);
    }

    assertQ("Check the total number of docs",
        req("q", "cat:(" + queryString.toString() + ")")
        , "//result[@numFound=1]");

    assertQ("Check the total number of docs",
        req("q", "int_i:" + finalCount)
        , "//result[@numFound=1]");

  }

  private UpdateRequestProcessor createRunUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new RunUpdateProcessorFactory().getInstance(req, rsp, next);
  }

  private String generateRandomString() {
    char[] chars = "abcdefghijklmnopqrstuvwxyz0123456789".toCharArray();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 20; i++) {
      char c = chars[random().nextInt(chars.length)];
      sb.append(c);
    }
    return sb.toString();
  }

}
