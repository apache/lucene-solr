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
package org.apache.solr.handler.dataimport;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.handler.dataimport.config.Entity;
import org.junit.Ignore;
import org.junit.Test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Test mailbox is like this: foldername(mailcount)
// top1(2) -> child11(6)
//         -> child12(0)
// top2(2) -> child21(1)
//                 -> grandchild211(2)
//                 -> grandchild212(1)
//         -> child22(2)

/**
 * Test for MailEntityProcessor. The tests are marked as ignored because we'd need a mail server (real or mocked) for
 * these to work.
 *
 * TODO: Find a way to make the tests actually test code
 *
 *
 * @see org.apache.solr.handler.dataimport.MailEntityProcessor
 * @since solr 1.4
 */
@Ignore("Needs a Mock Mail Server to work")
@SuppressWarnings("unchecked")
public class TestMailEntityProcessor extends AbstractDataImportHandlerTestCase {

  // Credentials
  private static final String user = "user";
  private static final String password = "password";
  private static final String host = "host";
  private static final String protocol = "imaps";

  private static Map<String, String> paramMap = new HashMap<>();

  @Test
  @Ignore("Needs a Mock Mail Server to work")
  public void testConnection() {
    // also tests recurse = false and default settings
    paramMap.put("folders", "top2");
    paramMap.put("recurse", "false");
    paramMap.put("processAttachement", "false");
    DataImporter di = new DataImporter();
    di.loadAndInit(getConfigFromMap(paramMap));
    Entity ent = di.getConfig().getEntities().get(0);
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals("top1 did not return 2 messages", swi.docs.size(), 2);
  }

  @Test
  @Ignore("Needs a Mock Mail Server to work")
  public void testRecursion() {
    paramMap.put("folders", "top2");
    paramMap.put("recurse", "true");
    paramMap.put("processAttachement", "false");
    DataImporter di = new DataImporter();
    di.loadAndInit(getConfigFromMap(paramMap));
    Entity ent = di.getConfig().getEntities().get(0);
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals("top2 and its children did not return 8 messages", swi.docs.size(), 8);
  }

  @Test
  @Ignore("Needs a Mock Mail Server to work")
  public void testExclude() {
    paramMap.put("folders", "top2");
    paramMap.put("recurse", "true");
    paramMap.put("processAttachement", "false");
    paramMap.put("exclude", ".*grandchild.*");
    DataImporter di = new DataImporter();
    di.loadAndInit(getConfigFromMap(paramMap));
    Entity ent = di.getConfig().getEntities().get(0);
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals("top2 and its direct children did not return 5 messages", swi.docs.size(), 5);
  }

  @Test
  @Ignore("Needs a Mock Mail Server to work")
  public void testInclude() {
    paramMap.put("folders", "top2");
    paramMap.put("recurse", "true");
    paramMap.put("processAttachement", "false");
    paramMap.put("include", ".*grandchild.*");
    DataImporter di = new DataImporter();
    di.loadAndInit(getConfigFromMap(paramMap));
    Entity ent = di.getConfig().getEntities().get(0);
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals("top2 and its direct children did not return 3 messages", swi.docs.size(), 3);
  }

  @Test
  @Ignore("Needs a Mock Mail Server to work")
  public void testIncludeAndExclude() {
    paramMap.put("folders", "top1,top2");
    paramMap.put("recurse", "true");
    paramMap.put("processAttachement", "false");
    paramMap.put("exclude", ".*top1.*");
    paramMap.put("include", ".*grandchild.*");
    DataImporter di = new DataImporter();
    di.loadAndInit(getConfigFromMap(paramMap));
    Entity ent = di.getConfig().getEntities().get(0);
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals("top2 and its direct children did not return 3 messages", swi.docs.size(), 3);
  }

  @Test
  @Ignore("Needs a Mock Mail Server to work")
  public void testFetchTimeSince() throws ParseException {
    paramMap.put("folders", "top1/child11");
    paramMap.put("recurse", "true");
    paramMap.put("processAttachement", "false");
    paramMap.put("fetchMailsSince", "2008-12-26 00:00:00");
    DataImporter di = new DataImporter();
    di.loadAndInit(getConfigFromMap(paramMap));
    Entity ent = di.getConfig().getEntities().get(0);
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals("top2 and its direct children did not return 3 messages", swi.docs.size(), 3);
  }

  private String getConfigFromMap(Map<String, String> params) {
    String conf =
            "<dataConfig>" +
                    "<document>" +
                    "<entity processor=\"org.apache.solr.handler.dataimport.MailEntityProcessor\" " +
                    "someconfig" +
                    "/>" +
                    "</document>" +
                    "</dataConfig>";
    params.put("user", user);
    params.put("password", password);
    params.put("host", host);
    params.put("protocol", protocol);
    StringBuilder attribs = new StringBuilder("");
    for (String key : params.keySet())
      attribs.append(" ").append(key).append("=" + "\"").append(params.get(key)).append("\"");
    attribs.append(" ");
    return conf.replace("someconfig", attribs.toString());
  }

  static class SolrWriterImpl extends SolrWriter {
    List<SolrInputDocument> docs = new ArrayList<>();
    Boolean deleteAllCalled;
    Boolean commitCalled;

    public SolrWriterImpl() {
      super(null, null);
    }

    @Override
    public boolean upload(SolrInputDocument doc) {
      return docs.add(doc);
    }


    @Override
    public void doDeleteAll() {
      deleteAllCalled = Boolean.TRUE;
    }

    @Override
    public void commit(boolean b) {
      commitCalled = Boolean.TRUE;
    }
  }
}
