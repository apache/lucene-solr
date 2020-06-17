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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseHS;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.response.SmileWriterTest;
import org.apache.solr.search.json.TestJsonRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL
public class TestSmileRequest extends SolrTestCaseJ4 {
  private static SolrTestCaseHS.SolrInstances servers;  // for distributed testing

  @BeforeClass
  public static void beforeTests() throws Exception {
    systemSetPropertySolrDisableShardsWhitelist("true");
    JSONTestUtil.failRepeatedKeys = true;
    initCore("solrconfig-tlog.xml", "schema_latest.xml");
  }

  public static void initServers() throws Exception {
    if (servers == null) {
      servers = new SolrTestCaseHS.SolrInstances(3, "solrconfig-tlog.xml", "schema_latest.xml");
    }
  }

  @AfterClass
  public static void afterTests() throws Exception {
    JSONTestUtil.failRepeatedKeys = false;
    if (servers != null) {
      servers.stop();
      servers = null;
    }
    systemClearPropertySolrDisableShardsWhitelist();
  }

  @Test
  public void testDistribJsonRequest() throws Exception {
    initServers();
    SolrTestCaseHS.Client client = servers.getClient(random().nextInt());
    client.tester = new SolrTestCaseHS.Client.Tester() {
      @Override
      public void assertJQ(SolrClient client, SolrParams args, String... tests) throws Exception {
        ((HttpSolrClient) client).setParser(SmileResponseParser.inst);
        QueryRequest query = new QueryRequest(args);
        String path = args.get("qt");
        if (path != null) {
          query.setPath(path);
        }
        NamedList<Object> rsp = client.request(query);
        @SuppressWarnings({"rawtypes"})
        Map m = rsp.asMap(5);
        String jsonStr = Utils.toJSONString(m);
        SolrTestCaseHS.matchJSON(jsonStr, tests);
      }
    };
    client.queryDefaults().set("shards", servers.getShards());
    TestJsonRequest.doJsonRequest(client, true);

  }

  //adding this to core adds the dependency on a few extra jars to our distribution.
  // So this is not added there
  public static class SmileResponseParser extends BinaryResponseParser {
    public static final SmileResponseParser inst = new SmileResponseParser();

    @Override
    public String getWriterType() {
      return "smile";
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public NamedList<Object> processResponse(InputStream body, String encoding) {
      try {
        Map m = (Map) SmileWriterTest.decodeSmile(body);
        return new NamedList(m);
      } catch (IOException e) {
        throw new RuntimeException(e);

      }
    }

  }
}
