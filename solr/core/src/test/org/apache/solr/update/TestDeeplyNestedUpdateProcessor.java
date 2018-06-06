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

package org.apache.solr.update;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrQueryParser;
import org.apache.solr.servlet.DirectSolrConnection;
import org.apache.solr.servlet.SolrRequestParsers;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDeeplyNestedUpdateProcessor extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-update-processor-chains.xml", "schema15.xml");
  }

  @Before
  public void before() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testDeeplyNestedURP() throws Exception {
    final String jDoc = "{\n" +
        "    \"add\": {\n" +
        "        \"doc\": {\n" +
        "            \"id\": \"1\",\n" +
        "            \"children\": [\n" +
        "                {\n" +
        "                    \"id\": \"2\",\n" +
        "                    \"foo_s\": \"Yaz\"\n" +
        "                    \"grandChild\": \n" +
        "                          {\n" +
        "                             \"id\": \"4\",\n" +
        "                             \"foo_s\": \"Jazz\"\n" +
        "                          },\n" +
        "                },\n" +
        "                {\n" +
        "                    \"id\": \"3\",\n" +
        "                    \"foo_s\": \"Bar\"\n" +
        "                }\n" +
        "            ]\n" +
        "        }\n" +
        "    }\n" +
        "}";

    List<ContentStream> streams = new ArrayList<>( 1 );
    streams.add( new ContentStreamBase.StringStream( jDoc ) );

    SolrQueryRequest req;
    try {
      req = new SolrRequestParsers(h.getCore().getSolrConfig()).buildRequestFrom( h.getCore(), params("update.chain", "deeply-nested"), streams );
      SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      h.getCore().execute( h.getCore().getRequestHandler("/update"), req, rsp );
      if( rsp.getException() != null ) {
        throw rsp.getException();
      }
    } catch (Exception e) {
      throw e;
    }
    System.out.println();
  }
}
