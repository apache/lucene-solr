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
    SolrInputDocument document1 = sdoc("id", 1, "parent_s", "X",
        "child1_s", sdoc("id", 2, "child_s", "y"),
        "child2_s", sdoc("id", 3, "child_s", "z"));

    List<ContentStream> streams = new ArrayList<>( 1 );
    final String xmlDoc = ClientUtils.toXML(document1);
    if( xmlDoc.length() > 1 ) {
      streams.add( new ContentStreamBase.StringStream( xmlDoc ) );
    }

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

    indexDeeplyNestedSolrInputDocumentsDirectly(document1);
  }

  private long getNewClock() {
    long time = System.currentTimeMillis();
    return time << 20;
  }

  private void indexDeeplyNestedSolrInputDocumentsDirectly(SolrInputDocument... docs) throws IOException {
//    final String value = "Kittens!!! \u20AC";
//    final String reqDoc = "<add><doc><field name=\"id\">42</field><field name=\"subject\">"+value+"</field></doc></add>";
//    .request( "/update?"+CommonParams.STREAM_BODY+"="+URLEncoder.encode(reqDoc, "UTF-8"), null );
//    h.getCore().execute(new UpdateRequestHandler(), , new SolrQueryResponse());
    SolrQueryRequest coreReq = new LocalSolrQueryRequest(h.getCore(), params("update.chain", "deeply-nested"));
    AddUpdateCommand updateCmd = new AddUpdateCommand(coreReq);
    for (SolrInputDocument doc: docs) {
      long version = getNewClock();
      updateCmd.setVersion(Math.abs(version));
      updateCmd.solrDoc = doc;
      h.getCore().getUpdateHandler().addDoc(updateCmd);
      updateCmd.clear();
    }
    assertU(commit());
  }
}
