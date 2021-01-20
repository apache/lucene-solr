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
package org.apache.solr.handler;

import java.util.ArrayList;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.*;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TODO -- this needs to actually test the results/query etc
 */
public class MoreLikeThisHandlerTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void moreLikeThisBeforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testInterface() throws Exception
  {
    SolrCore core = h.getCore();

    ModifiableSolrParams params = new ModifiableSolrParams();

    assertU(adoc("id","42","name","Tom Cruise","subword","Top Gun","subword","Risky Business","subword","The Color of Money","subword","Minority Report","subword", "Days of Thunder","subword", "Eyes Wide Shut","subword", "Far and Away", "foo_ti","10"));
    assertU(adoc("id","43","name","Tom Hanks","subword","The Green Mile","subword","Forest Gump","subword","Philadelphia Story","subword","Big","subword","Cast Away", "foo_ti","10"));
    assertU(adoc("id","44","name","Harrison Ford","subword","Star Wars","subword","Indiana Jones","subword","Patriot Games","subword","Regarding Henry"));
    assertU(adoc("id","45","name","George Harrison","subword","Yellow Submarine","subword","Help","subword","Magical Mystery Tour","subword","Sgt. Peppers Lonley Hearts Club Band"));
    assertU(adoc("id","46","name","Nicole Kidman","subword","Batman","subword","Days of Thunder","subword","Eyes Wide Shut","subword","Far and Away"));
    assertU(commit());

    params.set(MoreLikeThisParams.MLT, "true");
    params.set(MoreLikeThisParams.SIMILARITY_FIELDS, "name,subword");
    params.set(MoreLikeThisParams.INTERESTING_TERMS, "details");
    params.set(MoreLikeThisParams.MIN_TERM_FREQ,"1");
    params.set(MoreLikeThisParams.MIN_DOC_FREQ,"1");
    params.set("indent","true");

    // requires 'q' or a single content stream
    SolrException ex = expectThrows(SolrException.class, () -> {
      try (MoreLikeThisHandler mlt = new MoreLikeThisHandler();
           SolrQueryRequestBase req = new SolrQueryRequestBase(core, params) {}) {
        mlt.handleRequestBody(req, new SolrQueryResponse());
      }
    });
    assertEquals(ex.getMessage(), MoreLikeThisHandler.ERR_MSG_QUERY_OR_TEXT_REQUIRED);
    assertEquals(ex.code(), SolrException.ErrorCode.BAD_REQUEST.code);

    // requires a single content stream (more than one is not supported).
    ex = expectThrows(SolrException.class, () -> {
      try (MoreLikeThisHandler mlt = new MoreLikeThisHandler();
           SolrQueryRequestBase req = new SolrQueryRequestBase(core, params) {}) {
        ArrayList<ContentStream> streams = new ArrayList<>(2);
        streams.add(new ContentStreamBase.StringStream("hello"));
        streams.add(new ContentStreamBase.StringStream("there"));
        req.setContentStreams(streams);
        mlt.handleRequestBody(req, new SolrQueryResponse());
      }
    });
    assertEquals(ex.getMessage(), MoreLikeThisHandler.ERR_MSG_SINGLE_STREAM_ONLY);
    assertEquals(ex.code(), SolrException.ErrorCode.BAD_REQUEST.code);

    params.set(CommonParams.Q, "id:42");

    try (SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params)) {
      assertQ("morelikethis - tom cruise", mltreq,
          "//result/doc[1]/str[@name='id'][.='46']",
          "//result/doc[2]/str[@name='id'][.='43']");
    }

    params.set(MoreLikeThisParams.BOOST, "true");

    try (SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params)) {
      assertQ("morelikethis - tom cruise", mltreq,
          "//result/doc[1]/str[@name='id'][.='46']",
          "//result/doc[2]/str[@name='id'][.='43']");
    }
    
    params.set(CommonParams.Q, "id:44");
    try (SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params)) {
      assertQ("morelike this - harrison ford", mltreq,
          "//result/doc[1]/str[@name='id'][.='45']");
    }

    // test MoreLikeThis debug
    params.set(CommonParams.DEBUG_QUERY, "true");
    try (SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params)) {
      assertQ("morelike this - harrison ford", mltreq,
          "//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/str[@name='rawMLTQuery']",
          "//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/str[@name='boostedMLTQuery']",
          "//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/str[@name='realMLTQuery']",
          "//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/lst[@name='explain']/str[@name='45']"
      );
    }

    // test that qparser plugins work
    params.remove(CommonParams.DEBUG_QUERY);
    params.set(CommonParams.Q, "{!field f=id}44");
    try (SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params)) {
      assertQ(mltreq, "//result/doc[1]/str[@name='id'][.='45']");
    }

    params.set(CommonParams.Q, "id:42");
    params.set(MoreLikeThisParams.QF,"name^5.0 subword^0.1");
    try (SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params)) {
      assertQ("morelikethis with weights", mltreq,
          "//result/doc[1]/str[@name='id'][.='43']",
          "//result/doc[2]/str[@name='id'][.='46']");
    }

    // test that qparser plugins work w/ the MoreLikeThisHandler
    params.set(CommonParams.QT, "/mlt");
    params.set(CommonParams.Q, "{!field f=id}44");
    try (SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params)) {
      assertQ(mltreq, "//result/doc[1]/str[@name='id'][.='45']");
    }

    // test that debugging works (test for MoreLikeThis*Handler*)
    params.set(CommonParams.QT, "/mlt");
    params.set(CommonParams.DEBUG_QUERY, "true");
    try (SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params)) {
      assertQ(mltreq,
          "//result/doc[1]/str[@name='id'][.='45']",
          "//lst[@name='debug']/lst[@name='explain']"
      );
    }
  }

  @Test
  public void testMultifieldSimilarity() throws Exception
  {
    SolrCore core = h.getCore();
    ModifiableSolrParams params = new ModifiableSolrParams();

    assertU(adoc("id", "1", "name", "aaa bbb ccc", "subword", "        zzz"));
    assertU(adoc("id", "2", "name", "    bbb ccc", "subword", "    bbb zzz"));
    assertU(adoc("id", "3", "name", "        ccc", "subword", "aaa bbb zzz"));
    assertU(adoc("id", "4", "name", "        ccc", "subword", "    bbb    "));
    assertU(commit());

    params.set(CommonParams.QT, "/mlt");
    params.set(MoreLikeThisParams.MLT, "true");
    params.set(MoreLikeThisParams.SIMILARITY_FIELDS, "name,subword");
    params.set(MoreLikeThisParams.INTERESTING_TERMS, "details");
    params.set(MoreLikeThisParams.MIN_TERM_FREQ, "1");
    params.set(MoreLikeThisParams.MIN_DOC_FREQ, "2");
    params.set(MoreLikeThisParams.BOOST, true);
    params.set("indent", "true");

    try (SolrQueryRequestBase req = new SolrQueryRequestBase(core, params) {}) {
      ArrayList<ContentStream> streams = new ArrayList<>(2);
      streams.add(new ContentStreamBase.StringStream("bbb", "zzz"));
      req.setContentStreams(streams);

      // Make sure we have terms from both fields in the interestingTerms array and all documents have been
      // retrieved as matching.
      assertQ(req,
          "//lst[@name = 'interestingTerms']/float[@name = 'subword:bbb']",
          "//lst[@name = 'interestingTerms']/float[@name = 'name:bbb']",
          "//result[@name = 'response' and @numFound = '4']");
    }
  }
}
