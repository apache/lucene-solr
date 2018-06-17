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
package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TimeUnits;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.Http2SolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.SuppressForbidden;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

@SuppressSSL(bugUrl = "nocommit")
@Slow
@TimeoutSuite(millis = 45 * TimeUnits.SECOND)
public class BasicHttp2SolrClientTest extends SolrJettyTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static class RedirectServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      resp.sendRedirect("/solr/collection1/select?" + req.getQueryString());
    }
  }

  public static class SlowServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException ignored) {}
    }
  }

  public static class DebugServlet extends HttpServlet {
    public static void clear() {
      lastMethod = null;
      headers = null;
      parameters = null;
      errorCode = null;
      queryString = null;
      cookies = null;
    }

    public static Integer errorCode = null;
    public static String lastMethod = null;
    public static HashMap<String,String> headers = null;
    public static Map<String,String[]> parameters = null;
    public static String queryString = null;
    public static javax.servlet.http.Cookie[] cookies = null;

    public static void setErrorCode(Integer code) {
      errorCode = code;
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "delete";
      recordRequest(req, resp);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "get";
      recordRequest(req, resp);
    }

    @Override
    protected void doHead(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "head";
      recordRequest(req, resp);
    }

    private void setHeaders(HttpServletRequest req) {
      Enumeration<String> headerNames = req.getHeaderNames();
      headers = new HashMap<>();
      while (headerNames.hasMoreElements()) {
        final String name = headerNames.nextElement();
        headers.put(name, req.getHeader(name));
      }
    }

    @SuppressForbidden(reason = "fake servlet only")
    private void setParameters(HttpServletRequest req) {
      parameters = req.getParameterMap();
    }

    private void setQueryString(HttpServletRequest req) {
      queryString = req.getQueryString();
    }

    private void setCookies(HttpServletRequest req) {
      javax.servlet.http.Cookie[] ck = req.getCookies();
      cookies = req.getCookies();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "post";
      recordRequest(req, resp);
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "put";
      recordRequest(req, resp);
    }

    private void recordRequest(HttpServletRequest req, HttpServletResponse resp) {
      setHeaders(req);
      setParameters(req);
      setQueryString(req);
      setCookies(req);
      if (null != errorCode) {
        try {
          resp.sendError(errorCode);
        } catch (IOException e) {
          throw new RuntimeException("sendError IO fail in DebugServlet", e);
        }
      }
    }
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    JettyConfig jettyConfig = JettyConfig.builder()
        .withServlet(new ServletHolder(RedirectServlet.class), "/redirect/*")
        .withServlet(new ServletHolder(SlowServlet.class), "/slow/*")
        .withServlet(new ServletHolder(DebugServlet.class), "/debug/*")
        .withSSLConfig(sslConfig)
        .withHttpClient(getHttpClient())
        .withJettyQtp(getQtp())
        .build();
    createJetty(legacyExampleCollection1SolrHome(), jettyConfig);
  }

  @Test
  public void testCollectionParameters() throws Exception {
    int numDocs = 100;
    try (Http2SolrClient client = new Http2SolrClient.Builder(jetty.getBaseUrl().toString()).build()) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "collection");
      doc.addField("data_txt",
          "collectio sdf n sdjfisdf  sdkfjsdifjsd lflsk dfls dflksdj flksdjf lsdfj sdklf jsdlfj sldkfj sdlkfj sldkfjls difjlskdfj lsdkjf sldkfj sdlkfj sldkfj sldkfjsalidfjweipfjsdi jdfoiv dfoilvdf oivh sdif ousdnvio dfubvusdjf ilseajf lisdvlidfvoidfmv piodf jgiopwerjoigf3j4goiergjoi er8hdfoigejr89gijdfliv jdfljvdfliv jlsidv ldfkbv lidfjv lksd jvglidfjbv lidfhjbv dlfisvj ;osd fjk;osdk fldjsvlkdsfnivdfn vlidsfn gvlisd jflisdjfo ;jsdligjds likg dsfligjv ldsif gjlksdj fios;djglds;k jfilsdj gl;isdfj gvlisdk;j flisdjgv lsd");
      client.add("collection1", doc);
      client.commit("collection1");

      assertEquals(1, client.query("collection1", new SolrQuery("id:collection")).getResults().getNumFound());

      for (int i = 0; i < numDocs; i++) {
        SolrInputDocument doc2 = new SolrInputDocument();
        doc2.addField("id", "collection" + i);
        client.add("collection1", doc2, -1, new Http2SolrClient.OnComplete<UpdateResponse>() {

          @Override
          public void onSuccess(UpdateResponse result) {
            System.out.println("async result:" + result + " " + result.getStatus());
          }

          @Override
          public void onFailure(Throwable e) {
            e.printStackTrace();
          }
        });
      }

    }

    final String collection1Url = jetty.getBaseUrl().toString();
    try (Http2SolrClient client = new Http2SolrClient.Builder(collection1Url).build()) {
      client.commit("collection1", false, true, true);
      long numFound = client.query("collection1", new SolrQuery("*:*")).getResults().getNumFound();
      System.out.println("num found:" + numFound);
      assertEquals(numDocs + 1, numFound);
      final AtomicLong asyncNumFound = new AtomicLong();
      client.query("collection1", new SolrQuery("*:*"), new Http2SolrClient.OnComplete<QueryResponse>() {

        @Override
        public void onSuccess(QueryResponse result) {
          long numFound = result.getResults().getNumFound();
          System.out.println("Async num found:" + numFound);
          asyncNumFound.set(numFound);
        }

        @Override
        public void onFailure(Throwable e) {}
      });

    }

  }

  @Test
  public void testRedirect() throws Exception {
    final String clientUrl = jetty.getBaseUrl().toString() + "/redirect/foo";
    try (Http2SolrClient client = new Http2SolrClient.Builder(clientUrl).build()) {
      SolrQuery q = new SolrQuery("*:*");
      // default = false
      try {
        client.query(q);
        fail("Should have thrown an exception.");
      } catch (SolrServerException e) {
        assertTrue(e.getMessage().contains("redirect"));
      }

      client.setFollowRedirects(true);
      client.query(q);

      // And back again:
      client.setFollowRedirects(false);
      try {
        client.query(q);
        fail("Should have thrown an exception.");
      } catch (SolrServerException e) {
        assertTrue(e.getMessage().contains("redirect"));
      }
    }

  }

  @Test
  public void testDelete() throws Exception {
    DebugServlet.clear();
    try (Http2SolrClient client = new Http2SolrClient.Builder(jetty.getBaseUrl().toString() + "/debug/foo").build()) {
      try {
        client.deleteById("id");
      } catch (RemoteSolrException ignored) {}

      // default method
      assertEquals("post", DebugServlet.lastMethod);
      // agent
      System.out.println("headers:" + DebugServlet.headers);
      assertEquals("Solr[" + Http2SolrClient.class.getName() + "] 2.0", DebugServlet.headers.get("user-agent"));
      // default wt
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      // default version
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      // agent
      assertEquals("Solr[" + Http2SolrClient.class.getName() + "] 2.0", DebugServlet.headers.get("user-agent"));
      // keepalive
      // assertEquals("keep-alive", DebugServlet.headers.get("Connection"));

      try {
        client.deleteByQuery("*:*");
      } catch (RemoteSolrException ignored) {}

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals("Solr[" + Http2SolrClient.class.getName() + "] 2.0", DebugServlet.headers.get("user-agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals("Solr[" + Http2SolrClient.class.getName() + "] 2.0", DebugServlet.headers.get("user-agent"));
      // assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
    }

  }
}
