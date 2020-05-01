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
package org.apache.solr.client.solrj;

import java.io.BufferedOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.invoke.MethodHandles;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class TestSolrJErrorHandling extends SolrJettyTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  List<Throwable> unexpected = new CopyOnWriteArrayList<>();


  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    unexpected.clear();
  }

  public String getChain(Throwable th) {
    StringBuilder sb = new StringBuilder(40);
    Throwable lastCause = null;
    do {
      if (lastCause != null) sb.append("->");
      sb.append(th.getClass().getSimpleName());
      lastCause = th;
      th = th.getCause();
    } while(th != null);
    sb.append("(" + lastCause.getMessage() + ")");
    return sb.toString();
  }

  public void showExceptions() throws Exception {
    if (unexpected.isEmpty()) return;

    Map<String,Integer> counts = new HashMap<>();

    // dedup in case there are many clients or many exceptions
    for (Throwable e : unexpected) {
      String chain = getChain(e);
      Integer prev = counts.put(chain, 1);
      if (prev != null) {
        counts.put(chain, prev+1);
      }
    }

    StringBuilder sb = new StringBuilder("EXCEPTION LIST:");
    for (Map.Entry<String,Integer> entry : counts.entrySet()) {
      sb.append("\n\t").append(entry.getValue()).append(") ").append(entry.getKey());
    }

    log.error("{}", sb);
  }

  @Test
  public void testWithXml() throws Exception {
    HttpSolrClient client = (HttpSolrClient) getSolrClient();
    client.setRequestWriter(new RequestWriter());
    client.deleteByQuery("*:*"); // delete everything!
    doIt(client);
  }

  @Test
  public void testWithBinary() throws Exception {
    HttpSolrClient client = (HttpSolrClient) getSolrClient();
    client.setRequestWriter(new BinaryRequestWriter());
    client.deleteByQuery("*:*"); // delete everything!
    doIt(client);
  }

  Iterator<SolrInputDocument> manyDocs(final int base, final int numDocs) {
    return new Iterator<SolrInputDocument>() {
      int count = 0;
      @Override
      public boolean hasNext() {
        return count < numDocs;
      }

      @Override
      public SolrInputDocument next() {
        int id = base + count++;
        if (count == 1) {  // first doc is legit, and will increment a counter
          return sdoc("id","test", "count_i", map("inc",1));
        }
        // include "ignore_exception" so the log doesn't fill up with known exceptions, and change the values for each doc
        // so binary format won't compress too much
        return sdoc("id",Integer.toString(id),"ignore_exception_field_does_not_exist_"+id,"fieldval"+id);
      }

      @Override
      public void remove() {
      }
    };
  };

  void doThreads(final HttpSolrClient client, final int numThreads, final int numRequests) throws Exception {
    final AtomicInteger tries = new AtomicInteger(0);

    List<Thread> threads = new ArrayList<>();

    for (int i=0; i<numThreads; i++) {
      final int threadNum = i;
      threads.add( new Thread() {
                     int reqLeft = numRequests;

                     @Override
                     public void run() {
                       try {
                         while (--reqLeft >= 0) {
                           tries.incrementAndGet();
                           doSingle(client, threadNum);
                         }
                       } catch (Throwable e) {
                         // Allow thread to exit, we should have already recorded the exception.
                       }
                     }
                   }
      );
    }

    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }

    showExceptions();

    int count = getCount(client);
    if (count > tries.get()) {
      fail("Number of requests was " + tries.get() + " but final count was " + count);
    }

    assertEquals(tries.get(), getCount(client));

    assertTrue("got unexpected exceptions. ", unexpected.isEmpty() );
  }

  int getCount(HttpSolrClient client) throws IOException, SolrServerException {
    client.commit();
    QueryResponse rsp = client.query(params("q", "id:test", "fl", "count_i", "wt", "json"));
    int count = ((Number)rsp.getResults().get(0).get("count_i")).intValue();
    return count;
  }

  // this always failed with the Jetty 9.3 snapshot
  void doIt(HttpSolrClient client) throws Exception {
    client.deleteByQuery("*:*");
    doThreads(client,10,100);
    // doSingle(client, 1);
  }

  void doSingle(HttpSolrClient client, int threadNum) {
    try {
      client.add(manyDocs(threadNum*1000000, 1000));
    }
    catch (HttpSolrClient.RemoteSolrException e) {
      String msg = e.getMessage();
      assertTrue(msg, msg.contains("field_does_not_exist"));
    }
    catch (Throwable e) {
      unexpected.add(e);
      log.error("unexpected exception:", e);
      fail("FAILING unexpected exception: " + e);
    }
  }

  /***
  @Test
  public void testLive() throws Exception {
    HttpSolrClient client = new HttpSolrClient("http://localhost:8983/techproducts/solr/");
    client.add( sdoc() );
    doiIt(client);
  }
  ***/

  String getJsonDocs(int numDocs) {
    StringBuilder sb = new StringBuilder(numDocs * 20);
    sb.append("[");
    for (int i = 0; i < numDocs; i++) {
      sb.append("{ id : '" + i + "' , unknown_field_" + i + " : 'unknown field value' }");
    }
    sb.append("]");
    return sb.toString();
  }

  byte[] whitespace(int n) {
    byte[] arr = new byte[n];
    Arrays.fill(arr, (byte) ' ');
    return arr;
  }

  String getResponse(InputStream is) throws Exception {
    StringBuilder sb = new StringBuilder();
    byte[] buf = new byte[100000];
    for (;;) {
      int n = 0;
      try {
         n = is.read(buf);
      } catch (IOException e) {
        // a real HTTP client probably wouldn't try to read past the end and would thus
        // not get an exception until the *next* http request.
        log.error("CAUGHT IOException, but already read {} : {}", sb.length(), getChain(e));
      }
      if (n <= 0) break;
      sb.append(new String(buf, 0, n, StandardCharsets.UTF_8));
      log.info("BUFFER={}", sb);
      break;  // for now, assume we got whole response in one read... otherwise we could block when trying to read again
    }
    return sb.toString();
  }

  @Test
  public void testHttpURLConnection() throws Exception {

   String bodyString = getJsonDocs(200000);  // sometimes succeeds with this size, but larger can cause OOM from command line

    HttpSolrClient client = (HttpSolrClient) getSolrClient();

    String urlString = client.getBaseURL() + "/update";

    HttpURLConnection conn = null;
    URL url = new URL(urlString);

    conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

    OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream(), StandardCharsets.UTF_8);
    writer.write(bodyString);
    writer.flush();

    int code = 1;
    try {
      code = conn.getResponseCode();
    } catch (Throwable th) {
      log.error("ERROR DURING conn.getResponseCode():", th);
    }

/***
 java.io.IOException: Error writing to server
 at __randomizedtesting.SeedInfo.seed([2928C6EE314CD076:947A81A74F582526]:0)
 at sun.net.www.protocol.http.HttpURLConnection.writeRequests(HttpURLConnection.java:665)
 at sun.net.www.protocol.http.HttpURLConnection.writeRequests(HttpURLConnection.java:677)
 at sun.net.www.protocol.http.HttpURLConnection.getInputStream0(HttpURLConnection.java:1533)
 at sun.net.www.protocol.http.HttpURLConnection.getInputStream(HttpURLConnection.java:1440)
 at java.net.HttpURLConnection.getResponseCode(HttpURLConnection.java:480)
 */

    log.info("CODE= {}", code);
    InputStream is;
    if (code == 200) {
      is = conn.getInputStream();
    } else {
      log.info("Attempting to get error stream.");
      is = conn.getErrorStream();
      if (is == null) {
        log.info("Can't get error stream... try input stream?");
        is = conn.getInputStream();
      }
    }

    String rbody = IOUtils.toString(is, StandardCharsets.UTF_8);
    log.info("RESPONSE BODY:{}", rbody);
  }

  @Test
  public void testRawSocket() throws Exception {

    String hostName = "127.0.0.1";
    int port = jetty.getLocalPort();

    try (Socket socket = new Socket(hostName, port);
        OutputStream out = new BufferedOutputStream(socket.getOutputStream());
        InputStream in = socket.getInputStream();
    ) {
      byte[] body = getJsonDocs(100000).getBytes(StandardCharsets.UTF_8);
      int bodyLen = body.length;

      // bodyLen *= 10;  // make server wait for more

      byte[] whitespace = whitespace(1000000);
      bodyLen += whitespace.length;

      String headers = "POST /solr/collection1/update HTTP/1.1\n" +
          "Host: localhost:" + port + "\n" +
//        "User-Agent: curl/7.43.0\n" +
          "Accept: */*\n" +
          "Content-type:application/json\n" +
          "Content-Length: " + bodyLen + "\n" +
          "Connection: Keep-Alive\n";

      // Headers of HTTP connection are defined to be ASCII only:
      out.write(headers.getBytes(StandardCharsets.US_ASCII));
      out.write('\n');  // extra newline separates headers from body
      out.write(body);
      out.flush();

      // Now what if I try to write more?  This doesn't seem to throw an exception!
      Thread.sleep(1000);
      out.write(whitespace);  // whitespace
      out.flush();

      String rbody = getResponse(in);  // This will throw a connection reset exception if you try to read past the end of the HTTP response
      log.info("RESPONSE BODY: {}", rbody);
      assertTrue(rbody.contains("unknown_field"));

      /***
      // can I reuse now?
      // writing another request doesn't actually throw an exception, but the following read does
      out.write(headers);
      out.write("\n");  // extra newline separates headers from body
      out.write(body);
      out.flush();

      rbody = getResponse(in);
      log.info("RESPONSE BODY: {}", rbody);
      assertTrue(rbody.contains("unknown_field"));
      ***/
    }
  }


}
