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
package org.apache.solr.request;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.response.BinaryQueryResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.RTimer;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestWriterPerf extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() throws Exception {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
    initCore("solrconfig-functionquery.xml", "schema11.xml");
  }
  
  public String getCoreName() { return "basic"; }

  @Override
  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
  }
  @Override
  public void tearDown() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.tearDown();
  }

  String id = "id";
  String t1 = "f_t";
  String i1 = "f_i";
  String tag = "f_ss";


  void index(Object... olst) {
    ArrayList<String> lst = new ArrayList<>();
    for (Object o : olst) lst.add(o.toString());
    assertU(adoc(lst.toArray(new String[lst.size()])));
  }

  void makeIndex() {
    index(id,1, i1, 100,t1,"now is the time for all good men", tag,"patriotic");
    index(id,2, i1, 50 ,t1,"to come to the aid of their country.", tag,"patriotic",tag,"country",tag,"nation",tag,"speeches");
    index(id,3, i1, 2 ,t1,"how now brown cow", tag,"cow",tag,"jersey");
    index(id,4, i1, -100 ,t1,"the quick fox jumped over the lazy dog",tag,"fox",tag,"dog",tag,"quick",tag,"slow",tag,"lazy");
    index(id,5, i1, 50 ,t1,"the quick fox jumped way over the lazy dog",tag,"fox",tag,"dog");
    index(id,6, i1, -60 ,t1,"humpty dumpy sat on a wall",tag,"humpty",tag,"dumpty");
    index(id,7, i1, 123 ,t1,"humpty dumpy had a great fall",tag,"accidents");
    index(id,8, i1, 876 ,t1,"all the kings horses and all the kings men",tag,"king",tag,"horses",tag,"trouble");
    index(id,9, i1, 7 ,t1,"couldn't put humpty together again",tag,"humpty",tag,"broken");
    index(id,10, i1, 4321 ,t1,"this too shall pass",tag,"1",tag,"2",tag,"infinity");
    index(id,11, i1, 33 ,t1,"An eye for eye only ends up making the whole world blind.",tag,"ouch",tag,"eye",tag,"peace",tag,"world");
    index(id,12, i1, 379 ,t1,"Great works are performed, not by strength, but by perseverance.",tag,"herculese",tag,"strong",tag,"stubborn");
    assertU(optimize());
  }


  /** make sure to close req after you are done using the response */
  public SolrQueryResponse getResponse(SolrQueryRequest req) throws Exception {
    SolrQueryResponse rsp = new SolrQueryResponse();
    h.getCore().execute(h.getCore().getRequestHandler(null),req,rsp);
    if (rsp.getException() != null) {
      throw rsp.getException();
    }
    return rsp;
  }


  void doPerf(String writerName, SolrQueryRequest req, int encIter, int decIter) throws Exception {
    SolrQueryResponse rsp = getResponse(req);
    QueryResponseWriter w = h.getCore().getQueryResponseWriter(writerName);


    ByteArrayOutputStream out=null;

    System.gc();
    RTimer timer = new RTimer();
    for (int i=0; i<encIter; i++) {
    if (w instanceof BinaryQueryResponseWriter) {
      BinaryQueryResponseWriter binWriter = (BinaryQueryResponseWriter) w;
      out = new ByteArrayOutputStream();
      binWriter.write(out, req, rsp);
      out.close();
    } else {
      out = new ByteArrayOutputStream();
      // to be fair, from my previous tests, much of the performance will be sucked up
      // by java's UTF-8 encoding/decoding, not the actual writing
      Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
      w.write(writer, req, rsp);
      writer.close();
    }
    }

    double encodeTime = timer.getTime();

    byte[] arr = out.toByteArray();

    timer = new RTimer();
    writerName = writerName.intern();
    for (int i=0; i<decIter; i++) {
      ResponseParser rp = null;
      if (writerName == "xml") {
        rp = new XMLResponseParser();
      } else if (writerName == "javabin") {
        rp = new BinaryResponseParser();
      } else {
        break;
      }
      ByteArrayInputStream in = new ByteArrayInputStream(arr);
      rp.processResponse(in, "UTF-8");      
    }

    double decodeTime = timer.getTime();

    if (log.isInfoEnabled()) {
      log.info("writer {}, size={}, encodeRate={} decodeRate={}"
          , writerName, out.size(), (encIter * 1000L / encodeTime), (decIter * 1000L / decodeTime));
    }

    req.close();
  }

  public void testPerf() throws Exception {
    makeIndex();

    SolrQueryRequest req = req("q", "id:[* TO *] all country"
                    ,"start","0"
                    ,"rows","100"
                    ,"echoParams","all"
                    ,"fl","*,score"
                    ,"indent","false"
                    ,"facet","true"
                    ,"facet.field", i1
                    ,"facet.field", tag
                    ,"facet.field", t1
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"hl","true"
                    ,"hl.fl","t1"
            );


    // just for testing
    doPerf("xml", req, 2,2);
    doPerf("json", req, 2,2);
    doPerf("javabin", req, 2,2);

    int encIter=20000;
    int decIter=50000;


    // warm up hotspot
    // doPerf("xml", req, 200,1000);
    // doPerf("json", req, 200,1000);
    // doPerf("javabin", req, 200,1000);

    // doPerf("xml", req, encIter, decIter);
    // doPerf("json", req, encIter, decIter);
    //doPerf("javabin", req, encIter, decIter);
    // doPerf("javabin", req, 1, decIter);
  }

}
