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

package org.apache.solr.response;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.PushWriter;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;

public class TestPushWriter extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public void testStandardResponse() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamWriter osw = new OutputStreamWriter(baos, StandardCharsets.UTF_8);
    PushWriter pw = new JSONWriter(osw,
        new LocalSolrQueryRequest(null, new ModifiableSolrParams()), new SolrQueryResponse());
    writeData(pw);
    osw.flush();
    log.info(new String(baos.toByteArray(), StandardCharsets.UTF_8));
    Map m = (Map) Utils.fromJSON(baos.toByteArray());
    checkValues(m);
    try (JavaBinCodec jbc = new JavaBinCodec(baos= new ByteArrayOutputStream(), null)) {
      writeData(jbc);
      try (JavaBinCodec jbcUn = new JavaBinCodec()) {
        m = (Map) jbcUn.unmarshal(new ByteArrayInputStream(baos.toByteArray()));
      }
    }
    checkValues(m);
  }

  protected void checkValues(Map m) {
    assertEquals(0, ((Number)Utils.getObjectByPath(m, true, "responseHeader/status")).intValue());
    assertEquals(10, ((Number)Utils.getObjectByPath(m, true, "response/numFound")).intValue());
    assertEquals(1, ((Number)Utils.getObjectByPath(m, true, "response/docs[0]/id")).intValue());
    assertEquals(2, ((Number)Utils.getObjectByPath(m, true, "response/docs[1]/id")).intValue());
    assertEquals(3, ((Number)Utils.getObjectByPath(m, true, "response/docs[2]/id")).intValue());
  }

  protected void writeData(PushWriter pw) throws IOException {
    pw.writeMap(m -> {
      m.put("responseHeader", singletonMap("status", 0))
          .put("response", (MapWriter) m1 -> {
            m1.put("numFound", 10)
                .put("docs", (IteratorWriter) w -> {
                  w.add((MapWriter) m3 -> m3.put("id", 1))
                      .add(singletonMap("id", 2))
                      .add(singletonMap("id", 3));
                }); }); });
    pw.close();
  }
}
