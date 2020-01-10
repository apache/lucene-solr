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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.IOUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSQLHandlerNonCloud extends SolrJettyTestBase {

  private static File createSolrHome() throws Exception {
    File workDir = createTempDir().toFile();
    setupJettyTestHome(workDir, DEFAULT_TEST_COLLECTION_NAME);
    return workDir;
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    File solrHome = createSolrHome();
    solrHome.deleteOnExit();
    createAndStartJetty(solrHome.getAbsolutePath());
  }

  @Test
  public void testSQLHandler() throws Exception {
    String sql = "select id, field_i, str_s from " + DEFAULT_TEST_COLLECTION_NAME + " limit 10";
    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "stmt", sql);
    String url = jetty.getBaseUrl() + "/" + DEFAULT_TEST_COLLECTION_NAME;

    SolrStream solrStream = new SolrStream(url, sParams);
    IOException ex = expectThrows(IOException.class,  () -> getTuples(solrStream));
    assertTrue(ex.getMessage().contains(SQLHandler.sqlNonCloudErrorMsg));
  }

  private List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    List<Tuple> tuples = new ArrayList<>();
    try {
      tupleStream.open();
      for (; ; ) {
        Tuple t = tupleStream.read();
        if (t.EOF) {
          break;
        } else {
          tuples.add(t);
        }
      }
    } finally {
      IOUtils.closeQuietly(tupleStream);
    }
    return tuples;
  }

  public static SolrParams mapParams(String... vals) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    assertEquals("Parameters passed in here must be in pairs!", 0, (vals.length % 2));
    for (int idx = 0; idx < vals.length; idx += 2) {
      params.add(vals[idx], vals[idx + 1]);
    }

    return params;
  }
}
