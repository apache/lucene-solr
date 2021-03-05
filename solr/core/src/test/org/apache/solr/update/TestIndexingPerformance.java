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

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.util.RTimer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;

/** Bypass the normal Solr pipeline and just text indexing performance
 * starting at the update handler.  The same document is indexed repeatedly.
 * 
 * $ ant test -Dtestcase=TestIndexingPerformance -Dargs="-server -Diter=100000"; grep throughput build/test-results/*TestIndexingPerformance.xml
 */
public class TestIndexingPerformance extends SolrTestCaseJ4 {
  
  // TODO: fix this test to not require FSDirectory
  static String savedFactory;
  @BeforeClass
  public static void beforeClass() throws Exception {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");

    initCore("solrconfig_perf.xml", "schema12.xml");
  }
  @AfterClass
  public static void afterClass() {
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  

  public void testIndexingPerf() throws IOException {
    int iter=1000;
    String iterS = System.getProperty("iter");
    if (iterS != null) iter=Integer.parseInt(iterS);
    boolean overwrite = Boolean.parseBoolean(System.getProperty("overwrite","false"));
    String doc = System.getProperty("doc");
    if (doc != null) {
      StrUtils.splitSmart(doc,",",true);
    }


    SolrQueryRequest req = lrf.makeRequest();
    UpdateHandler updateHandler = req.getCore().getUpdateHandler();
    String field = "textgap";

    String[] fields = {field,"simple"
            ,field,"test"
            ,field,"how now brown cow"
            ,field,"what's that?"
            ,field,"radical!"
            ,field,"what's all this about, anyway?"
            ,field,"just how fast is this text indexing?"
    };


  /***
    String[] fields = {
            "a_i","1"
            ,"b_i","2"
            ,"c_i","3"
            ,"d_i","4"
            ,"e_i","5"
            ,"f_i","6"
            ,"g_i","7"
            ,"h_i","8"
            ,"i_i","9"
            ,"j_i","0"
            ,"k_i","0"
    };
   ***/

    final RTimer timer = new RTimer();

    AddUpdateCommand add = new AddUpdateCommand(req);
    add.overwrite = overwrite;

    for (int i=0; i<iter; i++) {
      add.clear();
      add.solrDoc = new SolrInputDocument();
      add.solrDoc.addField("id", Integer.toString(i));
      for (int j=0; j<fields.length; j+=2) {
        String f = fields[j];
        String val = fields[j+1];
        add.solrDoc.addField(f, val);
      }
      updateHandler.addDoc(add);
    }
    if (log.isInfoEnabled()) {
      log.info("doc={}", Arrays.toString(fields));
    }
    double elapsed = timer.getTime();
    if (log.isInfoEnabled()) {
      log.info("iter={} time={} throughput={}", iter, elapsed, ((long) iter * 1000) / elapsed);
    }

    //discard all the changes
    updateHandler.rollback(new RollbackUpdateCommand(req));

    req.close();
  }

}
