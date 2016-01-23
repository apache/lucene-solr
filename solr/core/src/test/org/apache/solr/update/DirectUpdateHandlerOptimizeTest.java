package org.apache.solr.update;
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

import java.io.File;
import java.io.FileFilter;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.BeforeClass;


/**
 *
 *
 **/
public class DirectUpdateHandlerOptimizeTest extends AbstractSolrTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
  }


  public void testOptimize() throws Exception {
    SolrCore core = h.getCore();

    UpdateHandler updater = core.getUpdateHandler();
    SolrQueryRequest req = req();
    AddUpdateCommand cmd = new AddUpdateCommand(req);

    //add just under the merge factor, so no segments are merged
    //the merge factor is 100 and the maxBufferedDocs is 2, so there should be 50 segments
    for (int i = 0; i < 99; i++) {
      // Add a valid document
      cmd.solrDoc = new SolrInputDocument();
      cmd.solrDoc.addField("id", "id_" + i);
      cmd.solrDoc.addField("subject", "subject_" + i);
      updater.addDoc(cmd);
    }

    CommitUpdateCommand cmtCmd = new CommitUpdateCommand(req, false);
    updater.commit(cmtCmd);
    updater.commit(cmtCmd);  // commit twice to give systems such as windows a chance to delete the old files

    String indexDir = core.getIndexDir();
    assertNumSegments(indexDir, 50);

    //now do an optimize
    cmtCmd = new CommitUpdateCommand(req, true);
    cmtCmd.maxOptimizeSegments = 25;
    updater.commit(cmtCmd);
    updater.commit(cmtCmd);
    assertNumSegments(indexDir, 25);

    cmtCmd.maxOptimizeSegments = -1;
    try {
      updater.commit(cmtCmd);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
    }
    cmtCmd.maxOptimizeSegments = 1;
    updater.commit(cmtCmd);
    updater.commit(cmtCmd);
    assertNumSegments(indexDir, 1);

    req.close();
  }

  private void assertNumSegments(String indexDir, int numSegs) {
    File file = new File(indexDir);
    File[] segs = file.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.getName().endsWith("cfs");
      }
    });

    //
    // TODO: we need a method that does not rely on physical inspection of the directory.
    //
    // assertTrue("Wrong number of segments: " + segs.length + " does not equal: " + numSegs, segs.length == numSegs);
  }

}
