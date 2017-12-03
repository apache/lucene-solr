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

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCoreBackup extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testBackupWithDocsNotSearchable() throws Exception {
    //See SOLR-11616 to see when this issue can be triggered

    assertU(adoc("id", "1"));
    assertU(commit());

    assertU(adoc("id", "2"));

    assertU(commit("openSearcher", "false"));
    assertQ(req("q", "*:*"), "//result[@numFound='1']");

    //call backup
    String location = createTempDir().toFile().getAbsolutePath();
    String snapshotName = TestUtil.randomSimpleString(random(), 1, 5);

    final CoreContainer cores = h.getCoreContainer();
    final CoreAdminHandler admin = new CoreAdminHandler(cores);
    SolrQueryResponse resp = new SolrQueryResponse();
    admin.handleRequestBody
        (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.BACKUPCORE.toString(),
            "core", DEFAULT_TEST_COLLECTION_NAME, "name", snapshotName, "location", location)
            , resp);
    assertNull("Backup should have succeeded", resp.getException());

  }
}
