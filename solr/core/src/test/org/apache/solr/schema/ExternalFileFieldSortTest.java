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
package org.apache.solr.schema;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ExternalFileFieldSortTest extends SolrTestCaseJ4 {

  static void updateExternalFile() throws IOException {
    final String testHome = SolrTestCaseJ4.getFile("solr/collection1").getParent();
    String filename = "external_eff";
    FileUtils.copyFile(new File(testHome + "/" + filename),
        new File(h.getCore().getDataDir() + "/" + filename));
  }

  private void addDocuments() {
    for (int i = 1; i <= 10; i++) {
      String id = Integer.toString(i);
      assertU("add a test doc", adoc("id", id));
    }
    assertU("commit", commit());
  }

  @Test
  public void testSort() throws Exception {
    initCore("solrconfig-basic.xml", "schema-eff.xml");
    updateExternalFile();

    addDocuments();
    assertQ("query",
        req("q", "*:*", "sort", "eff asc"),
        "//result/doc[position()=1]/str[.='3']",
        "//result/doc[position()=2]/str[.='1']",
        "//result/doc[position()=10]/str[.='8']");

    assertQ("test exists", req("q", "*:*", "sort", "exists(eff) desc"));
  }
  
  @Test
  public void testPointKeyFieldType() throws Exception {
    // This one should fail though, no "node" parameter specified
    SolrException e = expectThrows(SolrException.class, 
        () -> initCore("solrconfig-basic.xml", "bad-schema-eff.xml"));
    assertTrue(e.getMessage().contains("has a Point field type, which is not supported."));
  }
}
