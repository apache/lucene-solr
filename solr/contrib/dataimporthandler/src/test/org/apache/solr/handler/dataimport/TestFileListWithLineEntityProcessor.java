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
package org.apache.solr.handler.dataimport;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.junit.BeforeClass;

public class TestFileListWithLineEntityProcessor extends AbstractDataImportHandlerTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
  }
  
  public void test() throws Exception {
    File tmpdir = createTempDir(LuceneTestCase.getTestClass().getSimpleName()).toFile();
    createFile(tmpdir, "a.txt", "a line one\na line two\na line three".getBytes(StandardCharsets.UTF_8), false);
    createFile(tmpdir, "b.txt", "b line one\nb line two".getBytes(StandardCharsets.UTF_8), false);
    createFile(tmpdir, "c.txt", "c line one\nc line two\nc line three\nc line four".getBytes(StandardCharsets.UTF_8), false);
    
    String config = generateConfig(tmpdir);
    LocalSolrQueryRequest request = lrf.makeRequest(
        "command", "full-import", "dataConfig", config,
        "clean", "true", "commit", "true", "synchronous", "true", "indent", "true");
    h.query("/dataimport", request);
    
    assertQ(req("*:*"), "//*[@numFound='9']");
    assertQ(req("id:?\\ line\\ one"), "//*[@numFound='3']");
    assertQ(req("id:a\\ line*"), "//*[@numFound='3']");
    assertQ(req("id:b\\ line*"), "//*[@numFound='2']");
    assertQ(req("id:c\\ line*"), "//*[@numFound='4']");    
  }
  
  private String generateConfig(File dir) {
    return
    "<dataConfig> \n"+
    "<dataSource type=\"FileDataSource\" encoding=\"UTF-8\" name=\"fds\"/> \n"+
    "    <document> \n"+
    "       <entity name=\"f\" processor=\"FileListEntityProcessor\" fileName=\".*[.]txt\" baseDir=\"" + dir.getAbsolutePath() + "\" recursive=\"false\" rootEntity=\"false\"  transformer=\"TemplateTransformer\"> \n" +
    "             <entity name=\"jc\" processor=\"LineEntityProcessor\" url=\"${f.fileAbsolutePath}\" dataSource=\"fds\"  rootEntity=\"true\" transformer=\"TemplateTransformer\"> \n" +
    "              <field column=\"rawLine\" name=\"id\" /> \n" +
    "             </entity> \n"+              
    "        </entity> \n"+
    "    </document> \n"+
    "</dataConfig> \n";
  }  
}
