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
import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.designer.DefaultSampleDocumentsLoader;
import org.apache.solr.handler.designer.SampleDocuments;
import org.apache.solr.handler.designer.SampleDocumentsLoader;
import org.apache.solr.util.ExternalPaths;
import org.junit.Before;
import org.junit.Test;

import static org.apache.solr.handler.designer.DefaultSampleDocumentsLoader.CSV_MULTI_VALUE_DELIM_PARAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSampleDocumentsLoader {

  SampleDocumentsLoader loader;
  File exampleDir;

  @Before
  public void setup() throws IOException {
    loader = new DefaultSampleDocumentsLoader();
    loader.init(new NamedList<>());
    exampleDir = new File(ExternalPaths.SOURCE_HOME, "example");
    assertTrue("Required test data directory " + exampleDir.getCanonicalPath() + " not found!", exampleDir.isDirectory());
  }

  @Test
  public void testJson() throws Exception {
    loadTestDocs(null, new File(exampleDir, "films/films.json"), 500, 500);
  }

  @Test
  public void testCsv() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CSV_MULTI_VALUE_DELIM_PARAM, "\\|");
    List<SolrInputDocument> docs = loadTestDocs(params, new File(exampleDir, "films/films.csv"), -1, 1100);
    boolean foundIt = false;
    for (SolrInputDocument next : docs) {
      if (".45".equals(next.getFieldValue("name"))) {
        SolrInputField genreField = next.getField("genre");
        assertNotNull(genreField);
        assertEquals(8, genreField.getValueCount());
        foundIt = true;
        break;
      }
    }
    if (!foundIt) {
      fail("Didn't find the expected film with name '.45' in the parsed docs!");
    }
  }

  @Test
  public void testSolrXml() throws Exception {
    loadTestDocs(null, new File(exampleDir, "films/films.xml"), 1000, 1000);
  }

  protected List<SolrInputDocument> loadTestDocs(SolrParams params, File inputDocs, int maxDocsToLoad, int expectedDocs) throws IOException {
    assertTrue(inputDocs.getCanonicalPath() + " not found", inputDocs.isFile());
    ContentStream stream = getContentStream(inputDocs);
    SampleDocuments sampleDocs = loader.parseDocsFromStream(params, stream, maxDocsToLoad);
    assertNotNull(sampleDocs);
    assertEquals(sampleDocs.parsed.size(), expectedDocs);
    return sampleDocs.parsed;
  }

  public static String guessContentTypeFromFilename(String name) {
    int dotAt = name.lastIndexOf('.');
    if (dotAt != -1) {
      final String ext = name.substring(dotAt + 1);
      switch (ext) {
        case "csv":
          return "text/csv";
        case "txt":
          return "text/plain";
        case "json":
          return "application/json";
        case "xml":
          return "text/xml";
      }
    }
    return "application/octet-stream";
  }

  protected ContentStream getContentStream(File file) {
    return getContentStream(file, guessContentTypeFromFilename(file.getName()));
  }

  protected ContentStream getContentStream(File file, String contentType) {
    ContentStreamBase.FileStream stream = new ContentStreamBase.FileStream(file);
    stream.setContentType(contentType);
    return stream;
  }
}
