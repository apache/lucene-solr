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
package org.apache.solr.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.handler.SnapPuller;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.TestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

/**
 *
 */
public class TestArbitraryIndexDir extends AbstractSolrTestCase{

  // TODO: fix this test to not require FSDirectory
  static String savedFactory;
  @BeforeClass
  public static void beforeClass() {
    // this test wants to start solr, and then open a separate indexwriter of its own on the same dir.
    System.setProperty("solr.tests.nrtMode", "false");
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");
  }
  @AfterClass
  public static void afterClass() {
    System.clearProperty("solr.tests.nrtMode");
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    File tmpDataDir = createTempDir();

    solrConfig = TestHarness.createConfig(getSolrHome(), "solrconfig.xml");
    h = new TestHarness( tmpDataDir.getAbsolutePath(),
        solrConfig,
        "schema12.xml");
    lrf = h.getRequestFactory
    ("standard",0,20,CommonParams.VERSION,"2.2");
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();

  }

  @Test
  public void testLoadNewIndexDir() throws IOException, ParserConfigurationException, SAXException {
    //add a doc in original index dir
    assertU(adoc("id", String.valueOf(1),
        "name", "name"+String.valueOf(1)));
    //create a new index dir and index.properties file
    File idxprops = new File(h.getCore().getDataDir() + SnapPuller.INDEX_PROPERTIES);
    Properties p = new Properties();
    File newDir = new File(h.getCore().getDataDir() + "index_temp");
    newDir.mkdirs();
    p.put("index", newDir.getName());
    Writer os = null;
    try {
      os = new OutputStreamWriter(new FileOutputStream(idxprops), StandardCharsets.UTF_8);
      p.store(os, "index properties");
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unable to write " + SnapPuller.INDEX_PROPERTIES, e);
    } finally {
      IOUtils.closeWhileHandlingException(os);
    }

    //add a doc in the new index dir
    Directory dir = newFSDirectory(newDir);
    IndexWriter iw = new IndexWriter(
        dir,
        new IndexWriterConfig(TEST_VERSION_CURRENT, new StandardAnalyzer(TEST_VERSION_CURRENT))
    );
    Document doc = new Document();
    doc.add(new TextField("id", "2", Field.Store.YES));
    doc.add(new TextField("name", "name2", Field.Store.YES));
    iw.addDocument(doc);
    iw.commit();
    iw.close();

    //commit will cause searcher to open with the new index dir
    assertU(commit());
    //new index dir contains just 1 doc.
    assertQ("return doc with id 2",
        req("id:2"),
        "*[count(//doc)=1]"
    );
    dir.close();
    newDir.delete();
  }
}
