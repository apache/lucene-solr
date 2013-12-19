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
package org.apache.solr.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.hadoop.morphline.MorphlineMapRunner;
import org.apache.solr.morphlines.solr.AbstractSolrMorphlineTestBase;
import org.apache.solr.util.ExternalPaths;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class MRUnitBase extends SolrTestCaseJ4 {
  
  protected static final String RESOURCES_DIR = ExternalPaths.SOURCE_HOME + "/contrib/map-reduce/src/test-files";  
  protected static final String DOCUMENTS_DIR = RESOURCES_DIR + "/test-documents";
  protected static File solrHomeZip;

  @BeforeClass
  public static void setupClass() throws Exception {
    solrHomeZip = SolrOutputFormat.createSolrHomeZip(new File(RESOURCES_DIR + "/solr/mrunit"));
    assertNotNull(solrHomeZip);
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    solrHomeZip.delete();
  }
  
  protected void setupHadoopConfig(Configuration config) throws IOException {
    
    String tempDir = TEMP_DIR + "/test-morphlines-" + System.currentTimeMillis();
    new File(tempDir).mkdirs();
    FileUtils.copyFile(new File(RESOURCES_DIR + "/custom-mimetypes.xml"), new File(tempDir + "/custom-mimetypes.xml"));
    
    AbstractSolrMorphlineTestBase.setupMorphline(tempDir, "test-morphlines/solrCellDocumentTypes", true);
    
    config.set(MorphlineMapRunner.MORPHLINE_FILE_PARAM, tempDir + "/test-morphlines/solrCellDocumentTypes.conf");
    config.set(SolrOutputFormat.ZIP_NAME, solrHomeZip.getName());
  }
  
}
