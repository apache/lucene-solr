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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.util.ExternalPaths;


public class UtilsForTests {
  protected static final String RESOURCES_DIR = ExternalPaths.SOURCE_HOME + "/contrib/map-reduce/src/test-files";  
  
  public static void validateSolrServerDocumentCount(File solrHomeDir, FileSystem fs, Path outDir, int expectedDocs, int expectedShards)
      throws IOException, SolrServerException {
    
    long actualDocs = 0;
    int actualShards = 0;
    for (FileStatus dir : fs.listStatus(outDir)) { // for each shard
      if (dir.getPath().getName().startsWith("part") && dir.isDirectory()) {
        actualShards++;
        EmbeddedSolrServer solr = SolrRecordWriter.createEmbeddedSolrServer(
            new Path(solrHomeDir.getAbsolutePath()), fs, dir.getPath());
        
        try {
          SolrQuery query = new SolrQuery();
          query.setQuery("*:*");
          QueryResponse resp = solr.query(query);
          long numDocs = resp.getResults().getNumFound();
          actualDocs += numDocs;
        } finally {
          solr.shutdown();
        }
      }
    }
    assertEquals(expectedShards, actualShards);
    assertEquals(expectedDocs, actualDocs);
  }

}
