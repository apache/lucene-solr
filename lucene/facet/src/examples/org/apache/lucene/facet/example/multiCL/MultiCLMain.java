package org.apache.lucene.facet.example.multiCL;

import java.util.List;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import org.apache.lucene.facet.example.ExampleResult;
import org.apache.lucene.facet.example.ExampleUtils;
import org.apache.lucene.facet.search.results.FacetResult;

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

/**
 * @lucene.experimental
 */
public class MultiCLMain {

  /**
   * Driver for the multi sample.
   * 
   * @throws Exception
   *             on error (no detailed exception handling here for sample
   *             simplicity
   */
  public static void main(String[] args) throws Exception {
    new MultiCLMain().runSample();
    ExampleUtils.log("DONE");
  }

  public ExampleResult runSample() throws Exception {

    // create Directories for the search index and for the taxonomy index
    Directory indexDir = new RAMDirectory();
    Directory taxoDir = new RAMDirectory();

    // index the sample documents
    ExampleUtils.log("index the sample documents...");
    MultiCLIndexer.index(indexDir, taxoDir);

    ExampleUtils.log("search the sample documents...");
    List<FacetResult> facetRes = MultiCLSearcher.searchWithFacets(indexDir,
        taxoDir, MultiCLIndexer.MULTI_IPARAMS);

    ExampleResult res = new ExampleResult();
    res.setFacetResults(facetRes);
    return res;
  }

}
