package org.apache.lucene.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

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

public class FacetTestUtils {

  public static class IndexTaxonomyReaderPair {
    public DirectoryReader indexReader;
    public DirectoryTaxonomyReader taxReader;
    public IndexSearcher indexSearcher;

    public void close() throws IOException {
      indexReader.close();
      taxReader.close();
    }

  }

  public static class IndexTaxonomyWriterPair {
    public IndexWriter indexWriter;
    public TaxonomyWriter taxWriter;

    public void close() throws IOException {
      indexWriter.close();
      taxWriter.close();
    }

    public void commit() throws IOException {
      indexWriter.commit();
      taxWriter.commit();
    }
  }

  public static Directory[][] createIndexTaxonomyDirs(int number) {
    Directory[][] dirs = new Directory[number][2];
    for (int i = 0; i < number; i++) {
      dirs[i][0] = LuceneTestCase.newDirectory();
      dirs[i][1] = LuceneTestCase.newDirectory();
    }
    return dirs;
  }

  public static IndexTaxonomyReaderPair[] createIndexTaxonomyReaderPair(
      Directory[][] dirs) throws IOException {
    IndexTaxonomyReaderPair[] pairs = new IndexTaxonomyReaderPair[dirs.length];
    for (int i = 0; i < dirs.length; i++) {
      IndexTaxonomyReaderPair pair = new IndexTaxonomyReaderPair();
      pair.indexReader = DirectoryReader.open(dirs[i][0]);
      pair.indexSearcher = new IndexSearcher(pair.indexReader);
      pair.taxReader = new DirectoryTaxonomyReader(dirs[i][1]);
      pairs[i] = pair;
    }
    return pairs;
  }
  
  public static IndexTaxonomyWriterPair[] createIndexTaxonomyWriterPair(
      Directory[][] dirs) throws IOException {
    IndexTaxonomyWriterPair[] pairs = new IndexTaxonomyWriterPair[dirs.length];
    for (int i = 0; i < dirs.length; i++) {
      IndexTaxonomyWriterPair pair = new IndexTaxonomyWriterPair();
      pair.indexWriter = new IndexWriter(dirs[i][0], new IndexWriterConfig(
          LuceneTestCase.TEST_VERSION_CURRENT, new StandardAnalyzer(
              LuceneTestCase.TEST_VERSION_CURRENT)));
      pair.taxWriter = new DirectoryTaxonomyWriter(dirs[i][1]);
      pair.indexWriter.commit();
      pair.taxWriter.commit();
      pairs[i] = pair;
    }
    return pairs;
  }

  public static Collector[] search(IndexSearcher searcher,
      TaxonomyReader taxonomyReader, FacetIndexingParams iParams, int k,
      String... facetNames) throws IOException {
    
    Collector[] collectors = new Collector[2];
    
    List<FacetRequest> fRequests = new ArrayList<FacetRequest>();
    for (String facetName : facetNames) {
      CategoryPath cp = new CategoryPath(facetName);
      FacetRequest fq = new CountFacetRequest(cp, k);
      fRequests.add(fq);
    }
    FacetSearchParams facetSearchParams = new FacetSearchParams(fRequests, iParams);

    TopScoreDocCollector topDocsCollector = TopScoreDocCollector.create(
        searcher.getIndexReader().maxDoc(), true);
    FacetsCollector facetsCollector = new FacetsCollector(
        facetSearchParams, searcher.getIndexReader(), taxonomyReader);
    Collector mColl = MultiCollector.wrap(topDocsCollector, facetsCollector);
    
    collectors[0] = topDocsCollector;
    collectors[1] = facetsCollector;

    searcher.search(new MatchAllDocsQuery(), mColl);
    return collectors;
  }

  public static String toSimpleString(FacetResult fr) {
    StringBuilder sb = new StringBuilder();
    toSimpleString(0, sb, fr.getFacetResultNode(), "");
    return sb.toString();
  }
  
  private static void toSimpleString(int depth, StringBuilder sb, FacetResultNode node, String indent) {
    sb.append(indent + node.label.components[depth] + " (" + (int) node.value + ")\n");
    for (FacetResultNode childNode : node.subResults) {
      toSimpleString(depth + 1, sb, childNode, indent + "  ");
    }
  }

}
