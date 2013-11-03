package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.FacetTestUtils;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

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

public class TestSumValueSourceFacetRequest extends FacetTestCase {

  @Test
  public void testNoScore() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    FacetFields facetFields = new FacetFields(taxonomyWriter);
    for (int i = 0; i < 4; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("price", (i+1)));
      facetFields.addFields(doc, Collections.singletonList(new CategoryPath("a", Integer.toString(i % 2))));
      iw.addDocument(doc);
    }
    
    taxonomyWriter.close();
    iw.close();
    
    DirectoryReader r = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);

    ValueSource valueSource = new LongFieldSource("price");
    FacetSearchParams fsp = new FacetSearchParams(new SumValueSourceFacetRequest(new CategoryPath("a"), 10, valueSource, false));
    FacetsCollector fc = FacetsCollector.create(fsp, r, taxo);
    newSearcher(r).search(new MatchAllDocsQuery(), fc);
    
    List<FacetResult> res = fc.getFacetResults();
    assertEquals("a (0)\n  1 (6)\n  0 (4)\n", FacetTestUtils.toSimpleString(res.get(0)));
    
    IOUtils.close(taxo, taxoDir, r, indexDir);
  }

  @Test
  public void testWithScore() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    FacetFields facetFields = new FacetFields(taxonomyWriter);
    for (int i = 0; i < 4; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("price", (i+1)));
      facetFields.addFields(doc, Collections.singletonList(new CategoryPath("a", Integer.toString(i % 2))));
      iw.addDocument(doc);
    }
    
    taxonomyWriter.close();
    iw.close();
    
    DirectoryReader r = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);

    ValueSource valueSource = new ValueSource() {
      @Override
      public FunctionValues getValues(@SuppressWarnings("rawtypes") Map context, AtomicReaderContext readerContext) throws IOException {
        final Scorer scorer = (Scorer) context.get("scorer");
        assert scorer != null;
        return new DoubleDocValues(this) {
          @Override
          public double doubleVal(int document) {
            try {
              return scorer.score();
            } catch (IOException exception) {
              throw new RuntimeException(exception);
            }
          }
        };
      }

      @Override public boolean equals(Object o) { return o == this; }
      @Override public int hashCode() { return System.identityHashCode(this); }
      @Override public String description() { return "score()"; }
    };
    
    FacetSearchParams fsp = new FacetSearchParams(new SumValueSourceFacetRequest(new CategoryPath("a"), 10, valueSource, true));
    FacetsCollector fc = FacetsCollector.create(fsp, r, taxo);
    TopScoreDocCollector tsdc = TopScoreDocCollector.create(10, true);
    // score documents by their 'price' field - makes asserting the correct counts for the categories easier
    Query q = new FunctionQuery(new LongFieldSource("price"));
    newSearcher(r).search(q, MultiCollector.wrap(tsdc, fc));
    
    List<FacetResult> res = fc.getFacetResults();
    assertEquals("a (0)\n  1 (6)\n  0 (4)\n", FacetTestUtils.toSimpleString(res.get(0)));
    
    IOUtils.close(taxo, taxoDir, r, indexDir);
  }

  @Test
  public void testRollupValues() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    FacetIndexingParams fip = new FacetIndexingParams(new CategoryListParams() {
      @Override
      public OrdinalPolicy getOrdinalPolicy(String dimension) {
        return OrdinalPolicy.NO_PARENTS;
      }
    });
    FacetFields facetFields = new FacetFields(taxonomyWriter, fip);
    for (int i = 0; i < 4; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("price", (i+1)));
      facetFields.addFields(doc, Collections.singletonList(new CategoryPath("a", Integer.toString(i % 2), "1")));
      iw.addDocument(doc);
    }
    
    taxonomyWriter.close();
    iw.close();
    
    DirectoryReader r = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);

    ValueSource valueSource = new LongFieldSource("price");
    FacetSearchParams fsp = new FacetSearchParams(fip, new SumValueSourceFacetRequest(new CategoryPath("a"), 10, valueSource, false));
    FacetsCollector fc = FacetsCollector.create(fsp, r, taxo);
    newSearcher(r).search(new MatchAllDocsQuery(), fc);
    
    List<FacetResult> res = fc.getFacetResults();
    assertEquals("a (10)\n  1 (6)\n  0 (4)\n", FacetTestUtils.toSimpleString(res.get(0)));
    
    IOUtils.close(taxo, taxoDir, r, indexDir);
  }

}
