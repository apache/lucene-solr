package org.apache.lucene.facet;

import java.util.Random;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

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

public class TestRandomSamplingFacetsCollector extends FacetTestCase {
  
  public void testRandomSampling() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    
    FacetsConfig config = new FacetsConfig();
    
    int numDocs = atLeast(10000);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField("EvenOdd", (i % 2 == 0) ? "even" : "odd", Store.NO));
      doc.add(new FacetField("iMod10", String.valueOf(i % 10)));
      writer.addDocument(config.build(taxoWriter, doc));
    }
    Random random = random();
    
    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    IOUtils.close(writer, taxoWriter);
    
    // Test empty results
    RandomSamplingFacetsCollector collectRandomZeroResults = new RandomSamplingFacetsCollector(numDocs / 10, random.nextLong());
    
    // There should be no divisions by zero
    searcher.search(new TermQuery(new Term("EvenOdd", "NeverMatches")), collectRandomZeroResults);
    
    // There should be no divisions by zero and no null result
    assertNotNull(collectRandomZeroResults.getMatchingDocs());
    
    // There should be no results at all
    for (MatchingDocs doc : collectRandomZeroResults.getMatchingDocs()) {
      assertEquals(0, doc.totalHits);
    }
    
    // Now start searching and retrieve results.
    
    // Use a query to select half of the documents.
    TermQuery query = new TermQuery(new Term("EvenOdd", "even"));
    
    // there will be 5 facet values (0, 2, 4, 6 and 8), as only the even (i %
    // 10) are hits.
    // there is a REAL small chance that one of the 5 values will be missed when
    // sampling.
    // but is that 0.8 (chance not to take a value) ^ 2000 * 5 (any can be
    // missing) ~ 10^-193
    // so that is probably not going to happen.
    int maxNumChildren = 5;
    
    RandomSamplingFacetsCollector random100Percent = new RandomSamplingFacetsCollector(numDocs, random.nextLong()); // no sampling
    RandomSamplingFacetsCollector random10Percent = new RandomSamplingFacetsCollector(numDocs / 10, random.nextLong()); // 10 % of total docs, 20% of the hits

    FacetsCollector fc = new FacetsCollector();
    
    searcher.search(query, MultiCollector.wrap(fc, random100Percent, random10Percent));
    
    FastTaxonomyFacetCounts random10FacetCounts = new FastTaxonomyFacetCounts(taxoReader, config, random10Percent);
    FastTaxonomyFacetCounts random100FacetCounts = new FastTaxonomyFacetCounts(taxoReader, config, random100Percent);
    FastTaxonomyFacetCounts exactFacetCounts = new FastTaxonomyFacetCounts(taxoReader, config, fc);
    
    FacetResult random10Result = random10Percent.amortizeFacetCounts(random10FacetCounts.getTopChildren(10, "iMod10"), config, searcher);
    FacetResult random100Result = random100FacetCounts.getTopChildren(10, "iMod10");
    FacetResult exactResult = exactFacetCounts.getTopChildren(10, "iMod10");
    
    assertEquals(random100Result, exactResult);
    
    // we should have five children, but there is a small chance we have less.
    // (see above).
    assertTrue(random10Result.childCount <= maxNumChildren);
    // there should be one child at least.
    assertTrue(random10Result.childCount >= 1);
    
    // now calculate some statistics to determine if the sampled result is 'ok'.
    // because random sampling is used, the results will vary each time.
    int sum = 0;
    for (LabelAndValue lav : random10Result.labelValues) {
      sum += lav.value.intValue();
    }
    float mu = (float) sum / (float) maxNumChildren;
    
    float variance = 0;
    for (LabelAndValue lav : random10Result.labelValues) {
      variance += Math.pow((mu - lav.value.intValue()), 2);
    }
    variance = variance / maxNumChildren;
    float sigma = (float) Math.sqrt(variance);
    
    // we query only half the documents and have 5 categories. The average
    // number of docs in a category will thus be the total divided by 5*2
    float targetMu = numDocs / (5.0f * 2.0f);
    
    // the average should be in the range and the standard deviation should not
    // be too great
    assertTrue(sigma < 200);
    assertTrue(targetMu - 3 * sigma < mu && mu < targetMu + 3 * sigma);
    
    IOUtils.close(searcher.getIndexReader(), taxoReader, dir, taxoDir);
  }
  
}
