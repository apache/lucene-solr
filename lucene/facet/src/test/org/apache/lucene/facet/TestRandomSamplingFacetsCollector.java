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
package org.apache.lucene.facet;

import java.util.List;
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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

public class TestRandomSamplingFacetsCollector extends FacetTestCase {
  
  // The first 50 chi-square value for p-value=0.05, taken from:
  // http://en.wikibooks.org/wiki/Engineering_Tables/Chi-Squared_Distibution
  private static final float[] CHI_SQUARE_VALUES = new float[] {0.0f, 3.841f,
      5.991f, 7.815f, 9.488f, 11.07f, 12.592f, 14.067f, 15.507f, 16.919f,
      18.307f, 19.675f, 21.026f, 22.362f, 23.685f, 24.996f, 26.296f, 27.587f,
      28.869f, 30.144f, 31.41f, 32.671f, 33.924f, 35.172f, 36.415f, 37.652f,
      38.885f, 40.113f, 41.337f, 42.557f, 43.773f, 44.985f, 46.194f, 47.4f,
      48.602f, 49.802f, 50.998f, 52.192f, 53.384f, 54.572f, 55.758f, 56.942f,
      58.124f, 59.304f, 60.481f, 61.656f, 62.83f, 64.001f, 65.171f, 66.339f,
      67.505f};
  
  public void testRandomSampling() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    
    Random random = random();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    RandomIndexWriter writer = new RandomIndexWriter(random, dir);
    
    FacetsConfig config = new FacetsConfig();
    
    final int numCategories = 10;
    int numDocs = atLeast(10000);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField("EvenOdd", (i % 2 == 0) ? "even" : "odd", Store.NO));
      doc.add(new FacetField("iMod10", Integer.toString(i % numCategories)));
      writer.addDocument(config.build(taxoWriter, doc));
    }
    writer.forceMerge(CHI_SQUARE_VALUES.length - 1);
    
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
    
    RandomSamplingFacetsCollector random10Percent = new RandomSamplingFacetsCollector(numDocs / 10, random.nextLong()); // 10% of total docs, 20% of the hits

    FacetsCollector fc = new FacetsCollector();
    
    searcher.search(query, MultiCollector.wrap(fc, random10Percent));
    
    final List<MatchingDocs> matchingDocs = random10Percent.getMatchingDocs();

    // count the total hits and sampled docs, also store the number of sampled
    // docs per segment
    int totalSampledDocs = 0, totalHits = 0;
    int[] numSampledDocs = new int[matchingDocs.size()];
//    System.out.println("numSegments=" + numSampledDocs.length);
    for (int i = 0; i < numSampledDocs.length; i++) {
      MatchingDocs md = matchingDocs.get(i);
      final DocIdSetIterator iter = md.bits.iterator();
      while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) ++numSampledDocs[i];
      totalSampledDocs += numSampledDocs[i];
      totalHits += md.totalHits;
    }
    
    // compute the chi-square value for the sampled documents' distribution
    float chi_square = 0;
    for (int i = 0; i < numSampledDocs.length; i++) {
      MatchingDocs md = matchingDocs.get(i);
      float ei = (float) md.totalHits / totalHits;
      if (ei > 0.0f) {
        float oi = (float) numSampledDocs[i] / totalSampledDocs;
        chi_square += (Math.pow(ei - oi, 2) / ei);
      }
    }
    
    // Verify that the chi-square value isn't too big. According to
    // http://en.wikipedia.org/wiki/Chi-squared_distribution#Table_of_.CF.872_value_vs_p-value,
    // we basically verify that there is a really small chance of hitting a very
    // bad sample (p-value < 0.05), for n-degrees of freedom. The number 'n' depends
    // on the number of segments.
    assertTrue("chisquare not statistically significant enough: " + chi_square, chi_square < CHI_SQUARE_VALUES[numSampledDocs.length]);
    
    // Test amortized counts - should be 5X the sampled count, but maximum numDocs/10
    final FastTaxonomyFacetCounts random10FacetCounts = new FastTaxonomyFacetCounts(taxoReader, config, random10Percent);
    final FacetResult random10Result = random10FacetCounts.getTopChildren(10, "iMod10");
    final FacetResult amortized10Result = random10Percent.amortizeFacetCounts(random10Result, config, searcher);
    for (int i = 0; i < amortized10Result.labelValues.length; i++) {
      LabelAndValue amortized = amortized10Result.labelValues[i];
      LabelAndValue sampled = random10Result.labelValues[i];
      // since numDocs may not divide by 10 exactly, allow for some slack in the amortized count 
      assertEquals(amortized.value.floatValue(), Math.min(5 * sampled.value.floatValue(), numDocs / 10.f), 1.0);
    }
    
    IOUtils.close(searcher.getIndexReader(), taxoReader, dir, taxoDir);
  }
  
}
