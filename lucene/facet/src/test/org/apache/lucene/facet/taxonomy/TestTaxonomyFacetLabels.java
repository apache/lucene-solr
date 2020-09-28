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
package org.apache.lucene.facet.taxonomy;

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class TestTaxonomyFacetLabels extends FacetTestCase {

  private List<Document> prepareDocuments() {
    List<Document> docs = new ArrayList<>();

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    docs.add(doc);

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    docs.add(doc);

    doc = new Document();
    doc.add(new FacetField("Author", "Tom"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    docs.add(doc);

    doc = new Document();
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    docs.add(doc);

    doc = new Document();
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    docs.add(doc);

    return docs;
  }

  private List<Integer> allDocIds(MatchingDocs m, boolean decreasingDocIds) throws IOException {
    DocIdSetIterator disi = m.bits.iterator();
    List<Integer> docIds = new ArrayList<>();
    while (disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      docIds.add(disi.docID());
    }

    if (decreasingDocIds == true) {
      Collections.reverse(docIds);
    }
    return docIds;
  }

  private List<FacetLabel> lookupFacetLabels(TaxonomyFacetLabels taxoLabels,
                                             List<MatchingDocs> matchingDocs) throws IOException {
    return lookupFacetLabels(taxoLabels, matchingDocs, null, false);
  }

  private List<FacetLabel> lookupFacetLabels(TaxonomyFacetLabels taxoLabels,
                                             List<MatchingDocs> matchingDocs,
                                             String dimension) throws IOException {
    return lookupFacetLabels(taxoLabels, matchingDocs, dimension, false);
  }

  private List<FacetLabel> lookupFacetLabels(TaxonomyFacetLabels taxoLabels, List<MatchingDocs> matchingDocs, String dimension,
                                             boolean decreasingDocIds) throws IOException {
    List<FacetLabel> facetLabels = new ArrayList<>();

    for (MatchingDocs m : matchingDocs) {
      TaxonomyFacetLabels.FacetLabelReader facetLabelReader = taxoLabels.getFacetLabelReader(m.context);
      List<Integer> docIds = allDocIds(m, decreasingDocIds);
      FacetLabel facetLabel;
      for (Integer docId : docIds) {
        while (true) {
          if (dimension != null) {
            facetLabel = facetLabelReader.nextFacetLabel(docId, dimension);
          } else {
            facetLabel = facetLabelReader.nextFacetLabel(docId);
          }

          if (facetLabel == null) {
            break;
          }
          facetLabels.add(facetLabel);
        }
      }
    }

    return facetLabels;
  }


  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Publish Date", true);

    for (Document doc : prepareDocuments()) {
      writer.addDocument(config.build(taxoWriter, doc));
    }

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());
    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    FacetsCollector fc = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), fc);

    TaxonomyFacetLabels taxoLabels = new TaxonomyFacetLabels(taxoReader, FacetsConfig.DEFAULT_INDEX_FIELD_NAME);

    // Check labels for all dimensions
    List<FacetLabel> facetLabels = lookupFacetLabels(taxoLabels, fc.getMatchingDocs());
    assertEquals("Incorrect number of facet labels received", 10, facetLabels.size());

    // Check labels for all dimensions
    assertTrue(facetLabels.stream()
        .filter(l -> "Author".equals(l.components[0]))
        .map(l -> l.components[1]).collect(Collectors.toSet())
               .equals(new HashSet<>(Arrays.asList("Bob", "Lisa", "Susan", "Frank", "Tom"))));

    assertTrue(facetLabels.stream()
        .filter(l -> "Publish Date".equals(l.components[0]))
        .map(l -> String.join("/", l.components[1], l.components[2], l.components[3]))
        .collect(Collectors.toSet())
               .equals(new HashSet<>(Arrays.asList("2010/10/15", "2010/10/20", "2012/1/1", "2012/1/7", "1999/5/5"))));

    // Check labels for a specific dimension
    facetLabels = lookupFacetLabels(taxoLabels, fc.getMatchingDocs(), "Publish Date");
    assertEquals("Incorrect number of facet labels received for 'Publish Date'", 5, facetLabels.size());

    assertTrue(facetLabels.stream()
        .map(l -> String.join("/", l.components[1], l.components[2], l.components[3]))
        .collect(Collectors.toSet())
               .equals(new HashSet<>(Arrays.asList("2010/10/15", "2010/10/20", "2012/1/1", "2012/1/7", "1999/5/5"))));

    try {
      facetLabels = lookupFacetLabels(taxoLabels, fc.getMatchingDocs(), null, true);
      fail("IllegalArgumentException was not thrown for using docIds supplied in decreasing order");
    } catch (IllegalArgumentException ae) {
      assertTrue(ae.getMessage().contains("docs out of order"));
    }

    try {
      facetLabels = lookupFacetLabels(taxoLabels, fc.getMatchingDocs(), "Publish Date", true);
      fail("Assertion error was not thrown for using docIds supplied in decreasing order");
    } catch (IllegalArgumentException ae) {
      assertTrue(ae.getMessage().contains("docs out of order"));
    }

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }
}
