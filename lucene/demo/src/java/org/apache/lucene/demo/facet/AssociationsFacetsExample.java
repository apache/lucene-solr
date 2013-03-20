package org.apache.lucene.demo.facet;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.associations.AssociationFloatSumFacetRequest;
import org.apache.lucene.facet.associations.AssociationIntSumFacetRequest;
import org.apache.lucene.facet.associations.AssociationsFacetFields;
import org.apache.lucene.facet.associations.CategoryAssociation;
import org.apache.lucene.facet.associations.CategoryAssociationsContainer;
import org.apache.lucene.facet.associations.CategoryFloatAssociation;
import org.apache.lucene.facet.associations.CategoryIntAssociation;
import org.apache.lucene.facet.associations.MultiAssociationsFacetsAggregator;
import org.apache.lucene.facet.associations.SumFloatAssociationFacetsAggregator;
import org.apache.lucene.facet.associations.SumIntAssociationFacetsAggregator;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.search.FacetsAggregator;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

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

/** Shows example usage of category associations. */
public class AssociationsFacetsExample {

  /**
   * Categories per document, {@link #ASSOCIATIONS} hold the association value
   * for each category.
   */
  public static CategoryPath[][] CATEGORIES = {
    // Doc #1
    { new CategoryPath("tags", "lucene") , 
      new CategoryPath("genre", "computing")
    },
        
    // Doc #2
    { new CategoryPath("tags", "lucene"),  
      new CategoryPath("tags", "solr"),
      new CategoryPath("genre", "computing"),
      new CategoryPath("genre", "software")
    }
  };

  /** Association values for each category. */
  public static CategoryAssociation[][] ASSOCIATIONS = {
    // Doc #1 associations
    {
      /* 3 occurrences for tag 'lucene' */
      new CategoryIntAssociation(3), 
      /* 87% confidence level of genre 'computing' */
      new CategoryFloatAssociation(0.87f)
    },
    
    // Doc #2 associations
    {
      /* 1 occurrence for tag 'lucene' */
      new CategoryIntAssociation(1),
      /* 2 occurrences for tag 'solr' */
      new CategoryIntAssociation(2),
      /* 75% confidence level of genre 'computing' */
      new CategoryFloatAssociation(0.75f),
      /* 34% confidence level of genre 'software' */
      new CategoryFloatAssociation(0.34f),
    }
  };

  private final Directory indexDir = new RAMDirectory();
  private final Directory taxoDir = new RAMDirectory();

  /** Empty constructor */
  public AssociationsFacetsExample() {}
  
  /** Build the example index. */
  private void index() throws IOException {
    IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(FacetExamples.EXAMPLES_VER, 
        new WhitespaceAnalyzer(FacetExamples.EXAMPLES_VER)));

    // Writes facet ords to a separate directory from the main index
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);

    // Reused across documents, to add the necessary facet fields
    FacetFields facetFields = new AssociationsFacetFields(taxoWriter);
    
    for (int i = 0; i < CATEGORIES.length; i++) {
      Document doc = new Document();
      CategoryAssociationsContainer associations = new CategoryAssociationsContainer();
      for (int j = 0; j < CATEGORIES[i].length; j++) {
        associations.setAssociation(CATEGORIES[i][j], ASSOCIATIONS[i][j]);
      }
      facetFields.addFields(doc, associations);
      indexWriter.addDocument(doc);
    }
    
    indexWriter.close();
    taxoWriter.close();
  }

  /** User runs a query and aggregates facets by summing their association values. */
  private List<FacetResult> sumAssociations() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    
    CategoryPath tags = new CategoryPath("tags");
    CategoryPath genre = new CategoryPath("genre");
    FacetSearchParams fsp = new FacetSearchParams(
        new AssociationIntSumFacetRequest(tags, 10), 
        new AssociationFloatSumFacetRequest(genre, 10));
  
    // every category has a different type of association, so use chain their
    // respective aggregators.
    final Map<CategoryPath,FacetsAggregator> aggregators = new HashMap<CategoryPath,FacetsAggregator>();
    aggregators.put(tags, new SumIntAssociationFacetsAggregator());
    aggregators.put(genre, new SumFloatAssociationFacetsAggregator());
    FacetsAccumulator fa = new FacetsAccumulator(fsp, indexReader, taxoReader) {
      @Override
      public FacetsAggregator getAggregator() {
        return new MultiAssociationsFacetsAggregator(aggregators);
      }
    };
    FacetsCollector fc = FacetsCollector.create(fa);
    
    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), fc);
    
    // Retrieve results
    List<FacetResult> facetResults = fc.getFacetResults();
    
    indexReader.close();
    taxoReader.close();
    
    return facetResults;
  }
  
  /** Runs summing association example. */
  public List<FacetResult> runSumAssociations() throws IOException {
    index();
    return sumAssociations();
  }
  
  /** Runs the sum int/float associations examples and prints the results. */
  public static void main(String[] args) throws Exception {
    System.out.println("Sum associations example:");
    System.out.println("-------------------------");
    List<FacetResult> results = new AssociationsFacetsExample().runSumAssociations();
    for (FacetResult res : results) {
      System.out.println(res);
    }
  }
  
}
