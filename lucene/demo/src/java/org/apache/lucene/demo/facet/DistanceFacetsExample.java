package org.apache.lucene.demo.facet;

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

import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.facet.DoubleRange;
import org.apache.lucene.facet.DoubleRangeFacetCounts;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LongRange;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;



/** Shows simple usage of dynamic range faceting, using the
 *  expressions module to calculate distance. */
public class DistanceFacetsExample implements Closeable {

  private final Directory indexDir = new RAMDirectory();
  private IndexSearcher searcher;

  /** Empty constructor */
  public DistanceFacetsExample() {}
  
  /** Build the example index. */
  public void index() throws IOException {
    IndexWriter writer = new IndexWriter(indexDir, new IndexWriterConfig(FacetExamples.EXAMPLES_VER, 
        new WhitespaceAnalyzer(FacetExamples.EXAMPLES_VER)));

    // Add documents with latitude/longitude location:
    Document doc = new Document();
    doc.add(new DoubleField("latitude", 40.759011, Field.Store.NO));
    doc.add(new DoubleField("longitude", -73.9844722, Field.Store.NO));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new DoubleField("latitude", 40.718266, Field.Store.NO));
    doc.add(new DoubleField("longitude", -74.007819, Field.Store.NO));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new DoubleField("latitude", 40.7051157, Field.Store.NO));
    doc.add(new DoubleField("longitude", -74.0088305, Field.Store.NO));
    writer.addDocument(doc);

    // Open near-real-time searcher
    searcher = new IndexSearcher(DirectoryReader.open(writer, true));
    writer.close();
  }

  /** User runs a query and counts facets. */
  public FacetResult search() throws IOException {

    Expression distance;
    try {
      distance = JavascriptCompiler.compile("haversin(40.7143528,-74.0059731,latitude,longitude)");
    } catch (ParseException pe) {
      // Should not happen
      throw new RuntimeException(pe);
    }
    SimpleBindings bindings = new SimpleBindings();
    bindings.add(new SortField("latitude", SortField.Type.DOUBLE));
    bindings.add(new SortField("longitude", SortField.Type.DOUBLE));

    FacetsCollector fc = new FacetsCollector();

    searcher.search(new MatchAllDocsQuery(), fc);

    Facets facets = new DoubleRangeFacetCounts("field", distance.getValueSource(bindings), fc,
        new DoubleRange("< 1 km", 0.0, true, 1.0, false),
        new DoubleRange("< 2 km", 0.0, true, 2.0, false),
        new DoubleRange("< 5 km", 0.0, true, 5.0, false),
        new DoubleRange("< 10 km", 0.0, true, 10.0, false),
        new DoubleRange("< 20 km", 0.0, true, 20.0, false),
        new DoubleRange("< 50 km", 0.0, true, 50.0, false));

    return facets.getTopChildren(10, "field");
  }

  // nocommit how to show drillDown?

  @Override
  public void close() throws IOException {
    searcher.getIndexReader().close();
    indexDir.close();
  }

  /** Runs the search and drill-down examples and prints the results. */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    RangeFacetsExample example = new RangeFacetsExample();
    example.index();

    System.out.println("Dirance facet counting example:");
    System.out.println("-----------------------");
    System.out.println(example.search());

    example.close();
  }
}
