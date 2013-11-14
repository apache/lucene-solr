package org.apache.lucene.facet.simple;

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.util.PrintTaxonomyStats;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util._TestUtil;

public class TestTaxonomyFacetsSumValueSource extends FacetTestCase {

  public void testBasic() throws Exception {

    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    IndexWriter writer = new FacetIndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())), taxoWriter, new FacetsConfig());

    // Reused across documents, to add the necessary facet
    // fields:
    Document doc = new Document();
    doc.add(new IntField("num", 10, Field.Store.NO));
    doc.add(new FacetField("Author", "Bob"));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new IntField("num", 20, Field.Store.NO));
    doc.add(new FacetField("Author", "Lisa"));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new IntField("num", 30, Field.Store.NO));
    doc.add(new FacetField("Author", "Lisa"));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new IntField("num", 40, Field.Store.NO));
    doc.add(new FacetField("Author", "Susan"));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new IntField("num", 45, Field.Store.NO));
    doc.add(new FacetField("Author", "Frank"));
    writer.addDocument(doc);

    // NRT open
    IndexSearcher searcher = newSearcher(DirectoryReader.open(writer, true));
    writer.close();

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    taxoWriter.close();

    // Aggregate the facet counts:
    SimpleFacetsCollector c = new SimpleFacetsCollector();

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), c);

    TaxonomyFacetSumValueSource facets = new TaxonomyFacetSumValueSource(taxoReader, new FacetsConfig(), c, new IntFieldSource("num"));

    // Retrieve & verify results:
    assertEquals("Author (145.0)\n  Lisa (50.0)\n  Frank (45.0)\n  Susan (40.0)\n  Bob (10.0)\n", facets.getTopChildren(10, "Author").toString());

    taxoReader.close();
    searcher.getIndexReader().close();
    dir.close();
    taxoDir.close();
  }

  // LUCENE-5333
  public void testSparseFacets() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    IndexWriter writer = new FacetIndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())), taxoWriter, new FacetsConfig());

    Document doc = new Document();
    doc.add(new IntField("num", 10, Field.Store.NO));
    doc.add(new FacetField("a", "foo1"));
    writer.addDocument(doc);

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new IntField("num", 20, Field.Store.NO));
    doc.add(new FacetField("a", "foo2"));
    doc.add(new FacetField("b", "bar1"));
    writer.addDocument(doc);

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new IntField("num", 30, Field.Store.NO));
    doc.add(new FacetField("a", "foo3"));
    doc.add(new FacetField("b", "bar2"));
    doc.add(new FacetField("c", "baz1"));
    writer.addDocument(doc);

    // NRT open
    IndexSearcher searcher = newSearcher(DirectoryReader.open(writer, true));
    writer.close();

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    taxoWriter.close();

    SimpleFacetsCollector c = new SimpleFacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);    

    TaxonomyFacetSumValueSource facets = new TaxonomyFacetSumValueSource(taxoReader, new FacetsConfig(), c, new IntFieldSource("num"));

    // Ask for top 10 labels for any dims that have counts:
    List<SimpleFacetResult> results = facets.getAllDims(10);

    assertEquals(3, results.size());
    assertEquals("a (60.0)\n  foo3 (30.0)\n  foo2 (20.0)\n  foo1 (10.0)\n", results.get(0).toString());
    assertEquals("b (50.0)\n  bar2 (30.0)\n  bar1 (20.0)\n", results.get(1).toString());
    assertEquals("c (30.0)\n  baz1 (30.0)\n", results.get(2).toString());

    searcher.getIndexReader().close();
    taxoReader.close();
    taxoDir.close();
    dir.close();
  }

  // nocommit in the sparse case test that we are really
  // sorting by the correct dim count
}
