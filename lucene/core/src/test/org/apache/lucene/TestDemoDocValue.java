package org.apache.lucene;

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

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.SortedSetDocValues.OrdIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * A very simple demo used in the API documentation (src/java/overview.html).
 *
 * Please try to keep src/java/overview.html up-to-date when making changes
 * to this class.
 */
public class TestDemoDocValue extends LuceneTestCase {
  
  // nocommit: only Lucene42/Asserting implemented right now
  private Codec saved;
  @Override
  public void setUp() throws Exception {
    super.setUp();
    saved = Codec.getDefault();
    Codec.setDefault(_TestUtil.alwaysDocValuesFormat(DocValuesFormat.forName("Asserting")));
  }

  @Override
  public void tearDown() throws Exception {
    Codec.setDefault(saved);
    super.tearDown();
  }

  public void testOneValue() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    // Store the index in memory:
    Directory directory = newDirectory();
    // To store an index on disk, use this instead:
    // Directory directory = FSDirectory.open(new File("/tmp/testindex"));
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, analyzer);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("hello")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    DirectoryReader ireader = DirectoryReader.open(directory); // read-only=true
    SortedSetDocValues dv = getOnlySegmentReader(ireader).getSortedSetDocValues("field");
    OrdIterator oi = dv.getOrds(0, null);
    assertEquals(0, oi.nextOrd());
    assertEquals(OrdIterator.NO_MORE_ORDS, oi.nextOrd());
    
    BytesRef bytes = new BytesRef();
    dv.lookupOrd(0, bytes);
    assertEquals(new BytesRef("hello"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testTwoDocumentsMerged() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    // Store the index in memory:
    Directory directory = newDirectory();
    // To store an index on disk, use this instead:
    // Directory directory = FSDirectory.open(new File("/tmp/testindex"));
    IndexWriterConfig iwconfig = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("hello")));
    iwriter.addDocument(doc);
    iwriter.commit();
    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("world")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    DirectoryReader ireader = DirectoryReader.open(directory); // read-only=true
    SortedSetDocValues dv = getOnlySegmentReader(ireader).getSortedSetDocValues("field");
    OrdIterator oi = dv.getOrds(0, null);
    assertEquals(0, oi.nextOrd());
    assertEquals(OrdIterator.NO_MORE_ORDS, oi.nextOrd());
    
    BytesRef bytes = new BytesRef();
    dv.lookupOrd(0, bytes);
    assertEquals(new BytesRef("hello"), bytes);
    
    oi = dv.getOrds(1, oi);
    assertEquals(1, oi.nextOrd());
    assertEquals(OrdIterator.NO_MORE_ORDS, oi.nextOrd());
    
    dv.lookupOrd(1, bytes);
    assertEquals(new BytesRef("world"), bytes);
    
    assertEquals(2, dv.getValueCount());

    ireader.close();
    directory.close();
  }
  
  public void testTwoValues() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    // Store the index in memory:
    Directory directory = newDirectory();
    // To store an index on disk, use this instead:
    // Directory directory = FSDirectory.open(new File("/tmp/testindex"));
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, analyzer);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("hello")));
    doc.add(new SortedSetDocValuesField("field", new BytesRef("world")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    DirectoryReader ireader = DirectoryReader.open(directory); // read-only=true
    SortedSetDocValues dv = getOnlySegmentReader(ireader).getSortedSetDocValues("field");
    OrdIterator oi = dv.getOrds(0, null);
    assertEquals(0, oi.nextOrd());
    assertEquals(1, oi.nextOrd());
    assertEquals(OrdIterator.NO_MORE_ORDS, oi.nextOrd());
    
    BytesRef bytes = new BytesRef();
    dv.lookupOrd(0, bytes);
    assertEquals(new BytesRef("hello"), bytes);
    
    dv.lookupOrd(1, bytes);
    assertEquals(new BytesRef("world"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testTwoValuesUnordered() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    // Store the index in memory:
    Directory directory = newDirectory();
    // To store an index on disk, use this instead:
    // Directory directory = FSDirectory.open(new File("/tmp/testindex"));
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, analyzer);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("world")));
    doc.add(new SortedSetDocValuesField("field", new BytesRef("hello")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    DirectoryReader ireader = DirectoryReader.open(directory); // read-only=true
    SortedSetDocValues dv = getOnlySegmentReader(ireader).getSortedSetDocValues("field");
    OrdIterator oi = dv.getOrds(0, null);
    assertEquals(0, oi.nextOrd());
    assertEquals(1, oi.nextOrd());
    assertEquals(OrdIterator.NO_MORE_ORDS, oi.nextOrd());
    
    BytesRef bytes = new BytesRef();
    dv.lookupOrd(0, bytes);
    assertEquals(new BytesRef("hello"), bytes);
    
    dv.lookupOrd(1, bytes);
    assertEquals(new BytesRef("world"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testThreeValuesTwoDocs() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    // Store the index in memory:
    Directory directory = newDirectory();
    // To store an index on disk, use this instead:
    // Directory directory = FSDirectory.open(new File("/tmp/testindex"));
    IndexWriterConfig iwconfig = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("hello")));
    doc.add(new SortedSetDocValuesField("field", new BytesRef("world")));
    iwriter.addDocument(doc);
    iwriter.commit();
    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("hello")));
    doc.add(new SortedSetDocValuesField("field", new BytesRef("beer")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    DirectoryReader ireader = DirectoryReader.open(directory); // read-only=true
    SortedSetDocValues dv = getOnlySegmentReader(ireader).getSortedSetDocValues("field");
    assertEquals(3, dv.getValueCount());
    
    OrdIterator oi = dv.getOrds(0, null);
    assertEquals(1, oi.nextOrd());
    assertEquals(2, oi.nextOrd());
    assertEquals(OrdIterator.NO_MORE_ORDS, oi.nextOrd());
    
    oi = dv.getOrds(1, null);
    assertEquals(0, oi.nextOrd());
    assertEquals(1, oi.nextOrd());
    assertEquals(OrdIterator.NO_MORE_ORDS, oi.nextOrd());
    
    BytesRef bytes = new BytesRef();
    dv.lookupOrd(0, bytes);
    assertEquals(new BytesRef("beer"), bytes);
    
    dv.lookupOrd(1, bytes);
    assertEquals(new BytesRef("hello"), bytes);
    
    dv.lookupOrd(2, bytes);
    assertEquals(new BytesRef("world"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testTwoDocumentsLastMissing() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    // Store the index in memory:
    Directory directory = newDirectory();
    // To store an index on disk, use this instead:
    // Directory directory = FSDirectory.open(new File("/tmp/testindex"));
    IndexWriterConfig iwconfig = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("hello")));
    iwriter.addDocument(doc);
    doc = new Document();
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    DirectoryReader ireader = DirectoryReader.open(directory); // read-only=true
    SortedSetDocValues dv = getOnlySegmentReader(ireader).getSortedSetDocValues("field");
    OrdIterator oi = dv.getOrds(0, null);
    assertEquals(0, oi.nextOrd());
    assertEquals(OrdIterator.NO_MORE_ORDS, oi.nextOrd());
    
    BytesRef bytes = new BytesRef();
    dv.lookupOrd(0, bytes);
    assertEquals(new BytesRef("hello"), bytes);
    
    assertEquals(1, dv.getValueCount());

    ireader.close();
    directory.close();
  }
  
  public void testTwoDocumentsLastMissingMerge() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    // Store the index in memory:
    Directory directory = newDirectory();
    // To store an index on disk, use this instead:
    // Directory directory = FSDirectory.open(new File("/tmp/testindex"));
    IndexWriterConfig iwconfig = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("hello")));
    iwriter.addDocument(doc);
    iwriter.commit();
    doc = new Document();
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    DirectoryReader ireader = DirectoryReader.open(directory); // read-only=true
    SortedSetDocValues dv = getOnlySegmentReader(ireader).getSortedSetDocValues("field");
    OrdIterator oi = dv.getOrds(0, null);
    assertEquals(0, oi.nextOrd());
    assertEquals(OrdIterator.NO_MORE_ORDS, oi.nextOrd());
    
    BytesRef bytes = new BytesRef();
    dv.lookupOrd(0, bytes);
    assertEquals(new BytesRef("hello"), bytes);
    
    assertEquals(1, dv.getValueCount());

    ireader.close();
    directory.close();
  }
  
  public void testTwoDocumentsFirstMissing() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    // Store the index in memory:
    Directory directory = newDirectory();
    // To store an index on disk, use this instead:
    // Directory directory = FSDirectory.open(new File("/tmp/testindex"));
    IndexWriterConfig iwconfig = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    Document doc = new Document();
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("hello")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    DirectoryReader ireader = DirectoryReader.open(directory); // read-only=true
    SortedSetDocValues dv = getOnlySegmentReader(ireader).getSortedSetDocValues("field");
    OrdIterator oi = dv.getOrds(1, null);
    assertEquals(0, oi.nextOrd());
    assertEquals(OrdIterator.NO_MORE_ORDS, oi.nextOrd());
    
    BytesRef bytes = new BytesRef();
    dv.lookupOrd(0, bytes);
    assertEquals(new BytesRef("hello"), bytes);
    
    assertEquals(1, dv.getValueCount());

    ireader.close();
    directory.close();
  }
  
  public void testTwoDocumentsFirstMissingMerge() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    // Store the index in memory:
    Directory directory = newDirectory();
    // To store an index on disk, use this instead:
    // Directory directory = FSDirectory.open(new File("/tmp/testindex"));
    IndexWriterConfig iwconfig = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    Document doc = new Document();
    iwriter.addDocument(doc);
    iwriter.commit();
    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("hello")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    DirectoryReader ireader = DirectoryReader.open(directory); // read-only=true
    SortedSetDocValues dv = getOnlySegmentReader(ireader).getSortedSetDocValues("field");
    OrdIterator oi = dv.getOrds(1, null);
    assertEquals(0, oi.nextOrd());
    assertEquals(OrdIterator.NO_MORE_ORDS, oi.nextOrd());
    
    BytesRef bytes = new BytesRef();
    dv.lookupOrd(0, bytes);
    assertEquals(new BytesRef("hello"), bytes);
    
    assertEquals(1, dv.getValueCount());

    ireader.close();
    directory.close();
  }
}
