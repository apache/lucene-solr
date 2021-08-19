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
package org.apache.lucene.index;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CheckIndex.Status.DocValuesStatus;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Abstract class to do basic tests for a docvalues format.
 * NOTE: This test focuses on the docvalues impl, nothing else.
 * The [stretch] goal is for this test to be
 * so thorough in testing a new DocValuesFormat that if this
 * test passes, then all Lucene/Solr tests should also pass.  Ie,
 * if there is some bug in a given DocValuesFormat that this
 * test fails to catch then this test needs to be improved! */
public abstract class BaseDocValuesFormatTestCase extends BaseIndexFileFormatTestCase {

  @Override
  protected void addRandomFields(Document doc) {
    if (usually()) {
      doc.add(new NumericDocValuesField("ndv", random().nextInt(1 << 12)));
      doc.add(new BinaryDocValuesField("bdv", newBytesRef(TestUtil.randomSimpleString(random()))));
      doc.add(
          new SortedDocValuesField("sdv", newBytesRef(TestUtil.randomSimpleString(random(), 2))));
    }
    int numValues = random().nextInt(5);
    for (int i = 0; i < numValues; ++i) {
      doc.add(
          new SortedSetDocValuesField(
              "ssdv", newBytesRef(TestUtil.randomSimpleString(random(), 2))));
    }
    numValues = random().nextInt(5);
    for (int i = 0; i < numValues; ++i) {
      doc.add(new SortedNumericDocValuesField("sndv", TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE)));
    }
  }

  public void testOneNumber() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new NumericDocValuesField("dv", 5));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.count(new TermQuery(new Term("fieldname", longTerm))));
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, 1);
    assertEquals(1, hits.totalHits.value);
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      Document hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv");
      int docID = hits.scoreDocs[i].doc;
      assertEquals(docID, dv.advance(docID));
      assertEquals(5, dv.longValue());
    }

    ireader.close();
    directory.close();
  }

  public void testOneFloat() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new FloatDocValuesField("dv", 5.7f));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.count(new TermQuery(new Term("fieldname", longTerm))));
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, 1);
    assertEquals(1, hits.totalHits.value);
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int docID = hits.scoreDocs[i].doc;
      Document hitDoc = isearcher.doc(docID);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      
      NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv");
      assertEquals(docID, dv.advance(docID));
      assertEquals(Float.floatToRawIntBits(5.7f), dv.longValue());
    }

    ireader.close();
    directory.close();
  }
  
  public void testTwoNumbers() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new NumericDocValuesField("dv1", 5));
    doc.add(new NumericDocValuesField("dv2", 17));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.count(new TermQuery(new Term("fieldname", longTerm))));
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, 1);
    assertEquals(1, hits.totalHits.value);
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int docID = hits.scoreDocs[i].doc;
      Document hitDoc = isearcher.doc(docID);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv1");
      assertEquals(docID, dv.advance(docID));
      assertEquals(5, dv.longValue());
      dv = ireader.leaves().get(0).reader().getNumericDocValues("dv2");
      assertEquals(docID, dv.advance(docID));
      assertEquals(17, dv.longValue());
    }

    ireader.close();
    directory.close();
  }

  public void testTwoBinaryValues() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new BinaryDocValuesField("dv1", newBytesRef(longTerm)));
    doc.add(new BinaryDocValuesField("dv2", newBytesRef(text)));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.count(new TermQuery(new Term("fieldname", longTerm))));
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, 1);
    assertEquals(1, hits.totalHits.value);
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int hitDocID = hits.scoreDocs[i].doc;
      Document hitDoc = isearcher.doc(hitDocID);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv1");
      assertEquals(hitDocID, dv.advance(hitDocID));
      BytesRef scratch = dv.binaryValue();
      assertEquals(newBytesRef(longTerm), scratch);
      dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv2");
      assertEquals(hitDocID, dv.advance(hitDocID));
      scratch = dv.binaryValue();
      assertEquals(newBytesRef(text), scratch);
    }

    ireader.close();
    directory.close();
  }
  
  public void testVariouslyCompressibleBinaryValues() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
    int numDocs = 1 + random().nextInt(100);

    HashMap<Integer,BytesRef> writtenValues = new HashMap<>(numDocs);
    
    // Small vocabulary ranges will be highly compressible 
    int vocabRange = 1 + random().nextInt(Byte.MAX_VALUE - 1);

    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      
      // Generate random-sized byte array with random choice of bytes in vocab range
      byte[] value = new byte[500 + random().nextInt(1024)];
      for (int j = 0; j < value.length; j++) {
        value[j] = (byte) random().nextInt(vocabRange);
      }
      BytesRef bytesRef = newBytesRef(value);
      writtenValues.put(i, bytesRef);
      doc.add(newTextField("id", Integer.toString(i), Field.Store.YES));
      doc.add(new BinaryDocValuesField("dv1", bytesRef));
      iwriter.addDocument(doc);
    }
    iwriter.forceMerge(1);
    iwriter.close();

    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    for (int i = 0; i < numDocs; i++) {
      String id = Integer.toString(i);
      Query query = new TermQuery(new Term("id", id));
      TopDocs hits = isearcher.search(query, 1);
      assertEquals(1, hits.totalHits.value);
      // Iterate through the results:
      int hitDocID = hits.scoreDocs[0].doc;
      Document hitDoc = isearcher.doc(hitDocID);
      assertEquals(id, hitDoc.get("id"));
      assert ireader.leaves().size() == 1;
      BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv1");
      assertEquals(hitDocID, dv.advance(hitDocID));
      BytesRef scratch = dv.binaryValue();
      assertEquals(writtenValues.get(i), scratch);
    }

    ireader.close();
    directory.close();
  }  

  public void testTwoFieldsMixed() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new NumericDocValuesField("dv1", 5));
    doc.add(new BinaryDocValuesField("dv2", newBytesRef("hello world")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.count(new TermQuery(new Term("fieldname", longTerm))));
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, 1);
    assertEquals(1, hits.totalHits.value);
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int docID = hits.scoreDocs[i].doc;
      Document hitDoc = isearcher.doc(docID);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv1");
      assertEquals(docID, dv.advance(docID));
      assertEquals(5, dv.longValue());
      BinaryDocValues dv2 = ireader.leaves().get(0).reader().getBinaryDocValues("dv2");
      assertEquals(docID, dv2.advance(docID));
      assertEquals(newBytesRef("hello world"), dv2.binaryValue());
    }

    ireader.close();
    directory.close();
  }
  
  public void testThreeFieldsMixed() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new SortedDocValuesField("dv1", newBytesRef("hello hello")));
    doc.add(new NumericDocValuesField("dv2", 5));
    doc.add(new BinaryDocValuesField("dv3", newBytesRef("hello world")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.count(new TermQuery(new Term("fieldname", longTerm))));
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, 1);
    assertEquals(1, hits.totalHits.value);
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int docID = hits.scoreDocs[i].doc;
      Document hitDoc = isearcher.doc(docID);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv1");
      assertEquals(docID, dv.advance(docID));
      int ord = dv.ordValue();
      BytesRef scratch = dv.lookupOrd(ord);
      assertEquals(newBytesRef("hello hello"), scratch);
      NumericDocValues dv2 = ireader.leaves().get(0).reader().getNumericDocValues("dv2");
      assertEquals(docID, dv2.advance(docID));
      assertEquals(5, dv2.longValue());
      BinaryDocValues dv3 = ireader.leaves().get(0).reader().getBinaryDocValues("dv3");
      assertEquals(docID, dv3.advance(docID));
      assertEquals(newBytesRef("hello world"), dv3.binaryValue());
    }

    ireader.close();
    directory.close();
  }
  
  public void testThreeFieldsMixed2() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new BinaryDocValuesField("dv1", newBytesRef("hello world")));
    doc.add(new SortedDocValuesField("dv2", newBytesRef("hello hello")));
    doc.add(new NumericDocValuesField("dv3", 5));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.count(new TermQuery(new Term("fieldname", longTerm))));
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, 1);
    assertEquals(1, hits.totalHits.value);
    BytesRef scratch = newBytesRef();
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int docID = hits.scoreDocs[i].doc;
      Document hitDoc = isearcher.doc(docID);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv2");
      assertEquals(docID, dv.advance(docID));
      int ord = dv.ordValue();
      scratch = dv.lookupOrd(ord);
      assertEquals(newBytesRef("hello hello"), scratch);
      NumericDocValues dv2 = ireader.leaves().get(0).reader().getNumericDocValues("dv3");
      assertEquals(docID, dv2.advance(docID));
      assertEquals(5, dv2.longValue());
      BinaryDocValues dv3 = ireader.leaves().get(0).reader().getBinaryDocValues("dv1");
      assertEquals(docID, dv3.advance(docID));
      assertEquals(newBytesRef("hello world"), dv3.binaryValue());
    }

    ireader.close();
    directory.close();
  }
  
  public void testTwoDocumentsNumeric() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 1));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("dv", 2));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals(1, dv.longValue());
    assertEquals(1, dv.nextDoc());
    assertEquals(2, dv.longValue());

    ireader.close();
    directory.close();
  }
  
  public void testTwoDocumentsMerged() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(newField("id", "0", StringField.TYPE_STORED));
    doc.add(new NumericDocValuesField("dv", -10));
    iwriter.addDocument(doc);
    iwriter.commit();
    doc = new Document();
    doc.add(newField("id", "1", StringField.TYPE_STORED));
    doc.add(new NumericDocValuesField("dv", 99));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv");
    for(int i=0;i<2;i++) {
      Document doc2 = ireader.leaves().get(0).reader().document(i);
      long expected;
      if (doc2.get("id").equals("0")) {
        expected = -10;
      } else {
        expected = 99;
      }
      assertEquals(i, dv.nextDoc());
      assertEquals(expected, dv.longValue());
    }

    ireader.close();
    directory.close();
  }

  public void testBigNumericRange() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", Long.MIN_VALUE));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("dv", Long.MAX_VALUE));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals(Long.MIN_VALUE, dv.longValue());
    assertEquals(1, dv.nextDoc());
    assertEquals(Long.MAX_VALUE, dv.longValue());

    ireader.close();
    directory.close();
  }
  
  public void testBigNumericRange2() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", -8841491950446638677L));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("dv", 9062230939892376225L));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals(-8841491950446638677L, dv.longValue());
    assertEquals(1, dv.nextDoc());
    assertEquals(9062230939892376225L, dv.longValue());

    ireader.close();
    directory.close();
  }
  
  public void testBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new BinaryDocValuesField("dv", newBytesRef("hello world")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.count(new TermQuery(new Term("fieldname", longTerm))));
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, 1);
    assertEquals(1, hits.totalHits.value);
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int hitDocID = hits.scoreDocs[i].doc;
      Document hitDoc = isearcher.doc(hitDocID);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv");
      assertEquals(hitDocID, dv.advance(hitDocID));
      assertEquals(newBytesRef("hello world"), dv.binaryValue());
    }

    ireader.close();
    directory.close();
  }
  
  public void testBytesTwoDocumentsMerged() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(newField("id", "0", StringField.TYPE_STORED));
    doc.add(new BinaryDocValuesField("dv", newBytesRef("hello world 1")));
    iwriter.addDocument(doc);
    iwriter.commit();
    doc = new Document();
    doc.add(newField("id", "1", StringField.TYPE_STORED));
    doc.add(new BinaryDocValuesField("dv", newBytesRef("hello 2")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv");
    for(int i=0;i<2;i++) {
      Document doc2 = ireader.leaves().get(0).reader().document(i);
      String expected;
      if (doc2.get("id").equals("0")) {
        expected = "hello world 1";
      } else {
        expected = "hello 2";
      }
      assertEquals(i, dv.nextDoc());
      assertEquals(expected, dv.binaryValue().utf8ToString());
    }

    ireader.close();
    directory.close();
  }
  
  public void testBytesMergeAwayAllValues() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    iwriter.addDocument(doc);    
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new BinaryDocValuesField("field", newBytesRef("hi")));
    iwriter.addDocument(doc);
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();
    
    BinaryDocValues dv = getOnlyLeafReader(ireader).getBinaryDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());
    
    ireader.close();
    directory.close();
  }

  public void testSortedBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new SortedDocValuesField("dv", newBytesRef("hello world")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.count(new TermQuery(new Term("fieldname", longTerm))));
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, 1);
    assertEquals(1, hits.totalHits.value);
    BytesRef scratch = newBytesRef();
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int docID = hits.scoreDocs[i].doc;
      Document hitDoc = isearcher.doc(docID);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
      assertEquals(docID, dv.advance(docID));
      scratch = dv.lookupOrd(dv.ordValue());
      assertEquals(newBytesRef("hello world"), scratch);
    }

    ireader.close();
    directory.close();
  }

  public void testSortedBytesTwoDocuments() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", newBytesRef("hello world 1")));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", newBytesRef("hello world 2")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
    BytesRef scratch = newBytesRef();
    assertEquals(0, dv.nextDoc());
    scratch = dv.lookupOrd(dv.ordValue());
    assertEquals("hello world 1", scratch.utf8ToString());
    assertEquals(1, dv.nextDoc());
    scratch = dv.lookupOrd(dv.ordValue());
    assertEquals("hello world 2", scratch.utf8ToString());

    ireader.close();
    directory.close();
  }
  
  public void testSortedBytesThreeDocuments() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", newBytesRef("hello world 1")));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", newBytesRef("hello world 2")));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", newBytesRef("hello world 1")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
    assertEquals(2, dv.getValueCount());
    assertEquals(0, dv.nextDoc());
    assertEquals(0, dv.ordValue());
    BytesRef scratch = dv.lookupOrd(0);
    assertEquals("hello world 1", scratch.utf8ToString());
    assertEquals(1, dv.nextDoc());
    assertEquals(1, dv.ordValue());
    scratch = dv.lookupOrd(1);
    assertEquals("hello world 2", scratch.utf8ToString());
    assertEquals(2, dv.nextDoc());
    assertEquals(0, dv.ordValue());

    ireader.close();
    directory.close();
  }

  public void testSortedBytesTwoDocumentsMerged() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(newField("id", "0", StringField.TYPE_STORED));
    doc.add(new SortedDocValuesField("dv", newBytesRef("hello world 1")));
    iwriter.addDocument(doc);
    iwriter.commit();
    doc = new Document();
    doc.add(newField("id", "1", StringField.TYPE_STORED));
    doc.add(new SortedDocValuesField("dv", newBytesRef("hello world 2")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
    assertEquals(2, dv.getValueCount()); // 2 ords
    assertEquals(0, dv.nextDoc());
    BytesRef scratch = dv.lookupOrd(dv.ordValue());
    assertEquals(newBytesRef("hello world 1"), scratch);
    scratch = dv.lookupOrd(1);
    assertEquals(newBytesRef("hello world 2"), scratch);
    for (int i = 0; i < 2; i++) {
      Document doc2 = ireader.leaves().get(0).reader().document(i);
      String expected;
      if (doc2.get("id").equals("0")) {
        expected = "hello world 1";
      } else {
        expected = "hello world 2";
      }
      if (dv.docID() < i) {
        assertEquals(i, dv.nextDoc());
      }
      scratch = dv.lookupOrd(dv.ordValue());
      assertEquals(expected, scratch.utf8ToString());
    }

    ireader.close();
    directory.close();
  }
  
  public void testSortedMergeAwayAllValues() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    iwriter.addDocument(doc);    
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new SortedDocValuesField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();
    
    SortedDocValues dv = getOnlyLeafReader(ireader).getSortedDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());
    
    ireader.close();
    directory.close();
  }

  public void testBytesWithNewline() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("dv", newBytesRef("hello\nworld\r1")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals(newBytesRef("hello\nworld\r1"), dv.binaryValue());

    ireader.close();
    directory.close();
  }

  public void testMissingSortedBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", newBytesRef("hello world 2")));
    iwriter.addDocument(doc);
    // 2nd doc missing the DV field
    iwriter.addDocument(new Document());
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
    assertEquals(0, dv.nextDoc());
    BytesRef scratch = dv.lookupOrd(dv.ordValue());
    assertEquals(newBytesRef("hello world 2"), scratch);
    assertEquals(NO_MORE_DOCS, dv.nextDoc());
    ireader.close();
    directory.close();
  }
  
  public void testSortedTermsEnum() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    doc.add(new SortedDocValuesField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new SortedDocValuesField("field", newBytesRef("world")));
    iwriter.addDocument(doc);

    doc = new Document();
    doc.add(new SortedDocValuesField("field", newBytesRef("beer")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedDocValues dv = getOnlyLeafReader(ireader).getSortedDocValues("field");
    assertEquals(3, dv.getValueCount());
    
    TermsEnum termsEnum = dv.termsEnum();
    
    // next()
    assertEquals("beer", termsEnum.next().utf8ToString());
    assertEquals(0, termsEnum.ord());
    assertEquals("hello", termsEnum.next().utf8ToString());
    assertEquals(1, termsEnum.ord());
    assertEquals("world", termsEnum.next().utf8ToString());
    assertEquals(2, termsEnum.ord());
    
    // seekCeil()
    assertEquals(SeekStatus.NOT_FOUND, termsEnum.seekCeil(newBytesRef("ha!")));
    assertEquals("hello", termsEnum.term().utf8ToString());
    assertEquals(1, termsEnum.ord());
    assertEquals(SeekStatus.FOUND, termsEnum.seekCeil(newBytesRef("beer")));
    assertEquals("beer", termsEnum.term().utf8ToString());
    assertEquals(0, termsEnum.ord());
    assertEquals(SeekStatus.END, termsEnum.seekCeil(newBytesRef("zzz")));
    assertEquals(SeekStatus.NOT_FOUND, termsEnum.seekCeil(newBytesRef("aba")));
    assertEquals(0, termsEnum.ord());
    
    // seekExact()
    assertTrue(termsEnum.seekExact(newBytesRef("beer")));
    assertEquals("beer", termsEnum.term().utf8ToString());
    assertEquals(0, termsEnum.ord());
    assertTrue(termsEnum.seekExact(newBytesRef("hello")));
    assertEquals(Codec.getDefault().toString(), "hello", termsEnum.term().utf8ToString());
    assertEquals(1, termsEnum.ord());
    assertTrue(termsEnum.seekExact(newBytesRef("world")));
    assertEquals("world", termsEnum.term().utf8ToString());
    assertEquals(2, termsEnum.ord());
    assertFalse(termsEnum.seekExact(newBytesRef("bogus")));

    // seek(ord)
    termsEnum.seekExact(0);
    assertEquals("beer", termsEnum.term().utf8ToString());
    assertEquals(0, termsEnum.ord());
    termsEnum.seekExact(1);
    assertEquals("hello", termsEnum.term().utf8ToString());
    assertEquals(1, termsEnum.ord());
    termsEnum.seekExact(2);
    assertEquals("world", termsEnum.term().utf8ToString());
    assertEquals(2, termsEnum.ord());

    // NORMAL automaton
    termsEnum = dv.intersect(new CompiledAutomaton(new RegExp(".*l.*").toAutomaton()));
    assertEquals("hello", termsEnum.next().utf8ToString());
    assertEquals(1, termsEnum.ord());
    assertEquals("world", termsEnum.next().utf8ToString());
    assertEquals(2, termsEnum.ord());
    assertNull(termsEnum.next());

    // SINGLE automaton
    termsEnum = dv.intersect(new CompiledAutomaton(new RegExp("hello").toAutomaton()));
    assertEquals("hello", termsEnum.next().utf8ToString());
    assertEquals(1, termsEnum.ord());
    assertNull(termsEnum.next());

    ireader.close();
    directory.close();
  }
  
  public void testEmptySortedBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", newBytesRef("")));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", newBytesRef("")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals(0, dv.ordValue());
    assertEquals(1, dv.nextDoc());
    assertEquals(0, dv.ordValue());
    BytesRef scratch = dv.lookupOrd(0);
    assertEquals("", scratch.utf8ToString());

    ireader.close();
    directory.close();
  }
  
  public void testEmptyBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("dv", newBytesRef("")));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new BinaryDocValuesField("dv", newBytesRef("")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals("", dv.binaryValue().utf8ToString());
    assertEquals(1, dv.nextDoc());
    assertEquals("", dv.binaryValue().utf8ToString());

    ireader.close();
    directory.close();
  }
  
  public void testVeryLargeButLegalBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    byte bytes[] = new byte[32766];
    random().nextBytes(bytes);
    BytesRef b = newBytesRef(bytes);
    doc.add(new BinaryDocValuesField("dv", b));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals(newBytesRef(bytes), dv.binaryValue());

    ireader.close();
    directory.close();
  }
  
  public void testVeryLargeButLegalSortedBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    byte bytes[] = new byte[32766];
    random().nextBytes(bytes);
    BytesRef b = newBytesRef(bytes);
    doc.add(new SortedDocValuesField("dv", b));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    SortedDocValues dv = DocValues.getSorted(ireader.leaves().get(0).reader(), "dv");
    assertEquals(0, dv.nextDoc());
    assertEquals(newBytesRef(bytes), dv.lookupOrd(dv.ordValue()));
    ireader.close();
    directory.close();
  }
  
  public void testCodecUsesOwnBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("dv", newBytesRef("boo!")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals("boo!", dv.binaryValue().utf8ToString());

    ireader.close();
    directory.close();
  }
  
  public void testCodecUsesOwnSortedBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", newBytesRef("boo!")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = DocValues.getBinary(ireader.leaves().get(0).reader(), "dv");
    byte mybytes[] = new byte[20];
    assertEquals(0, dv.nextDoc());
    assertEquals("boo!", dv.binaryValue().utf8ToString());
    assertFalse(dv.binaryValue().bytes == mybytes);

    ireader.close();
    directory.close();
  }
  
  /*
   * Simple test case to show how to use the API
   */
  public void testDocValuesSimple() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter writer = new IndexWriter(dir, conf);
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("docId", i));
      doc.add(new TextField("docId", "" + i, Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.commit();
    writer.forceMerge(1, true);

    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    assertEquals(1, reader.leaves().size());
  
    IndexSearcher searcher = new IndexSearcher(reader);

    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term("docId", "0")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("docId", "1")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("docId", "2")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("docId", "3")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("docId", "4")), BooleanClause.Occur.SHOULD);

    TopDocs search = searcher.search(query.build(), 10);
    assertEquals(5, search.totalHits.value);
    ScoreDoc[] scoreDocs = search.scoreDocs;
    NumericDocValues docValues = getOnlyLeafReader(reader).getNumericDocValues("docId");
    for (int i = 0; i < scoreDocs.length; i++) {
      assertEquals(i, scoreDocs[i].doc);
      assertEquals(i, docValues.advance(i));
      assertEquals(i, docValues.longValue());
    }
    reader.close();
    dir.close();
  }
  
  public void testRandomSortedBytes() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig cfg = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, cfg);
    int numDocs = atLeast(100);
    BytesRefHash hash = new BytesRefHash();
    Map<String, String> docToString = new HashMap<>();
    int maxLength = TestUtil.nextInt(random(), 1, 50);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(newTextField("id", "" + i, Field.Store.YES));
      String string = TestUtil.randomRealisticUnicodeString(random(), 1, maxLength);
      BytesRef br = newBytesRef(string);
      doc.add(new SortedDocValuesField("field", br));
      hash.add(br);
      docToString.put("" + i, string);
      w.addDocument(doc);
    }
    if (rarely()) {
      w.commit();
    }
    int numDocsNoValue = atLeast(10);
    for (int i = 0; i < numDocsNoValue; i++) {
      Document doc = new Document();
      doc.add(newTextField("id", "noValue", Field.Store.YES));
      w.addDocument(doc);
    }
    if (rarely()) {
      w.commit();
    }
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      String id = "" + i + numDocs;
      doc.add(newTextField("id", id, Field.Store.YES));
      String string = TestUtil.randomRealisticUnicodeString(random(), 1, maxLength);
      BytesRef br = newBytesRef(string);
      hash.add(br);
      docToString.put(id, string);
      doc.add(new SortedDocValuesField("field", br));
      w.addDocument(doc);
    }
    w.commit();
    IndexReader reader = w.getReader();
    SortedDocValues docValues = MultiDocValues.getSortedValues(reader, "field");
    int[] sort = hash.sort();
    BytesRef expected = newBytesRef();
    assertEquals(hash.size(), docValues.getValueCount());
    for (int i = 0; i < hash.size(); i++) {
      hash.get(sort[i], expected);
      final BytesRef actual = docValues.lookupOrd(i);
      assertEquals(expected.utf8ToString(), actual.utf8ToString());
      int ord = docValues.lookupTerm(expected);
      assertEquals(i, ord);
    }
    Set<Entry<String, String>> entrySet = docToString.entrySet();

    for (Entry<String, String> entry : entrySet) {
      // pk lookup
      PostingsEnum termPostingsEnum =
          TestUtil.docs(random(), reader, "id", newBytesRef(entry.getKey()), null, 0);
      int docId = termPostingsEnum.nextDoc();
      expected = newBytesRef(entry.getValue());
      docValues = MultiDocValues.getSortedValues(reader, "field");
      assertEquals(docId, docValues.advance(docId));
      final BytesRef actual = docValues.binaryValue();
      assertEquals(expected, actual);
    }

    reader.close();
    w.close();
    dir.close();
  }

  private void doTestNumericsVsStoredFields(double density, LongSupplier longs) throws Exception {
    doTestNumericsVsStoredFields(density, longs, 256);
  }
  private void doTestNumericsVsStoredFields(double density, LongSupplier longs, int minDocs) throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    Document doc = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    Field storedField = newStringField("stored", "", Field.Store.YES);
    Field dvField = new NumericDocValuesField("dv", 0);
    doc.add(idField);
    doc.add(storedField);
    doc.add(dvField);
    
    // index some docs
    int numDocs = atLeast((int) (minDocs*1.172));
    // numDocs should be always > 256 so that in case of a codec that optimizes
    // for numbers of values <= 256, all storage layouts are tested
    assert numDocs > 256;
    for (int i = 0; i < numDocs; i++) {
      if (random().nextDouble() > density) {
        writer.addDocument(new Document());
        continue;
      }
      idField.setStringValue(Integer.toString(i));
      long value = longs.getAsLong();
      storedField.setStringValue(Long.toString(value));
      dvField.setLongValue(value);
      writer.addDocument(doc);
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }
    
    // delete some docs
    int numDeletions = random().nextInt(numDocs/10);
    for (int i = 0; i < numDeletions; i++) {
      int id = random().nextInt(numDocs);
      writer.deleteDocuments(new Term("id", Integer.toString(id)));
    }

    // merge some segments and ensure that at least one of them has more than
    // max(256, minDocs) values
    writer.forceMerge(numDocs / Math.max(256, minDocs));

    writer.close();
    // compare
    assertDVIterate(dir);
    dir.close();
  }

  // Asserts equality of stored value vs. DocValue by iterating DocValues one at a time
  protected void assertDVIterate(Directory dir) throws IOException {
    DirectoryReader ir = DirectoryReader.open(dir);
    TestUtil.checkReader(ir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      NumericDocValues docValues = DocValues.getNumeric(r, "dv");
      docValues.nextDoc();
      for (int i = 0; i < r.maxDoc(); i++) {
        String storedValue = r.document(i).get("stored");
        if (storedValue == null) {
          assertTrue(docValues.docID() > i);
        } else {
          assertEquals(i, docValues.docID());
          assertEquals(Long.parseLong(storedValue), docValues.longValue());
          docValues.nextDoc();
        }
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.docID());
    }
    ir.close();
  }

  private void doTestSortedNumericsVsStoredFields(LongSupplier counts, LongSupplier values) throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    
    // index some docs
    int numDocs = atLeast(300);
    // numDocs should be always > 256 so that in case of a codec that optimizes
    // for numbers of values <= 256, all storage layouts are tested
    assert numDocs > 256;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
      
      int valueCount = (int) counts.getAsLong();
      long valueArray[] = new long[valueCount];
      for (int j = 0; j < valueCount; j++) {
        long value = values.getAsLong();
        valueArray[j] = value;
        doc.add(new SortedNumericDocValuesField("dv", value));
      }
      Arrays.sort(valueArray);
      for (int j = 0; j < valueCount; j++) {
        doc.add(new StoredField("stored", Long.toString(valueArray[j])));
      }
      writer.addDocument(doc);
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }
    
    // delete some docs
    int numDeletions = random().nextInt(numDocs/10);
    for (int i = 0; i < numDeletions; i++) {
      int id = random().nextInt(numDocs);
      writer.deleteDocuments(new Term("id", Integer.toString(id)));
    }

    // merge some segments and ensure that at least one of them has more than
    // 256 values
    writer.forceMerge(numDocs / 256);

    writer.close();
    
    // compare
    DirectoryReader ir = DirectoryReader.open(dir);
    TestUtil.checkReader(ir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(r, "dv");
      for (int i = 0; i < r.maxDoc(); i++) {
        if (i > docValues.docID()) {
          docValues.nextDoc();
        }
        String expected[] = r.document(i).getValues("stored");
        if (i < docValues.docID()) {
          assertEquals(0, expected.length);
        } else {
          String actual[] = new String[docValues.docValueCount()];
          for (int j = 0; j < actual.length; j++) {
            actual[j] = Long.toString(docValues.nextValue());
          }
          assertArrayEquals(expected, actual);
        }
      }
    }
    ir.close();
    dir.close();
  }
  
  public void testBooleanNumericsVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestNumericsVsStoredFields(1, () -> random().nextInt(2));
    }
  }

  public void testSparseBooleanNumericsVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestNumericsVsStoredFields(random().nextDouble(), () -> random().nextInt(2));
    }
  }

  public void testByteNumericsVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestNumericsVsStoredFields(1, () -> TestUtil.nextInt(random(), Byte.MIN_VALUE, Byte.MAX_VALUE));
    }
  }

  public void testSparseByteNumericsVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestNumericsVsStoredFields(random().nextDouble(), () -> TestUtil.nextInt(random(), Byte.MIN_VALUE, Byte.MAX_VALUE));
    }
  }

  public void testShortNumericsVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestNumericsVsStoredFields(1, () -> TestUtil.nextInt(random(), Short.MIN_VALUE, Short.MAX_VALUE));
    }
  }

  public void testSparseShortNumericsVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestNumericsVsStoredFields(random().nextDouble(), () -> TestUtil.nextInt(random(), Short.MIN_VALUE, Short.MAX_VALUE));
    }
  }

  public void testIntNumericsVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestNumericsVsStoredFields(1, random()::nextInt);
    }
  }
  
  public void testSparseIntNumericsVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestNumericsVsStoredFields(random().nextDouble(), random()::nextInt);
    }
  }
  
  public void testLongNumericsVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestNumericsVsStoredFields(1, random()::nextLong);
    }
  }
  
  public void testSparseLongNumericsVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestNumericsVsStoredFields(random().nextDouble(), random()::nextLong);
    }
  }

  private void doTestBinaryVsStoredFields(double density, Supplier<byte[]> bytes) throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    Document doc = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    Field storedField = new StoredField("stored", new byte[0]);
    Field dvField = new BinaryDocValuesField("dv", newBytesRef());
    doc.add(idField);
    doc.add(storedField);
    doc.add(dvField);
    
    // index some docs
    int numDocs = atLeast(300);
    for (int i = 0; i < numDocs; i++) {
      if (random().nextDouble() > density) {
        writer.addDocument(new Document());
        continue;
      }
      idField.setStringValue(Integer.toString(i));
      byte[] buffer = bytes.get();
      storedField.setBytesValue(buffer);
      dvField.setBytesValue(buffer);
      writer.addDocument(doc);
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }
    
    // delete some docs
    int numDeletions = random().nextInt(numDocs/10);
    for (int i = 0; i < numDeletions; i++) {
      int id = random().nextInt(numDocs);
      writer.deleteDocuments(new Term("id", Integer.toString(id)));
    }
    
    // compare
    DirectoryReader ir = writer.getReader();
    TestUtil.checkReader(ir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      BinaryDocValues docValues = DocValues.getBinary(r, "dv");
      docValues.nextDoc();
      for (int i = 0; i < r.maxDoc(); i++) {
        BytesRef binaryValue = r.document(i).getBinaryValue("stored");
        if (binaryValue == null) {
          assertTrue(docValues.docID() > i);
        } else {
          assertEquals(i, docValues.docID());
          assertEquals(binaryValue, docValues.binaryValue());
          docValues.nextDoc();
        }
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.docID());
    }
    ir.close();
    
    // compare again
    writer.forceMerge(1);
    ir = writer.getReader();
    TestUtil.checkReader(ir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      BinaryDocValues docValues = DocValues.getBinary(r, "dv");
      docValues.nextDoc();
      for (int i = 0; i < r.maxDoc(); i++) {
        BytesRef binaryValue = r.document(i).getBinaryValue("stored");
        if (binaryValue == null) {
          assertTrue(docValues.docID() > i);
        } else {
          assertEquals(i, docValues.docID());
          assertEquals(binaryValue, docValues.binaryValue());
          docValues.nextDoc();
        }
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.docID());
    }
    ir.close();
    writer.close();
    dir.close();
  }
  
  public void testBinaryFixedLengthVsStoredFields() throws Exception {
    doTestBinaryFixedLengthVsStoredFields(1);
  }

  public void testSparseBinaryFixedLengthVsStoredFields() throws Exception {
    doTestBinaryFixedLengthVsStoredFields(random().nextDouble());
  }

  private void doTestBinaryFixedLengthVsStoredFields(double density) throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      int fixedLength = TestUtil.nextInt(random(), 0, 10);
      doTestBinaryVsStoredFields(density, () -> {
        byte buffer[] = new byte[fixedLength];
        random().nextBytes(buffer);
        return buffer;
      });
    }
  }

  public void testBinaryVariableLengthVsStoredFields() throws Exception {
    doTestBinaryVariableLengthVsStoredFields(1);
  }

  public void testSparseBinaryVariableLengthVsStoredFields() throws Exception {
    doTestBinaryVariableLengthVsStoredFields(random().nextDouble());
  }

  public void doTestBinaryVariableLengthVsStoredFields(double density) throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestBinaryVsStoredFields(density, () -> {
        final int length = random().nextInt(10);
        byte buffer[] = new byte[length];
        random().nextBytes(buffer);
        return buffer;
      });
    }
  }
  
  protected void doTestSortedVsStoredFields(int numDocs, double density, Supplier<byte[]> bytes) throws Exception {
    Directory dir = newFSDirectory(createTempDir("dvduel"));
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    Document doc = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    Field storedField = new StoredField("stored", new byte[0]);
    Field dvField = new SortedDocValuesField("dv", newBytesRef());
    doc.add(idField);
    doc.add(storedField);
    doc.add(dvField);
    
    // index some docs
    for (int i = 0; i < numDocs; i++) {
      if (random().nextDouble() > density) {
        writer.addDocument(new Document());
        continue;
      }
      idField.setStringValue(Integer.toString(i));
      byte[] buffer = bytes.get();
      storedField.setBytesValue(buffer);
      dvField.setBytesValue(buffer);
      writer.addDocument(doc);
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }
    
    // delete some docs
    int numDeletions = random().nextInt(numDocs/10);
    for (int i = 0; i < numDeletions; i++) {
      int id = random().nextInt(numDocs);
      writer.deleteDocuments(new Term("id", Integer.toString(id)));
    }
    
    // compare
    DirectoryReader ir = writer.getReader();
    TestUtil.checkReader(ir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      BinaryDocValues docValues = DocValues.getBinary(r, "dv");
      docValues.nextDoc();
      for (int i = 0; i < r.maxDoc(); i++) {
        BytesRef binaryValue = r.document(i).getBinaryValue("stored");
        if (binaryValue == null) {
          assertTrue(docValues.docID() > i);
        } else {
          assertEquals(i, docValues.docID());
          assertEquals(binaryValue, docValues.binaryValue());
          docValues.nextDoc();
        }
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.docID());
    }
    ir.close();
    writer.forceMerge(1);
    
    // compare again
    ir = writer.getReader();
    TestUtil.checkReader(ir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      BinaryDocValues docValues = DocValues.getBinary(r, "dv");
      docValues.nextDoc();
      for (int i = 0; i < r.maxDoc(); i++) {
        BytesRef binaryValue = r.document(i).getBinaryValue("stored");
        if (binaryValue == null) {
          assertTrue(docValues.docID() > i);
        } else {
          assertEquals(i, docValues.docID());
          assertEquals(binaryValue, docValues.binaryValue());
          docValues.nextDoc();
        }
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.docID());
    }
    ir.close();
    writer.close();
    dir.close();
  }
  
  public void testSortedFixedLengthVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      int fixedLength = TestUtil.nextInt(random(), 1, 10);
      doTestSortedVsStoredFields(atLeast(300), 1, fixedLength, fixedLength);
    }
  }
  
  public void testSparseSortedFixedLengthVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      int fixedLength = TestUtil.nextInt(random(), 1, 10);
      doTestSortedVsStoredFields(atLeast(300), random().nextDouble(), fixedLength, fixedLength);
    }
  }
  
  public void testSortedVariableLengthVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedVsStoredFields(atLeast(300), 1, 1, 10);
    }
  }

  public void testSparseSortedVariableLengthVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedVsStoredFields(atLeast(300), random().nextDouble(), 1, 10);
    }
  }

  protected void doTestSortedVsStoredFields(int numDocs, double density, int minLength, int maxLength) throws Exception {
    doTestSortedVsStoredFields(numDocs, density, () -> {
      int length = TestUtil.nextInt(random(), minLength, maxLength);
      byte[] buffer = new byte[length];
      random().nextBytes(buffer);
      return buffer;
    });
  }

  public void testSortedSetOneValue() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
    
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();
    
    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(0, dv.nextDoc());
    assertEquals(0, dv.nextOrd());
    assertEquals(NO_MORE_ORDS, dv.nextOrd());
    
    BytesRef bytes = dv.lookupOrd(0);
    assertEquals(newBytesRef("hello"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testSortedSetTwoFields() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
    
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    doc.add(new SortedSetDocValuesField("field2", newBytesRef("world")));
    iwriter.addDocument(doc);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();
    
    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(0, dv.nextDoc());
    
    assertEquals(0, dv.nextOrd());
    assertEquals(NO_MORE_ORDS, dv.nextOrd());
    
    BytesRef bytes = dv.lookupOrd(0);
    assertEquals(newBytesRef("hello"), bytes);

    dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field2");

    assertEquals(0, dv.nextDoc());
    assertEquals(0, dv.nextOrd());
    assertEquals(NO_MORE_ORDS, dv.nextOrd());
    
    bytes = dv.lookupOrd(0);
    assertEquals(newBytesRef("world"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testSortedSetTwoDocumentsMerged() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
  
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    iwriter.commit();
    
    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", newBytesRef("world")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(2, dv.getValueCount());
    
    assertEquals(0, dv.nextDoc());
    assertEquals(0, dv.nextOrd());
    assertEquals(NO_MORE_ORDS, dv.nextOrd());
    
    BytesRef bytes = dv.lookupOrd(0);
    assertEquals(newBytesRef("hello"), bytes);

    assertEquals(1, dv.nextDoc());
    assertEquals(1, dv.nextOrd());
    assertEquals(NO_MORE_ORDS, dv.nextOrd());
    
    bytes = dv.lookupOrd(1);
    assertEquals(newBytesRef("world"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testSortedSetTwoValues() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
    
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    doc.add(new SortedSetDocValuesField("field", newBytesRef("world")));
    iwriter.addDocument(doc);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();
    
    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(0, dv.nextDoc());
    
    assertEquals(0, dv.nextOrd());
    assertEquals(1, dv.nextOrd());
    assertEquals(NO_MORE_ORDS, dv.nextOrd());
    
    BytesRef bytes = dv.lookupOrd(0);
    assertEquals(newBytesRef("hello"), bytes);

    bytes = dv.lookupOrd(1);
    assertEquals(newBytesRef("world"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testSortedSetTwoValuesUnordered() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
    
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", newBytesRef("world")));
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();
    
    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(0, dv.nextDoc());
    
    assertEquals(0, dv.nextOrd());
    assertEquals(1, dv.nextOrd());
    assertEquals(NO_MORE_ORDS, dv.nextOrd());
    
    BytesRef bytes = dv.lookupOrd(0);
    assertEquals(newBytesRef("hello"), bytes);

    bytes = dv.lookupOrd(1);
    assertEquals(newBytesRef("world"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testSortedSetThreeValuesTwoDocs() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    doc.add(new SortedSetDocValuesField("field", newBytesRef("world")));
    iwriter.addDocument(doc);
    iwriter.commit();
    
    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    doc.add(new SortedSetDocValuesField("field", newBytesRef("beer")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(3, dv.getValueCount());
    
    assertEquals(0, dv.nextDoc());
    assertEquals(1, dv.nextOrd());
    assertEquals(2, dv.nextOrd());
    assertEquals(NO_MORE_ORDS, dv.nextOrd());
    
    assertEquals(1, dv.nextDoc());
    assertEquals(0, dv.nextOrd());
    assertEquals(1, dv.nextOrd());
    assertEquals(NO_MORE_ORDS, dv.nextOrd());
    
    BytesRef bytes = dv.lookupOrd(0);
    assertEquals(newBytesRef("beer"), bytes);

    bytes = dv.lookupOrd(1);
    assertEquals(newBytesRef("hello"), bytes);

    bytes = dv.lookupOrd(2);
    assertEquals(newBytesRef("world"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testSortedSetTwoDocumentsLastMissing() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    
    doc = new Document();
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();
    
    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(1, dv.getValueCount());
    
    assertEquals(0, dv.nextDoc());
    assertEquals(0, dv.nextOrd());
    assertEquals(NO_MORE_ORDS, dv.nextOrd());
    
    BytesRef bytes = dv.lookupOrd(0);
    assertEquals(newBytesRef("hello"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testSortedSetTwoDocumentsLastMissingMerge() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    iwriter.commit();
    
    doc = new Document();
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
   
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();
    
    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(1, dv.getValueCount());

    assertEquals(0, dv.nextDoc());
    assertEquals(0, dv.nextOrd());
    assertEquals(NO_MORE_ORDS, dv.nextOrd());
    
    BytesRef bytes = dv.lookupOrd(0);
    assertEquals(newBytesRef("hello"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testSortedSetTwoDocumentsFirstMissing() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    iwriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    
    iwriter.forceMerge(1);
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();
    
    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(1, dv.getValueCount());

    assertEquals(1, dv.nextDoc());
    assertEquals(0, dv.nextOrd());
    assertEquals(NO_MORE_ORDS, dv.nextOrd());
    
    BytesRef bytes = dv.lookupOrd(0);
    assertEquals(newBytesRef("hello"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testSortedSetTwoDocumentsFirstMissingMerge() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    iwriter.addDocument(doc);
    iwriter.commit();
    
    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();
    
    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(1, dv.getValueCount());

    assertEquals(1, dv.nextDoc());
    assertEquals(0, dv.nextOrd());
    assertEquals(NO_MORE_ORDS, dv.nextOrd());
    
    BytesRef bytes = dv.lookupOrd(0);
    assertEquals(newBytesRef("hello"), bytes);

    ireader.close();
    directory.close();
  }
  
  public void testSortedSetMergeAwayAllValues() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    iwriter.addDocument(doc);    
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();
    
    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(0, dv.getValueCount());
    
    ireader.close();
    directory.close();
  }
  
  public void testSortedSetTermsEnum() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    doc.add(new SortedSetDocValuesField("field", newBytesRef("world")));
    doc.add(new SortedSetDocValuesField("field", newBytesRef("beer")));
    iwriter.addDocument(doc);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(3, dv.getValueCount());
    
    TermsEnum termsEnum = dv.termsEnum();
    
    // next()
    assertEquals("beer", termsEnum.next().utf8ToString());
    assertEquals(0, termsEnum.ord());
    assertEquals("hello", termsEnum.next().utf8ToString());
    assertEquals(1, termsEnum.ord());
    assertEquals("world", termsEnum.next().utf8ToString());
    assertEquals(2, termsEnum.ord());
    
    // seekCeil()
    assertEquals(SeekStatus.NOT_FOUND, termsEnum.seekCeil(newBytesRef("ha!")));
    assertEquals("hello", termsEnum.term().utf8ToString());
    assertEquals(1, termsEnum.ord());
    assertEquals(SeekStatus.FOUND, termsEnum.seekCeil(newBytesRef("beer")));
    assertEquals("beer", termsEnum.term().utf8ToString());
    assertEquals(0, termsEnum.ord());
    assertEquals(SeekStatus.END, termsEnum.seekCeil(newBytesRef("zzz")));

    // seekExact()
    assertTrue(termsEnum.seekExact(newBytesRef("beer")));
    assertEquals("beer", termsEnum.term().utf8ToString());
    assertEquals(0, termsEnum.ord());
    assertTrue(termsEnum.seekExact(newBytesRef("hello")));
    assertEquals("hello", termsEnum.term().utf8ToString());
    assertEquals(1, termsEnum.ord());
    assertTrue(termsEnum.seekExact(newBytesRef("world")));
    assertEquals("world", termsEnum.term().utf8ToString());
    assertEquals(2, termsEnum.ord());
    assertFalse(termsEnum.seekExact(newBytesRef("bogus")));

    // seek(ord)
    termsEnum.seekExact(0);
    assertEquals("beer", termsEnum.term().utf8ToString());
    assertEquals(0, termsEnum.ord());
    termsEnum.seekExact(1);
    assertEquals("hello", termsEnum.term().utf8ToString());
    assertEquals(1, termsEnum.ord());
    termsEnum.seekExact(2);
    assertEquals("world", termsEnum.term().utf8ToString());
    assertEquals(2, termsEnum.ord());

    // NORMAL automaton
    termsEnum = dv.intersect(new CompiledAutomaton(new RegExp(".*l.*").toAutomaton()));
    assertEquals("hello", termsEnum.next().utf8ToString());
    assertEquals(1, termsEnum.ord());
    assertEquals("world", termsEnum.next().utf8ToString());
    assertEquals(2, termsEnum.ord());
    assertNull(termsEnum.next());

    // SINGLE automaton
    termsEnum = dv.intersect(new CompiledAutomaton(new RegExp("hello").toAutomaton()));
    assertEquals("hello", termsEnum.next().utf8ToString());
    assertEquals(1, termsEnum.ord());
    assertNull(termsEnum.next());

    ireader.close();
    directory.close();
  }

  protected void doTestSortedSetVsStoredFields(int numDocs, int minLength, int maxLength, int maxValuesPerDoc, int maxUniqueValues) throws Exception {
    Directory dir = newFSDirectory(createTempDir("dvduel"));
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);

    Set<String> valueSet = new HashSet<String>();
    for (int i = 0; i < 10000 && valueSet.size() < maxUniqueValues; ++i) {
      final int length = TestUtil.nextInt(random(), minLength, maxLength);
      valueSet.add(TestUtil.randomSimpleString(random(), length));
    }
    String[] uniqueValues = valueSet.toArray(new String[0]);

    // index some docs
    if (VERBOSE) {
      System.out.println("\nTEST: now add numDocs=" + numDocs);
    }
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      Field idField = new StringField("id", Integer.toString(i), Field.Store.NO);
      doc.add(idField);
      int numValues = TestUtil.nextInt(random(), 0, maxValuesPerDoc);
      // create a random set of strings
      Set<String> values = new TreeSet<>();
      for (int v = 0; v < numValues; v++) {
        values.add(RandomPicks.randomFrom(random(), uniqueValues));
      }

      // add ordered to the stored field
      for (String v : values) {
        doc.add(new StoredField("stored", v));
      }

      // add in any order to the dv field
      ArrayList<String> unordered = new ArrayList<>(values);
      Collections.shuffle(unordered, random());
      for (String v : unordered) {
        doc.add(new SortedSetDocValuesField("dv", newBytesRef(v)));
      }

      writer.addDocument(doc);
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }
    
    // delete some docs
    int numDeletions = random().nextInt(numDocs/10);
    if (VERBOSE) {
      System.out.println("\nTEST: now delete " + numDeletions + " docs");
    }
    for (int i = 0; i < numDeletions; i++) {
      int id = random().nextInt(numDocs);
      writer.deleteDocuments(new Term("id", Integer.toString(id)));
    }
    
    // compare
    if (VERBOSE) {
      System.out.println("\nTEST: now get reader");
    }
    DirectoryReader ir = writer.getReader();
    TestUtil.checkReader(ir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      SortedSetDocValues docValues = r.getSortedSetDocValues("dv");
      for (int i = 0; i < r.maxDoc(); i++) {
        String stringValues[] = r.document(i).getValues("stored");
        if (docValues != null) {
          if (docValues.docID() < i) {
            docValues.nextDoc();
          }
        }
        if (docValues != null && stringValues.length > 0) {
          assertEquals(i, docValues.docID());
          for (int j = 0; j < stringValues.length; j++) {
            assert docValues != null;
            long ord = docValues.nextOrd();
            assert ord != NO_MORE_ORDS;
            BytesRef scratch = docValues.lookupOrd(ord);
            assertEquals(stringValues[j], scratch.utf8ToString());
          }
          assertEquals(NO_MORE_ORDS, docValues.nextOrd());
        }
      }
    }
    if (VERBOSE) {
      System.out.println("\nTEST: now close reader");
    }
    ir.close();
    if (VERBOSE) {
      System.out.println("TEST: force merge");
    }
    writer.forceMerge(1);
    
    // compare again
    ir = writer.getReader();
    TestUtil.checkReader(ir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      SortedSetDocValues docValues = r.getSortedSetDocValues("dv");
      for (int i = 0; i < r.maxDoc(); i++) {
        String stringValues[] = r.document(i).getValues("stored");
        if (docValues.docID() < i) {
          docValues.nextDoc();
        }
        if (docValues != null && stringValues.length > 0) {
          assertEquals(i, docValues.docID());
          for (int j = 0; j < stringValues.length; j++) {
            assert docValues != null;
            long ord = docValues.nextOrd();
            assert ord != NO_MORE_ORDS;
            BytesRef scratch = docValues.lookupOrd(ord);
            assertEquals(stringValues[j], scratch.utf8ToString());
          }
          assertEquals(NO_MORE_ORDS, docValues.nextOrd());
        }
      }
    }
    if (VERBOSE) {
      System.out.println("TEST: close reader");
    }
    ir.close();
    if (VERBOSE) {
      System.out.println("TEST: close writer");
    }
    writer.close();
    if (VERBOSE) {
      System.out.println("TEST: close dir");
    }
    dir.close();
  }
  
  public void testSortedSetFixedLengthVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      int fixedLength = TestUtil.nextInt(random(), 1, 10);
      doTestSortedSetVsStoredFields(atLeast(300), fixedLength, fixedLength, 16, 100);
    }
  }
  
  public void testSortedNumericsSingleValuedVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedNumericsVsStoredFields(
          () -> 1,
          random()::nextLong
      );
    }
  }
  
  public void testSortedNumericsSingleValuedMissingVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedNumericsVsStoredFields(
          () -> random().nextBoolean() ? 0 : 1,
          random()::nextLong
      );
    }
  }

  public void testSortedNumericsMultipleValuesVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedNumericsVsStoredFields(
          () -> TestUtil.nextLong(random(), 0, 50),
          random()::nextLong
      );
    }
  }

  public void testSortedNumericsFewUniqueSetsVsStoredFields() throws Exception {
    final long[] values = new long[TestUtil.nextInt(random(), 2, 6)];
    for (int i = 0; i < values.length; ++i) {
      values[i] = random().nextLong();
    }
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedNumericsVsStoredFields(
          () -> TestUtil.nextLong(random(), 0, 6),
          () -> values[random().nextInt(values.length)]
      );
    }
  }

  public void testSortedSetVariableLengthVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedSetVsStoredFields(atLeast(300), 1, 10, 16, 100);
    }
  }

  public void testSortedSetFixedLengthSingleValuedVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      int fixedLength = TestUtil.nextInt(random(), 1, 10);
      doTestSortedSetVsStoredFields(atLeast(300), fixedLength, fixedLength, 1, 100);
    }
  }
  
  public void testSortedSetVariableLengthSingleValuedVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedSetVsStoredFields(atLeast(300), 1, 10, 1, 100);
    }
  }

  public void testSortedSetFixedLengthFewUniqueSetsVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedSetVsStoredFields(atLeast(300), 10, 10, 6, 6);
    }
  }

  public void testSortedSetVariableLengthFewUniqueSetsVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedSetVsStoredFields(atLeast(300), 1, 10, 6, 6);
    }
  }

  public void testSortedSetVariableLengthManyValuesPerDocVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedSetVsStoredFields(atLeast(20), 1, 10, 500, 1000);
    }
  }

  public void testSortedSetFixedLengthManyValuesPerDocVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedSetVsStoredFields(atLeast(20), 10, 10, 500, 1000);
    }
  }

  public void testGCDCompression() throws Exception {
    doTestGCDCompression(1);
  }

  public void testSparseGCDCompression() throws Exception {
    doTestGCDCompression(random().nextDouble());
  }

  private void doTestGCDCompression(double density) throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      final long min = - (((long) random().nextInt(1 << 30)) << 32);
      final long mul = random().nextInt() & 0xFFFFFFFFL;
      final LongSupplier longs = () -> {
        return min + mul * random().nextInt(1 << 20);
      };
      doTestNumericsVsStoredFields(density, longs);
    }
  }

  public void testZeros() throws Exception {
    doTestNumericsVsStoredFields(1, () -> 0);
  }

  public void testSparseZeros() throws Exception {
    doTestNumericsVsStoredFields(random().nextDouble(), () -> 0);
  }

  public void testZeroOrMin() throws Exception {
    // try to make GCD compression fail if the format did not anticipate that
    // the GCD of 0 and MIN_VALUE is negative
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      final LongSupplier longs = () -> {
        return random().nextBoolean() ? 0 : Long.MIN_VALUE;
      };
      doTestNumericsVsStoredFields(1, longs);
    }
  }
  
  public void testTwoNumbersOneMissing() throws IOException {
    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(null);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.YES));
    doc.add(new NumericDocValuesField("dv1", 0));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    iw.addDocument(doc);
    iw.forceMerge(1);
    iw.close();
    
    IndexReader ir = DirectoryReader.open(directory);
    assertEquals(1, ir.leaves().size());
    LeafReader ar = ir.leaves().get(0).reader();
    NumericDocValues dv = ar.getNumericDocValues("dv1");
    assertEquals(0, dv.nextDoc());
    assertEquals(0, dv.longValue());
    assertEquals(NO_MORE_DOCS, dv.nextDoc());
    ir.close();
    directory.close();
  }
  
  public void testTwoNumbersOneMissingWithMerging() throws IOException {
    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(null);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.YES));
    doc.add(new NumericDocValuesField("dv1", 0));
    iw.addDocument(doc);
    iw.commit();
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    iw.addDocument(doc);
    iw.forceMerge(1);
    iw.close();
    
    IndexReader ir = DirectoryReader.open(directory);
    assertEquals(1, ir.leaves().size());
    LeafReader ar = ir.leaves().get(0).reader();
    NumericDocValues dv = ar.getNumericDocValues("dv1");
    assertEquals(0, dv.nextDoc());
    assertEquals(0, dv.longValue());
    assertEquals(NO_MORE_DOCS, dv.nextDoc());
    ir.close();
    directory.close();
  }
  
  public void testThreeNumbersOneMissingWithMerging() throws IOException {
    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(null);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.YES));
    doc.add(new NumericDocValuesField("dv1", 0));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    iw.addDocument(doc);
    iw.commit();
    doc = new Document();
    doc.add(new StringField("id", "2", Field.Store.YES));
    doc.add(new NumericDocValuesField("dv1", 5));
    iw.addDocument(doc);
    iw.forceMerge(1);
    iw.close();
    
    IndexReader ir = DirectoryReader.open(directory);
    assertEquals(1, ir.leaves().size());
    LeafReader ar = ir.leaves().get(0).reader();
    NumericDocValues dv = ar.getNumericDocValues("dv1");
    assertEquals(0, dv.nextDoc());
    assertEquals(0, dv.longValue());
    assertEquals(2, dv.nextDoc());
    assertEquals(5, dv.longValue());
    ir.close();
    directory.close();
  }
  
  public void testTwoBytesOneMissing() throws IOException {
    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(null);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.YES));
    doc.add(new BinaryDocValuesField("dv1", newBytesRef()));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    iw.addDocument(doc);
    iw.forceMerge(1);
    iw.close();
    
    IndexReader ir = DirectoryReader.open(directory);
    assertEquals(1, ir.leaves().size());
    LeafReader ar = ir.leaves().get(0).reader();
    BinaryDocValues dv = ar.getBinaryDocValues("dv1");
    assertEquals(0, dv.nextDoc());
    assertEquals(newBytesRef(), dv.binaryValue());
    assertEquals(NO_MORE_DOCS, dv.nextDoc());
    ir.close();
    directory.close();
  }
  
  public void testTwoBytesOneMissingWithMerging() throws IOException {
    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(null);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.YES));
    doc.add(new BinaryDocValuesField("dv1", newBytesRef()));
    iw.addDocument(doc);
    iw.commit();
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    iw.addDocument(doc);
    iw.forceMerge(1);
    iw.close();
    
    IndexReader ir = DirectoryReader.open(directory);
    assertEquals(1, ir.leaves().size());
    LeafReader ar = ir.leaves().get(0).reader();
    BinaryDocValues dv = ar.getBinaryDocValues("dv1");
    assertEquals(0, dv.nextDoc());
    assertEquals(newBytesRef(), dv.binaryValue());
    assertEquals(NO_MORE_DOCS, dv.nextDoc());
    ir.close();
    directory.close();
  }
  
  public void testThreeBytesOneMissingWithMerging() throws IOException {
    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(null);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.YES));
    doc.add(new BinaryDocValuesField("dv1", newBytesRef()));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    iw.addDocument(doc);
    iw.commit();
    doc = new Document();
    doc.add(new StringField("id", "2", Field.Store.YES));
    doc.add(new BinaryDocValuesField("dv1", newBytesRef("boo")));
    iw.addDocument(doc);
    iw.forceMerge(1);
    iw.close();
    
    IndexReader ir = DirectoryReader.open(directory);
    assertEquals(1, ir.leaves().size());
    LeafReader ar = ir.leaves().get(0).reader();
    BinaryDocValues dv = ar.getBinaryDocValues("dv1");
    assertEquals(0, dv.nextDoc());
    assertEquals(newBytesRef(), dv.binaryValue());
    assertEquals(2, dv.nextDoc());
    assertEquals(newBytesRef("boo"), dv.binaryValue());
    assertEquals(NO_MORE_DOCS, dv.nextDoc());
    ir.close();
    directory.close();
  }
  
  /** Tests dv against stored fields with threads (binary/numeric/sorted, no missing) */
  public void testThreads() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    Document doc = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    Field storedBinField = new StoredField("storedBin", new byte[0]);
    Field dvBinField = new BinaryDocValuesField("dvBin", newBytesRef());
    Field dvSortedField = new SortedDocValuesField("dvSorted", newBytesRef());
    Field storedNumericField = new StoredField("storedNum", "");
    Field dvNumericField = new NumericDocValuesField("dvNum", 0);
    doc.add(idField);
    doc.add(storedBinField);
    doc.add(dvBinField);
    doc.add(dvSortedField);
    doc.add(storedNumericField);
    doc.add(dvNumericField);
    
    // index some docs
    int numDocs = atLeast(300);
    for (int i = 0; i < numDocs; i++) {
      idField.setStringValue(Integer.toString(i));
      int length = TestUtil.nextInt(random(), 0, 8);
      byte buffer[] = new byte[length];
      random().nextBytes(buffer);
      storedBinField.setBytesValue(buffer);
      dvBinField.setBytesValue(buffer);
      dvSortedField.setBytesValue(buffer);
      long numericValue = random().nextLong();
      storedNumericField.setStringValue(Long.toString(numericValue));
      dvNumericField.setLongValue(numericValue);
      writer.addDocument(doc);
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }
    
    // delete some docs
    int numDeletions = random().nextInt(numDocs/10);
    for (int i = 0; i < numDeletions; i++) {
      int id = random().nextInt(numDocs);
      writer.deleteDocuments(new Term("id", Integer.toString(id)));
    }
    writer.close();
    
    // compare
    final DirectoryReader ir = DirectoryReader.open(dir);
    int numThreads = TestUtil.nextInt(random(), 2, 7);
    Thread threads[] = new Thread[numThreads];
    final CountDownLatch startingGun = new CountDownLatch(1);
    
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            startingGun.await();
            for (LeafReaderContext context : ir.leaves()) {
              LeafReader r = context.reader();
              BinaryDocValues binaries = r.getBinaryDocValues("dvBin");
              SortedDocValues sorted = r.getSortedDocValues("dvSorted");
              NumericDocValues numerics = r.getNumericDocValues("dvNum");
              for (int j = 0; j < r.maxDoc(); j++) {
                BytesRef binaryValue = r.document(j).getBinaryValue("storedBin");
                assertEquals(j, binaries.nextDoc());
                BytesRef scratch = binaries.binaryValue();
                assertEquals(binaryValue, scratch);
                assertEquals(j, sorted.nextDoc());
                scratch = sorted.binaryValue();
                assertEquals(binaryValue, scratch);
                String expected = r.document(j).get("storedNum");
                assertEquals(j, numerics.nextDoc());
                assertEquals(Long.parseLong(expected), numerics.longValue());
              }
            }
            TestUtil.checkReader(ir);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
      threads[i].start();
    }
    startingGun.countDown();
    for (Thread t : threads) {
      t.join();
    }
    ir.close();
    dir.close();
  }
  
  /** Tests dv against stored fields with threads (all types + missing) */
  @Nightly
  public void testThreads2() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    Field idField = new StringField("id", "", Field.Store.NO);
    Field storedBinField = new StoredField("storedBin", new byte[0]);
    Field dvBinField = new BinaryDocValuesField("dvBin", newBytesRef());
    Field dvSortedField = new SortedDocValuesField("dvSorted", newBytesRef());
    Field storedNumericField = new StoredField("storedNum", "");
    Field dvNumericField = new NumericDocValuesField("dvNum", 0);
    
    // index some docs
    int numDocs = TestUtil.nextInt(random(), 1025, 2047);
    for (int i = 0; i < numDocs; i++) {
      idField.setStringValue(Integer.toString(i));
      int length = TestUtil.nextInt(random(), 0, 8);
      byte buffer[] = new byte[length];
      random().nextBytes(buffer);
      storedBinField.setBytesValue(buffer);
      dvBinField.setBytesValue(buffer);
      dvSortedField.setBytesValue(buffer);
      long numericValue = random().nextLong();
      storedNumericField.setStringValue(Long.toString(numericValue));
      dvNumericField.setLongValue(numericValue);
      Document doc = new Document();
      doc.add(idField);
      if (random().nextInt(4) > 0) {
        doc.add(storedBinField);
        doc.add(dvBinField);
        doc.add(dvSortedField);
      }
      if (random().nextInt(4) > 0) {
        doc.add(storedNumericField);
        doc.add(dvNumericField);
      }
      int numSortedSetFields = random().nextInt(3);
      Set<String> values = new TreeSet<>();
      for (int j = 0; j < numSortedSetFields; j++) {
        values.add(TestUtil.randomSimpleString(random()));
      }
      for (String v : values) {
        doc.add(new SortedSetDocValuesField("dvSortedSet", newBytesRef(v)));
        doc.add(new StoredField("storedSortedSet", v));
      }
      int numSortedNumericFields = random().nextInt(3);
      Set<Long> numValues = new TreeSet<>();
      for (int j = 0; j < numSortedNumericFields; j++) {
        numValues.add(TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE));
      }
      for (Long l : numValues) {
        doc.add(new SortedNumericDocValuesField("dvSortedNumeric", l));
        doc.add(new StoredField("storedSortedNumeric", Long.toString(l)));
      }
      writer.addDocument(doc);
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }
    
    // delete some docs
    int numDeletions = random().nextInt(numDocs/10);
    for (int i = 0; i < numDeletions; i++) {
      int id = random().nextInt(numDocs);
      writer.deleteDocuments(new Term("id", Integer.toString(id)));
    }
    writer.close();
    
    // compare
    final DirectoryReader ir = DirectoryReader.open(dir);
    int numThreads = TestUtil.nextInt(random(), 2, 7);
    Thread threads[] = new Thread[numThreads];
    final CountDownLatch startingGun = new CountDownLatch(1);
    
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            startingGun.await();
            for (LeafReaderContext context : ir.leaves()) {
              LeafReader r = context.reader();
              BinaryDocValues binaries = r.getBinaryDocValues("dvBin");
              SortedDocValues sorted = r.getSortedDocValues("dvSorted");
              NumericDocValues numerics = r.getNumericDocValues("dvNum");
              SortedSetDocValues sortedSet = r.getSortedSetDocValues("dvSortedSet");
              SortedNumericDocValues sortedNumeric = r.getSortedNumericDocValues("dvSortedNumeric");
              for (int j = 0; j < r.maxDoc(); j++) {
                BytesRef binaryValue = r.document(j).getBinaryValue("storedBin");
                if (binaryValue != null) {
                  if (binaries != null) {
                    assertEquals(j, binaries.nextDoc());
                    BytesRef scratch = binaries.binaryValue();
                    assertEquals(binaryValue, scratch);
                    assertEquals(j, sorted.nextDoc());
                    scratch = sorted.binaryValue();
                    assertEquals(binaryValue, scratch);
                  }
                }
               
                String number = r.document(j).get("storedNum");
                if (number != null) {
                  if (numerics != null) {
                    assertEquals(j, numerics.advance(j));
                    assertEquals(Long.parseLong(number), numerics.longValue());
                  }
                }
                
                String values[] = r.document(j).getValues("storedSortedSet");
                if (values.length > 0) {
                  assertNotNull(sortedSet);
                  assertEquals(j, sortedSet.nextDoc());
                  for (int k = 0; k < values.length; k++) {
                    long ord = sortedSet.nextOrd();
                    assertTrue(ord != SortedSetDocValues.NO_MORE_ORDS);
                    BytesRef value = sortedSet.lookupOrd(ord);
                    assertEquals(values[k], value.utf8ToString());
                  }
                  assertEquals(SortedSetDocValues.NO_MORE_ORDS, sortedSet.nextOrd());
                }
                
                String numValues[] = r.document(j).getValues("storedSortedNumeric");
                if (numValues.length > 0) {
                  assertNotNull(sortedNumeric);
                  assertEquals(j, sortedNumeric.nextDoc());
                  assertEquals(numValues.length, sortedNumeric.docValueCount());
                  for (int k = 0; k < numValues.length; k++) {
                    long v = sortedNumeric.nextValue();
                    assertEquals(numValues[k], Long.toString(v));
                  }
                }
              }
            }
            TestUtil.checkReader(ir);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
      threads[i].start();
    }
    startingGun.countDown();
    for (Thread t : threads) {
      t.join();
    }
    ir.close();
    dir.close();
  }
  
  @Nightly
  public void testThreads3() throws Exception {
    Directory dir = newFSDirectory(createTempDir());
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    
    int numSortedSets = random().nextInt(21);
    int numBinaries = random().nextInt(21);
    int numSortedNums = random().nextInt(21);
    
    int numDocs = TestUtil.nextInt(random(), 2025, 2047);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      
      for (int j = 0; j < numSortedSets; j++) {
        doc.add(
            new SortedSetDocValuesField(
                "ss" + j, newBytesRef(TestUtil.randomSimpleString(random()))));
        doc.add(
            new SortedSetDocValuesField(
                "ss" + j, newBytesRef(TestUtil.randomSimpleString(random()))));
      }
      
      for (int j = 0; j < numBinaries; j++) {
        doc.add(
            new BinaryDocValuesField("b" + j, newBytesRef(TestUtil.randomSimpleString(random()))));
      }
      
      for (int j = 0; j < numSortedNums; j++) {
        doc.add(new SortedNumericDocValuesField("sn" + j, TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE)));
        doc.add(new SortedNumericDocValuesField("sn" + j, TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE)));
      }
      writer.addDocument(doc);
    }
    writer.close();
    
    // now check with threads
    for (int i = 0; i < 10; i++) {
      final DirectoryReader r = DirectoryReader.open(dir);
      final CountDownLatch startingGun = new CountDownLatch(1);
      Thread threads[] = new Thread[TestUtil.nextInt(random(), 4, 10)];
      for (int tid = 0; tid < threads.length; tid++) {
        threads[tid] = new Thread() {
          @Override
          public void run() {
            try {
              ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
              PrintStream infoStream = new PrintStream(bos, false, IOUtils.UTF_8);
              startingGun.await();
              for (LeafReaderContext leaf : r.leaves()) {
                DocValuesStatus status = CheckIndex.testDocValues((SegmentReader)leaf.reader(), infoStream, true);
                if (status.error != null) {
                  throw status.error;
                }
              }
            } catch (Throwable e) {
              throw new RuntimeException(e);
            }
          }
        };
      }
      for (int tid = 0; tid < threads.length; tid++) {
        threads[tid].start();
      }
      startingGun.countDown();
      for (int tid = 0; tid < threads.length; tid++) {
        threads[tid].join();
      }
      r.close();
    }

    dir.close();
  }

  // LUCENE-5218
  public void testEmptyBinaryValueOnPageSizes() throws Exception {
    // Test larger and larger power-of-two sized values,
    // followed by empty string value:
    for(int i=0;i<20;i++) {
      if (i > 14 && codecAcceptsHugeBinaryValues("field") == false) {
        break;
      }
      Directory dir = newDirectory();
      RandomIndexWriter w = new RandomIndexWriter(random(), dir);
      BytesRef bytes = newBytesRef(new byte[1 << i], 0, 1 << i);
      for (int j = 0; j < 4; j++) {
        Document doc = new Document();
        doc.add(new BinaryDocValuesField("field", bytes));
        w.addDocument(doc);
      }
      Document doc = new Document();
      doc.add(new StoredField("id", "5"));
      doc.add(new BinaryDocValuesField("field", newBytesRef()));
      w.addDocument(doc);
      IndexReader r = w.getReader();
      w.close();

      BinaryDocValues values = MultiDocValues.getBinaryValues(r, "field");
      for(int j=0;j<5;j++) {
        assertEquals(j, values.nextDoc());
        BytesRef result = values.binaryValue();
        assertTrue(result.length == 0 || result.length == 1<<i);
      }
      r.close();
      dir.close();
    }
  }
  
  public void testOneSortedNumber() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("dv", 5));
    writer.addDocument(doc);
    writer.close();
    
    // Now search the index:
    IndexReader reader = DirectoryReader.open(directory);
    assert reader.leaves().size() == 1;
    SortedNumericDocValues dv = reader.leaves().get(0).reader().getSortedNumericDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals(1, dv.docValueCount());
    assertEquals(5, dv.nextValue());

    reader.close();
    directory.close();
  }
  
  public void testOneSortedNumberOneMissing() throws IOException {
    Directory directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("dv", 5));
    writer.addDocument(doc);
    writer.addDocument(new Document());
    writer.close();
    
    // Now search the index:
    IndexReader reader = DirectoryReader.open(directory);
    assert reader.leaves().size() == 1;
    SortedNumericDocValues dv = reader.leaves().get(0).reader().getSortedNumericDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals(1, dv.docValueCount());
    assertEquals(5, dv.nextValue());
    assertEquals(NO_MORE_DOCS, dv.nextDoc());
    
    reader.close();
    directory.close();
  }
  
  public void testNumberMergeAwayAllValues() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    iwriter.addDocument(doc);    
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new NumericDocValuesField("field", 5));
    iwriter.addDocument(doc);
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();
    
    NumericDocValues dv = getOnlyLeafReader(ireader).getNumericDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());
    
    ireader.close();
    directory.close();
  }
  
  public void testTwoSortedNumber() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("dv", 11));
    doc.add(new SortedNumericDocValuesField("dv", -5));
    writer.addDocument(doc);
    writer.close();
    
    // Now search the index:
    IndexReader reader = DirectoryReader.open(directory);
    assert reader.leaves().size() == 1;
    SortedNumericDocValues dv = reader.leaves().get(0).reader().getSortedNumericDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals(2, dv.docValueCount());
    assertEquals(-5, dv.nextValue());
    assertEquals(11, dv.nextValue());

    reader.close();
    directory.close();
  }
  
  public void testTwoSortedNumberSameValue() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("dv", 11));
    doc.add(new SortedNumericDocValuesField("dv", 11));
    writer.addDocument(doc);
    writer.close();
    
    // Now search the index:
    IndexReader reader = DirectoryReader.open(directory);
    assert reader.leaves().size() == 1;
    SortedNumericDocValues dv = reader.leaves().get(0).reader().getSortedNumericDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals(2, dv.docValueCount());
    assertEquals(11, dv.nextValue());
    assertEquals(11, dv.nextValue());

    reader.close();
    directory.close();
  }
  
  public void testTwoSortedNumberOneMissing() throws IOException {
    Directory directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("dv", 11));
    doc.add(new SortedNumericDocValuesField("dv", -5));
    writer.addDocument(doc);
    writer.addDocument(new Document());
    writer.close();
    
    // Now search the index:
    IndexReader reader = DirectoryReader.open(directory);
    assert reader.leaves().size() == 1;
    SortedNumericDocValues dv = reader.leaves().get(0).reader().getSortedNumericDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals(2, dv.docValueCount());
    assertEquals(-5, dv.nextValue());
    assertEquals(11, dv.nextValue());
    assertEquals(NO_MORE_DOCS, dv.nextDoc());
    
    reader.close();
    directory.close();
  }
  
  public void testSortedNumberMerge() throws IOException {
    Directory directory = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter writer = new IndexWriter(directory, iwc);
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("dv", 11));
    writer.addDocument(doc);
    writer.commit();
    doc = new Document();
    doc.add(new SortedNumericDocValuesField("dv", -5));
    writer.addDocument(doc);
    writer.forceMerge(1);
    writer.close();
    
    // Now search the index:
    IndexReader reader = DirectoryReader.open(directory);
    assert reader.leaves().size() == 1;
    SortedNumericDocValues dv = reader.leaves().get(0).reader().getSortedNumericDocValues("dv");
    assertEquals(0, dv.nextDoc());
    assertEquals(1, dv.docValueCount());
    assertEquals(11, dv.nextValue());
    assertEquals(1, dv.nextDoc());
    assertEquals(1, dv.docValueCount());
    assertEquals(-5, dv.nextValue());

    reader.close();
    directory.close();
  }
  
  public void testSortedNumberMergeAwayAllValues() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    iwriter.addDocument(doc);    
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new SortedNumericDocValuesField("field", 5));
    iwriter.addDocument(doc);
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();
    
    SortedNumericDocValues dv = getOnlyLeafReader(ireader).getSortedNumericDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());
    
    ireader.close();
    directory.close();
  }

  public void testSortedEnumAdvanceIndependently() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    SortedDocValuesField field = new SortedDocValuesField("field", newBytesRef("2"));
    doc.add(field);
    iwriter.addDocument(doc);
    field.setBytesValue(newBytesRef("1"));
    iwriter.addDocument(doc);
    field.setBytesValue(newBytesRef("3"));
    iwriter.addDocument(doc);

    iwriter.commit();
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedDocValues dv = getOnlyLeafReader(ireader).getSortedDocValues("field");
    doTestSortedSetEnumAdvanceIndependently(DocValues.singleton(dv));

    ireader.close();
    directory.close();
  }

  public void testSortedSetEnumAdvanceIndependently() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    SortedSetDocValuesField field1 = new SortedSetDocValuesField("field", newBytesRef("2"));
    SortedSetDocValuesField field2 = new SortedSetDocValuesField("field", newBytesRef("3"));
    doc.add(field1);
    doc.add(field2);
    iwriter.addDocument(doc);
    field1.setBytesValue(newBytesRef("1"));
    iwriter.addDocument(doc);
    field2.setBytesValue(newBytesRef("2"));
    iwriter.addDocument(doc);

    iwriter.commit();
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    doTestSortedSetEnumAdvanceIndependently(dv);

    ireader.close();
    directory.close();
  }

  protected void doTestSortedSetEnumAdvanceIndependently(SortedSetDocValues dv) throws IOException {
    if (dv.getValueCount() < 2) {
      return;
    }
    List<BytesRef> terms = new ArrayList<>();
    TermsEnum te = dv.termsEnum();
    terms.add(BytesRef.deepCopyOf(te.next()));
    terms.add(BytesRef.deepCopyOf(te.next()));

    // Make sure that calls to next() does not modify the term of the other enum
    TermsEnum enum1 = dv.termsEnum();
    TermsEnum enum2 = dv.termsEnum();
    BytesRefBuilder term1 = new BytesRefBuilder();
    BytesRefBuilder term2 = new BytesRefBuilder();

    term1.copyBytes(enum1.next());
    term2.copyBytes(enum2.next());
    term1.copyBytes(enum1.next());

    assertEquals(term1.get(), enum1.term());
    assertEquals(term2.get(), enum2.term());

    // Same for seekCeil
    enum1 = dv.termsEnum();
    enum2 = dv.termsEnum();
    term1 = new BytesRefBuilder();
    term2 = new BytesRefBuilder();

    term2.copyBytes(enum2.next());
    BytesRefBuilder seekTerm = new BytesRefBuilder();
    seekTerm.append(terms.get(0));
    seekTerm.append((byte) 0);
    enum1.seekCeil(seekTerm.get());
    term1.copyBytes(enum1.term());

    assertEquals(term1.get(), enum1.term());
    assertEquals(term2.get(), enum2.term());

    // Same for seekCeil on an exact value
    enum1 = dv.termsEnum();
    enum2 = dv.termsEnum();
    term1 = new BytesRefBuilder();
    term2 = new BytesRefBuilder();

    term2.copyBytes(enum2.next());
    enum1.seekCeil(terms.get(1));
    term1.copyBytes(enum1.term());
    
    assertEquals(term1.get(), enum1.term());
    assertEquals(term2.get(), enum2.term());

    // Same for seekExact
    enum1 = dv.termsEnum();
    enum2 = dv.termsEnum();
    term1 = new BytesRefBuilder();
    term2 = new BytesRefBuilder();

    term2.copyBytes(enum2.next());
    final boolean found = enum1.seekExact(terms.get(1));
    assertTrue(found);
    term1.copyBytes(enum1.term());

    // Same for seek by ord
    enum1 = dv.termsEnum();
    enum2 = dv.termsEnum();
    term1 = new BytesRefBuilder();
    term2 = new BytesRefBuilder();

    term2.copyBytes(enum2.next());
    enum1.seekExact(1);
    term1.copyBytes(enum1.term());

    assertEquals(term1.get(), enum1.term());
    assertEquals(term2.get(), enum2.term());
  }

  // same as testSortedMergeAwayAllValues but on more than 1024 docs to have sparse encoding on
  public void testSortedMergeAwayAllValuesLargeSegment() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new SortedDocValuesField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    final int numEmptyDocs = atLeast(1024);
    for (int i = 0; i < numEmptyDocs; ++i) {
      iwriter.addDocument(new Document());
    }
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedDocValues dv = getOnlyLeafReader(ireader).getSortedDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());

    ireader.close();
    directory.close();
  }

  // same as testSortedSetMergeAwayAllValues but on more than 1024 docs to have sparse encoding on
  public void testSortedSetMergeAwayAllValuesLargeSegment() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new SortedSetDocValuesField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    final int numEmptyDocs = atLeast(1024);
    for (int i = 0; i < numEmptyDocs; ++i) {
      iwriter.addDocument(new Document());
    }
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());

    ireader.close();
    directory.close();
  }

  // same as testNumericMergeAwayAllValues but on more than 1024 docs to have sparse encoding on
  public void testNumericMergeAwayAllValuesLargeSegment() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new NumericDocValuesField("field", 42L));
    iwriter.addDocument(doc);
    final int numEmptyDocs = atLeast(1024);
    for (int i = 0; i < numEmptyDocs; ++i) {
      iwriter.addDocument(new Document());
    }
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    NumericDocValues dv = getOnlyLeafReader(ireader).getNumericDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());

    ireader.close();
    directory.close();
  }

  // same as testSortedNumericMergeAwayAllValues but on more than 1024 docs to have sparse encoding on
  public void testSortedNumericMergeAwayAllValuesLargeSegment() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new SortedNumericDocValuesField("field", 42L));
    iwriter.addDocument(doc);
    final int numEmptyDocs = atLeast(1024);
    for (int i = 0; i < numEmptyDocs; ++i) {
      iwriter.addDocument(new Document());
    }
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedNumericDocValues dv = getOnlyLeafReader(ireader).getSortedNumericDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());

    ireader.close();
    directory.close();
  }

  // same as testBinaryMergeAwayAllValues but on more than 1024 docs to have sparse encoding on
  public void testBinaryMergeAwayAllValuesLargeSegment() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new BinaryDocValuesField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    final int numEmptyDocs = atLeast(1024);
    for (int i = 0; i < numEmptyDocs; ++i) {
      iwriter.addDocument(new Document());
    }
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    BinaryDocValues dv = getOnlyLeafReader(ireader).getBinaryDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());

    ireader.close();
    directory.close();
  }

  public void testRandomAdvanceNumeric() throws IOException {
    final long longRange;
    if (random().nextBoolean()) {
      longRange = TestUtil.nextInt(random(), 1, 1024);
    } else {
      longRange = TestUtil.nextLong(random(), 1, Long.MAX_VALUE);
    }
    doTestRandomAdvance(new FieldCreator() {
        @Override
        public Field next() {
          return new NumericDocValuesField("field", TestUtil.nextLong(random(), 0, longRange));
        }

        @Override
        public DocIdSetIterator iterator(IndexReader r) throws IOException {
          return MultiDocValues.getNumericValues(r, "field");
        }
      });
  }

  public void testRandomAdvanceBinary() throws IOException {
    doTestRandomAdvance(
        new FieldCreator() {
          @Override
          public Field next() {
            byte[] bytes = new byte[random().nextInt(10)];
            random().nextBytes(bytes);
            return new BinaryDocValuesField("field", newBytesRef(bytes));
          }

          @Override
          public DocIdSetIterator iterator(IndexReader r) throws IOException {
            return MultiDocValues.getBinaryValues(r, "field");
          }
      });
  }

  private interface FieldCreator {
    public Field next();
    public DocIdSetIterator iterator(IndexReader r) throws IOException;
  }

  private void doTestRandomAdvance(FieldCreator fieldCreator) throws IOException {

    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter w = new RandomIndexWriter(random(), directory, conf);
    int numChunks = atLeast(10);
    int id = 0;
    Set<Integer> missingSet = new HashSet<>();
    for(int i=0;i<numChunks;i++) {
      // change sparseness for each chunk
      double sparseChance = random().nextDouble();
      int docCount = atLeast(1000);
      for(int j=0;j<docCount;j++) {
        Document doc = new Document();
        doc.add(new StoredField("id", id));
        if (random().nextDouble() > sparseChance) {
          doc.add(fieldCreator.next());
        } else {
          missingSet.add(id);
        }
        id++;
        w.addDocument(doc);
      }
    }

    if (random().nextBoolean()) {
      w.forceMerge(1);
    }

    // Now search the index:
    IndexReader r = w.getReader();
    BitSet missing = new FixedBitSet(r.maxDoc());
    for(int docID=0;docID<r.maxDoc();docID++) {
      Document doc = r.document(docID);
      if (missingSet.contains(doc.getField("id").numericValue())) {
        missing.set(docID);
      }
    }
    
    int numIters = atLeast(10);
    for(int iter=0;iter<numIters;iter++) {
      DocIdSetIterator values = fieldCreator.iterator(r);
      assertEquals(-1, values.docID());

      while (true) {
        int docID;
        if (random().nextBoolean()) {
          docID = values.nextDoc();
        } else {
          int range;
          if (random().nextInt(10) == 7) {
            // big jump
            range = r.maxDoc()-values.docID();
          } else {
            // small jump
            range = 25;
          }
          int inc = TestUtil.nextInt(random(), 1, range);
          docID = values.advance(values.docID() + inc);
        }
        if (docID == NO_MORE_DOCS) {
          break;
        }
        assertFalse(missing.get(docID));
      }
    }

    IOUtils.close(r, w, directory);
  }
                                               
  protected boolean codecAcceptsHugeBinaryValues(String field) {
    return true;
  }
}
