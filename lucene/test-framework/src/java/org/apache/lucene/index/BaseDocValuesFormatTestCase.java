package org.apache.lucene.index;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Abstract class to do basic tests for a docvalues format.
 * NOTE: This test focuses on the docvalues impl, nothing else.
 * The [stretch] goal is for this test to be
 * so thorough in testing a new DocValuesFormat that if this
 * test passes, then all Lucene/Solr tests should also pass.  Ie,
 * if there is some bug in a given DocValuesFormat that this
 * test fails to catch then this test needs to be improved! */
public abstract class BaseDocValuesFormatTestCase extends LuceneTestCase {
  
  /** Returns the codec to run tests against */
  protected abstract Codec getCodec();
  
  private Codec savedCodec;
  
  public void setUp() throws Exception {
    super.setUp();
    // set the default codec, so adding test cases to this isn't fragile
    savedCodec = Codec.getDefault();
    Codec.setDefault(getCodec());
  }
  
  public void tearDown() throws Exception {
    Codec.setDefault(savedCodec); // restore
    super.tearDown();
  }

  public void testOneNumber() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    IndexWriter iwriter = new IndexWriter(directory, conf);
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

    assertEquals(1, isearcher.search(new TermQuery(new Term("fieldname", longTerm)), 1).totalHits);
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, null, 1);
    assertEquals(1, hits.totalHits);
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      StoredDocument hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv");
      assertEquals(5, dv.get(hits.scoreDocs[i].doc));
    }

    ireader.close();
    directory.close();
  }

  public void testOneFloat() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    IndexWriter iwriter = new IndexWriter(directory, conf);
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

    assertEquals(1, isearcher.search(new TermQuery(new Term("fieldname", longTerm)), 1).totalHits);
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, null, 1);
    assertEquals(1, hits.totalHits);
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      StoredDocument hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv");
      assertEquals(Float.floatToRawIntBits(5.7f), dv.get(hits.scoreDocs[i].doc));
    }

    ireader.close();
    directory.close();
  }
  
  public void testTwoNumbers() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    IndexWriter iwriter = new IndexWriter(directory, conf);
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

    assertEquals(1, isearcher.search(new TermQuery(new Term("fieldname", longTerm)), 1).totalHits);
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, null, 1);
    assertEquals(1, hits.totalHits);
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      StoredDocument hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv1");
      assertEquals(5, dv.get(hits.scoreDocs[i].doc));
      dv = ireader.leaves().get(0).reader().getNumericDocValues("dv2");
      assertEquals(17, dv.get(hits.scoreDocs[i].doc));
    }

    ireader.close();
    directory.close();
  }

  public void testTwoFieldsMixed() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new NumericDocValuesField("dv1", 5));
    doc.add(new BinaryDocValuesField("dv2", new BytesRef("hello world")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.search(new TermQuery(new Term("fieldname", longTerm)), 1).totalHits);
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, null, 1);
    assertEquals(1, hits.totalHits);
    BytesRef scratch = new BytesRef();
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      StoredDocument hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv1");
      assertEquals(5, dv.get(hits.scoreDocs[i].doc));
      BinaryDocValues dv2 = ireader.leaves().get(0).reader().getBinaryDocValues("dv2");
      dv2.get(hits.scoreDocs[i].doc, scratch);
      assertEquals(new BytesRef("hello world"), scratch);
    }

    ireader.close();
    directory.close();
  }
  
  public void testThreeFieldsMixed() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new SortedDocValuesField("dv1", new BytesRef("hello hello")));
    doc.add(new NumericDocValuesField("dv2", 5));
    doc.add(new BinaryDocValuesField("dv3", new BytesRef("hello world")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.search(new TermQuery(new Term("fieldname", longTerm)), 1).totalHits);
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, null, 1);
    assertEquals(1, hits.totalHits);
    BytesRef scratch = new BytesRef();
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      StoredDocument hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv1");
      int ord = dv.getOrd(0);
      dv.lookupOrd(ord, scratch);
      assertEquals(new BytesRef("hello hello"), scratch);
      NumericDocValues dv2 = ireader.leaves().get(0).reader().getNumericDocValues("dv2");
      assertEquals(5, dv2.get(hits.scoreDocs[i].doc));
      BinaryDocValues dv3 = ireader.leaves().get(0).reader().getBinaryDocValues("dv3");
      dv3.get(hits.scoreDocs[i].doc, scratch);
      assertEquals(new BytesRef("hello world"), scratch);
    }

    ireader.close();
    directory.close();
  }
  
  public void testThreeFieldsMixed2() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new BinaryDocValuesField("dv1", new BytesRef("hello world")));
    doc.add(new SortedDocValuesField("dv2", new BytesRef("hello hello")));
    doc.add(new NumericDocValuesField("dv3", 5));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.search(new TermQuery(new Term("fieldname", longTerm)), 1).totalHits);
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, null, 1);
    assertEquals(1, hits.totalHits);
    BytesRef scratch = new BytesRef();
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      StoredDocument hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv2");
      int ord = dv.getOrd(0);
      dv.lookupOrd(ord, scratch);
      assertEquals(new BytesRef("hello hello"), scratch);
      NumericDocValues dv2 = ireader.leaves().get(0).reader().getNumericDocValues("dv3");
      assertEquals(5, dv2.get(hits.scoreDocs[i].doc));
      BinaryDocValues dv3 = ireader.leaves().get(0).reader().getBinaryDocValues("dv1");
      dv3.get(hits.scoreDocs[i].doc, scratch);
      assertEquals(new BytesRef("hello world"), scratch);
    }

    ireader.close();
    directory.close();
  }
  
  public void testTwoDocumentsNumeric() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
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
    assertEquals(1, dv.get(0));
    assertEquals(2, dv.get(1));

    ireader.close();
    directory.close();
  }
  
  public void testTwoDocumentsMerged() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
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
      StoredDocument doc2 = ireader.leaves().get(0).reader().document(i);
      long expected;
      if (doc2.get("id").equals("0")) {
        expected = -10;
      } else {
        expected = 99;
      }
      assertEquals(expected, dv.get(i));
    }

    ireader.close();
    directory.close();
  }

  public void testBigNumericRange() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
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
    assertEquals(Long.MIN_VALUE, dv.get(0));
    assertEquals(Long.MAX_VALUE, dv.get(1));

    ireader.close();
    directory.close();
  }
  
  public void testBigNumericRange2() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
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
    assertEquals(-8841491950446638677L, dv.get(0));
    assertEquals(9062230939892376225L, dv.get(1));

    ireader.close();
    directory.close();
  }
  
  public void testBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new BinaryDocValuesField("dv", new BytesRef("hello world")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.search(new TermQuery(new Term("fieldname", longTerm)), 1).totalHits);
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, null, 1);
    assertEquals(1, hits.totalHits);
    BytesRef scratch = new BytesRef();
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      StoredDocument hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv");
      dv.get(hits.scoreDocs[i].doc, scratch);
      assertEquals(new BytesRef("hello world"), scratch);
    }

    ireader.close();
    directory.close();
  }
  
  public void testBytesTwoDocumentsMerged() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(newField("id", "0", StringField.TYPE_STORED));
    doc.add(new BinaryDocValuesField("dv", new BytesRef("hello world 1")));
    iwriter.addDocument(doc);
    iwriter.commit();
    doc = new Document();
    doc.add(newField("id", "1", StringField.TYPE_STORED));
    doc.add(new BinaryDocValuesField("dv", new BytesRef("hello 2")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv");
    BytesRef scratch = new BytesRef();
    for(int i=0;i<2;i++) {
      StoredDocument doc2 = ireader.leaves().get(0).reader().document(i);
      String expected;
      if (doc2.get("id").equals("0")) {
        expected = "hello world 1";
      } else {
        expected = "hello 2";
      }
      dv.get(i, scratch);
      assertEquals(expected, scratch.utf8ToString());
    }

    ireader.close();
    directory.close();
  }

  public void testSortedBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    doc.add(new SortedDocValuesField("dv", new BytesRef("hello world")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = new IndexSearcher(ireader);

    assertEquals(1, isearcher.search(new TermQuery(new Term("fieldname", longTerm)), 1).totalHits);
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, null, 1);
    assertEquals(1, hits.totalHits);
    BytesRef scratch = new BytesRef();
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      StoredDocument hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
      dv.lookupOrd(dv.getOrd(hits.scoreDocs[i].doc), scratch);
      assertEquals(new BytesRef("hello world"), scratch);
    }

    ireader.close();
    directory.close();
  }

  public void testSortedBytesTwoDocuments() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("hello world 1")));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("hello world 2")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
    BytesRef scratch = new BytesRef();
    dv.lookupOrd(dv.getOrd(0), scratch);
    assertEquals("hello world 1", scratch.utf8ToString());
    dv.lookupOrd(dv.getOrd(1), scratch);
    assertEquals("hello world 2", scratch.utf8ToString());

    ireader.close();
    directory.close();
  }
  
  public void testSortedBytesThreeDocuments() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("hello world 1")));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("hello world 2")));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("hello world 1")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
    assertEquals(2, dv.getValueCount());
    BytesRef scratch = new BytesRef();
    assertEquals(0, dv.getOrd(0));
    dv.lookupOrd(0, scratch);
    assertEquals("hello world 1", scratch.utf8ToString());
    assertEquals(1, dv.getOrd(1));
    dv.lookupOrd(1, scratch);
    assertEquals("hello world 2", scratch.utf8ToString());
    assertEquals(0, dv.getOrd(2));

    ireader.close();
    directory.close();
  }

  public void testSortedBytesTwoDocumentsMerged() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(newField("id", "0", StringField.TYPE_STORED));
    doc.add(new SortedDocValuesField("dv", new BytesRef("hello world 1")));
    iwriter.addDocument(doc);
    iwriter.commit();
    doc = new Document();
    doc.add(newField("id", "1", StringField.TYPE_STORED));
    doc.add(new SortedDocValuesField("dv", new BytesRef("hello world 2")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
    assertEquals(2, dv.getValueCount()); // 2 ords
    BytesRef scratch = new BytesRef();
    dv.lookupOrd(0, scratch);
    assertEquals(new BytesRef("hello world 1"), scratch);
    dv.lookupOrd(1, scratch);
    assertEquals(new BytesRef("hello world 2"), scratch);
    for(int i=0;i<2;i++) {
      StoredDocument doc2 = ireader.leaves().get(0).reader().document(i);
      String expected;
      if (doc2.get("id").equals("0")) {
        expected = "hello world 1";
      } else {
        expected = "hello world 2";
      }
      dv.lookupOrd(dv.getOrd(i), scratch);
      assertEquals(expected, scratch.utf8ToString());
    }

    ireader.close();
    directory.close();
  }

  public void testBytesWithNewline() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("dv", new BytesRef("hello\nworld\r1")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv");
    BytesRef scratch = new BytesRef();
    dv.get(0, scratch);
    assertEquals(new BytesRef("hello\nworld\r1"), scratch);

    ireader.close();
    directory.close();
  }

  public void testMissingSortedBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("hello world 2")));
    iwriter.addDocument(doc);
    // 2nd doc missing the DV field
    iwriter.addDocument(new Document());
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
    BytesRef scratch = new BytesRef();
    dv.lookupOrd(dv.getOrd(0), scratch);
    assertEquals(new BytesRef("hello world 2"), scratch);
    dv.lookupOrd(dv.getOrd(1), scratch);
    assertEquals(new BytesRef(""), scratch);
    ireader.close();
    directory.close();
  }
  
  public void testEmptySortedBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("")));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    SortedDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
    BytesRef scratch = new BytesRef();
    assertEquals(0, dv.getOrd(0));
    assertEquals(0, dv.getOrd(1));
    dv.lookupOrd(dv.getOrd(0), scratch);
    assertEquals("", scratch.utf8ToString());

    ireader.close();
    directory.close();
  }
  
  public void testEmptyBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("dv", new BytesRef("")));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new BinaryDocValuesField("dv", new BytesRef("")));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv");
    BytesRef scratch = new BytesRef();
    dv.get(0, scratch);
    assertEquals("", scratch.utf8ToString());
    dv.get(1, scratch);
    assertEquals("", scratch.utf8ToString());

    ireader.close();
    directory.close();
  }
  
  public void testVeryLargeButLegalBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    byte bytes[] = new byte[32766];
    BytesRef b = new BytesRef(bytes);
    random().nextBytes(bytes);
    doc.add(new BinaryDocValuesField("dv", b));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv");
    BytesRef scratch = new BytesRef();
    dv.get(0, scratch);
    assertEquals(new BytesRef(bytes), scratch);

    ireader.close();
    directory.close();
  }
  
  public void testVeryLargeButLegalSortedBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    byte bytes[] = new byte[32766];
    BytesRef b = new BytesRef(bytes);
    random().nextBytes(bytes);
    doc.add(new SortedDocValuesField("dv", b));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
    BytesRef scratch = new BytesRef();
    dv.get(0, scratch);
    assertEquals(new BytesRef(bytes), scratch);
    ireader.close();
    directory.close();
  }
  
  public void testCodecUsesOwnBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("dv", new BytesRef("boo!")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv");
    byte mybytes[] = new byte[20];
    BytesRef scratch = new BytesRef(mybytes);
    dv.get(0, scratch);
    assertEquals("boo!", scratch.utf8ToString());
    assertFalse(scratch.bytes == mybytes);

    ireader.close();
    directory.close();
  }
  
  public void testCodecUsesOwnSortedBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("boo!")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
    byte mybytes[] = new byte[20];
    BytesRef scratch = new BytesRef(mybytes);
    dv.get(0, scratch);
    assertEquals("boo!", scratch.utf8ToString());
    assertFalse(scratch.bytes == mybytes);

    ireader.close();
    directory.close();
  }
  
  public void testCodecUsesOwnBytesEachTime() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("dv", new BytesRef("foo!")));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new BinaryDocValuesField("dv", new BytesRef("bar!")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getBinaryDocValues("dv");
    BytesRef scratch = new BytesRef();
    dv.get(0, scratch);
    assertEquals("foo!", scratch.utf8ToString());
    
    BytesRef scratch2 = new BytesRef();
    dv.get(1, scratch2);
    assertEquals("bar!", scratch2.utf8ToString());
    // check scratch is still valid
    assertEquals("foo!", scratch.utf8ToString());

    ireader.close();
    directory.close();
  }
  
  public void testCodecUsesOwnSortedBytesEachTime() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    conf.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo!")));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("bar!")));
    iwriter.addDocument(doc);
    iwriter.close();
    
    // Now search the index:
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    assert ireader.leaves().size() == 1;
    BinaryDocValues dv = ireader.leaves().get(0).reader().getSortedDocValues("dv");
    BytesRef scratch = new BytesRef();
    dv.get(0, scratch);
    assertEquals("foo!", scratch.utf8ToString());
    
    BytesRef scratch2 = new BytesRef();
    dv.get(1, scratch2);
    assertEquals("bar!", scratch2.utf8ToString());
    // check scratch is still valid
    assertEquals("foo!", scratch.utf8ToString());

    ireader.close();
    directory.close();
  }
  
  /*
   * Simple test case to show how to use the API
   */
  public void testDocValuesSimple() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
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

    writer.close(true);

    DirectoryReader reader = DirectoryReader.open(dir, 1);
    assertEquals(1, reader.leaves().size());
  
    IndexSearcher searcher = new IndexSearcher(reader);

    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term("docId", "0")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("docId", "1")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("docId", "2")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("docId", "3")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("docId", "4")), BooleanClause.Occur.SHOULD);

    TopDocs search = searcher.search(query, 10);
    assertEquals(5, search.totalHits);
    ScoreDoc[] scoreDocs = search.scoreDocs;
    NumericDocValues docValues = getOnlySegmentReader(reader).getNumericDocValues("docId");
    for (int i = 0; i < scoreDocs.length; i++) {
      assertEquals(i, scoreDocs[i].doc);
      assertEquals(i, docValues.get(scoreDocs[i].doc));
    }
    reader.close();
    dir.close();
  }
  
  public void testRandomSortedBytes() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig cfg = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, cfg);
    int numDocs = atLeast(100);
    BytesRefHash hash = new BytesRefHash();
    Map<String, String> docToString = new HashMap<String, String>();
    int maxLength = _TestUtil.nextInt(random(), 1, 50);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(newTextField("id", "" + i, Field.Store.YES));
      String string = _TestUtil.randomRealisticUnicodeString(random(), 1, maxLength);
      BytesRef br = new BytesRef(string);
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
    BytesRef bytesRef = new BytesRef();
    hash.add(bytesRef); // add empty value for the gaps
    if (rarely()) {
      w.commit();
    }
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      String id = "" + i + numDocs;
      doc.add(newTextField("id", id, Field.Store.YES));
      String string = _TestUtil.randomRealisticUnicodeString(random(), 1, maxLength);
      BytesRef br = new BytesRef(string);
      hash.add(br);
      docToString.put(id, string);
      doc.add(new SortedDocValuesField("field", br));
      w.addDocument(doc);
    }
    w.commit();
    IndexReader reader = w.getReader();
    SortedDocValues docValues = MultiDocValues.getSortedValues(reader, "field");
    int[] sort = hash.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
    BytesRef expected = new BytesRef();
    BytesRef actual = new BytesRef();
    assertEquals(hash.size(), docValues.getValueCount());
    for (int i = 0; i < hash.size(); i++) {
      hash.get(sort[i], expected);
      docValues.lookupOrd(i, actual);
      assertEquals(expected.utf8ToString(), actual.utf8ToString());
      int ord = docValues.lookupTerm(expected, actual);
      assertEquals(i, ord);
    }
    AtomicReader slowR = SlowCompositeReaderWrapper.wrap(reader);
    Set<Entry<String, String>> entrySet = docToString.entrySet();

    for (Entry<String, String> entry : entrySet) {
      // pk lookup
      DocsEnum termDocsEnum = slowR.termDocsEnum(new Term("id", entry.getKey()));
      int docId = termDocsEnum.nextDoc();
      expected = new BytesRef(entry.getValue());
      docValues.get(docId, actual);
      assertEquals(expected, actual);
    }

    reader.close();
    w.close();
    dir.close();
  }
  
  private void doTestNumericsVsStoredFields(long minValue, long maxValue) throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    Field storedField = newStringField("stored", "", Field.Store.YES);
    Field dvField = new NumericDocValuesField("dv", 0);
    doc.add(idField);
    doc.add(storedField);
    doc.add(dvField);
    
    // index some docs
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; i++) {
      idField.setStringValue(Integer.toString(i));
      long value = _TestUtil.nextLong(random(), minValue, maxValue);
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
    writer.close();
    
    // compare
    DirectoryReader ir = DirectoryReader.open(dir);
    for (AtomicReaderContext context : ir.leaves()) {
      AtomicReader r = context.reader();
      NumericDocValues docValues = r.getNumericDocValues("dv");
      for (int i = 0; i < r.maxDoc(); i++) {
        long storedValue = Long.parseLong(r.document(i).get("stored"));
        assertEquals(storedValue, docValues.get(i));
      }
    }
    ir.close();
    dir.close();
  }
  
  public void testBooleanNumericsVsStoredFields() throws Exception {
    doTestNumericsVsStoredFields(0, 1);
  }
  
  public void testByteNumericsVsStoredFields() throws Exception {
    doTestNumericsVsStoredFields(Byte.MIN_VALUE, Byte.MAX_VALUE);
  }
  
  public void testShortNumericsVsStoredFields() throws Exception {
    doTestNumericsVsStoredFields(Short.MIN_VALUE, Short.MAX_VALUE);
  }
  
  public void testIntNumericsVsStoredFields() throws Exception {
    doTestNumericsVsStoredFields(Integer.MIN_VALUE, Integer.MAX_VALUE);
  }
  
  public void testLongNumericsVsStoredFields() throws Exception {
    doTestNumericsVsStoredFields(Long.MIN_VALUE, Long.MAX_VALUE);
  }
  
  private void doTestBinaryVsStoredFields(int minLength, int maxLength) throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    Field storedField = new StoredField("stored", new byte[0]);
    Field dvField = new BinaryDocValuesField("dv", new BytesRef());
    doc.add(idField);
    doc.add(storedField);
    doc.add(dvField);
    
    // index some docs
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; i++) {
      idField.setStringValue(Integer.toString(i));
      final int length;
      if (minLength == maxLength) {
        length = minLength; // fixed length
      } else {
        length = _TestUtil.nextInt(random(), minLength, maxLength);
      }
      byte buffer[] = new byte[length];
      random().nextBytes(buffer);
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
    writer.close();
    
    // compare
    DirectoryReader ir = DirectoryReader.open(dir);
    for (AtomicReaderContext context : ir.leaves()) {
      AtomicReader r = context.reader();
      BinaryDocValues docValues = r.getBinaryDocValues("dv");
      for (int i = 0; i < r.maxDoc(); i++) {
        BytesRef binaryValue = r.document(i).getBinaryValue("stored");
        BytesRef scratch = new BytesRef();
        docValues.get(i, scratch);
        assertEquals(binaryValue, scratch);
      }
    }
    ir.close();
    dir.close();
  }
  
  public void testBinaryFixedLengthVsStoredFields() throws Exception {
    int fixedLength = _TestUtil.nextInt(random(), 1, 10);
    doTestBinaryVsStoredFields(fixedLength, fixedLength);
  }
  
  public void testBinaryVariableLengthVsStoredFields() throws Exception {
    doTestBinaryVsStoredFields(1, 10);
  }
  
  private void doTestSortedVsStoredFields(int minLength, int maxLength) throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    Field storedField = new StoredField("stored", new byte[0]);
    Field dvField = new SortedDocValuesField("dv", new BytesRef());
    doc.add(idField);
    doc.add(storedField);
    doc.add(dvField);
    
    // index some docs
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; i++) {
      idField.setStringValue(Integer.toString(i));
      final int length;
      if (minLength == maxLength) {
        length = minLength; // fixed length
      } else {
        length = _TestUtil.nextInt(random(), minLength, maxLength);
      }
      byte buffer[] = new byte[length];
      random().nextBytes(buffer);
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
    writer.close();
    
    // compare
    DirectoryReader ir = DirectoryReader.open(dir);
    for (AtomicReaderContext context : ir.leaves()) {
      AtomicReader r = context.reader();
      BinaryDocValues docValues = r.getSortedDocValues("dv");
      for (int i = 0; i < r.maxDoc(); i++) {
        BytesRef binaryValue = r.document(i).getBinaryValue("stored");
        BytesRef scratch = new BytesRef();
        docValues.get(i, scratch);
        assertEquals(binaryValue, scratch);
      }
    }
    ir.close();
    dir.close();
  }
  
  public void testSortedFixedLengthVsStoredFields() throws Exception {
    int fixedLength = _TestUtil.nextInt(random(), 1, 10);
    doTestSortedVsStoredFields(fixedLength, fixedLength);
  }
  
  public void testSortedVariableLengthVsStoredFields() throws Exception {
    doTestSortedVsStoredFields(1, 10);
  }
}
