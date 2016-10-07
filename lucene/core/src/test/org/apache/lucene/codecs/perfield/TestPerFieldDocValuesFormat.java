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
package org.apache.lucene.codecs.perfield;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.asserting.AssertingCodec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomCodec;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;

/**
 * Basic tests of PerFieldDocValuesFormat
 */
public class TestPerFieldDocValuesFormat extends BaseDocValuesFormatTestCase {
  private Codec codec;
  
  @Override
  public void setUp() throws Exception {
    codec = new RandomCodec(new Random(random().nextLong()), Collections.<String>emptySet());
    super.setUp();
  }
  
  @Override
  protected Codec getCodec() {
    return codec;
  }

  @Override
  protected boolean codecAcceptsHugeBinaryValues(String field) {
    return TestUtil.fieldSupportsHugeBinaryDocValues(field);
  }
  
  // just a simple trivial test
  // TODO: we should come up with a test that somehow checks that segment suffix
  // is respected by all codec apis (not just docvalues and postings)
  public void testTwoFieldsTwoFormats() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!1
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    final DocValuesFormat fast = TestUtil.getDefaultDocValuesFormat();
    final DocValuesFormat slow = DocValuesFormat.forName("Memory");
    iwc.setCodec(new AssertingCodec() {
      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        if ("dv1".equals(field)) {
          return fast;
        } else {
          return slow;
        }
      }
    });
    IndexWriter iwriter = new IndexWriter(directory, iwc);
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
    IndexSearcher isearcher = newSearcher(ireader);

    assertEquals(1, isearcher.search(new TermQuery(new Term("fieldname", longTerm)), 1).totalHits);
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, 1);
    assertEquals(1, hits.totalHits);
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      Document hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
      assertEquals(text, hitDoc.get("fieldname"));
      assert ireader.leaves().size() == 1;
      NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv1");
      assertEquals(5, dv.get(hits.scoreDocs[i].doc));
      BinaryDocValues dv2 = ireader.leaves().get(0).reader().getBinaryDocValues("dv2");
      final BytesRef term = dv2.get(hits.scoreDocs[i].doc);
      assertEquals(new BytesRef("hello world"), term);
    }

    ireader.close();
    directory.close();
  }

  public void testMergeCalledOnTwoFormats() throws IOException {
    MergeRecordingDocValueFormatWrapper dvf1 = new MergeRecordingDocValueFormatWrapper(TestUtil.getDefaultDocValuesFormat());
    MergeRecordingDocValueFormatWrapper dvf2 = new MergeRecordingDocValueFormatWrapper(TestUtil.getDefaultDocValuesFormat());

    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(new AssertingCodec() {
      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        switch (field) {
          case "dv1":
          case "dv2":
            return dvf1;

          case "dv3":
            return dvf2;

          default:
            return super.getDocValuesFormatForField(field);
        }
      }
    });

    Directory directory = newDirectory();

    IndexWriter iwriter = new IndexWriter(directory, iwc);

    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv1", 5));
    doc.add(new NumericDocValuesField("dv2", 42));
    doc.add(new BinaryDocValuesField("dv3", new BytesRef("hello world")));
    iwriter.addDocument(doc);
    iwriter.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("dv1", 8));
    doc.add(new NumericDocValuesField("dv2", 45));
    doc.add(new BinaryDocValuesField("dv3", new BytesRef("goodbye world")));
    iwriter.addDocument(doc);
    iwriter.commit();

    iwriter.forceMerge(1, true);
    iwriter.close();

    assertEquals(1, dvf1.nbMergeCalls);
    assertEquals(new HashSet<>(Arrays.asList("dv1", "dv2")), new HashSet<>(dvf1.fieldNames));
    assertEquals(1, dvf2.nbMergeCalls);
    assertEquals(Collections.singletonList("dv3"), dvf2.fieldNames);

    directory.close();
  }

  private static final class MergeRecordingDocValueFormatWrapper extends DocValuesFormat {
    private final DocValuesFormat delegate;
    final List<String> fieldNames = new ArrayList<>();
    volatile int nbMergeCalls = 0;

    MergeRecordingDocValueFormatWrapper(DocValuesFormat delegate) {
      super(delegate.getName());
      this.delegate = delegate;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
      final DocValuesConsumer consumer = delegate.fieldsConsumer(state);
      return new DocValuesConsumer() {
        @Override
        public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
          consumer.addNumericField(field, values);
        }

        @Override
        public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
          consumer.addBinaryField(field, values);
        }

        @Override
        public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
          consumer.addSortedField(field, values, docToOrd);
        }

        @Override
        public void addSortedNumericField(FieldInfo field, Iterable<Number> docToValueCount, Iterable<Number> values) throws IOException {
          consumer.addSortedNumericField(field, docToValueCount, values);
        }

        @Override
        public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException {
          consumer.addSortedSetField(field, values, docToOrdCount, ords);
        }

        @Override
        public void merge(MergeState mergeState) throws IOException {
          nbMergeCalls++;
          for (FieldInfo fi : mergeState.mergeFieldInfos) {
            fieldNames.add(fi.name);
          }
          consumer.merge(mergeState);
        }

        @Override
        public void close() throws IOException {
          consumer.close();
        }
      };
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
      return delegate.fieldsProducer(state);
    }
  }
}
