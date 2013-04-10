package org.apache.lucene.codecs.perfield;

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
import java.util.Collections;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomCodec;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

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
  
  // just a simple trivial test
  // TODO: we should come up with a test that somehow checks that segment suffix
  // is respected by all codec apis (not just docvalues and postings)
  public void testTwoFieldsTwoFormats() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!1
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    final DocValuesFormat fast = DocValuesFormat.forName("Lucene42");
    final DocValuesFormat slow = DocValuesFormat.forName("SimpleText");
    iwc.setCodec(new Lucene42Codec() {
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
    TopDocs hits = isearcher.search(query, null, 1);
    assertEquals(1, hits.totalHits);
    BytesRef scratch = new BytesRef();
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      Document hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
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
}
