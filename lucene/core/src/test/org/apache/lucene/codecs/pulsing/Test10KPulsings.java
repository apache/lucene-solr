package org.apache.lucene.codecs.pulsing;

/**
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

import java.io.File;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.pulsing.Pulsing40PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Pulses 10k terms/docs, 
 * originally designed to find JRE bugs (https://issues.apache.org/jira/browse/LUCENE-3335)
 * 
 * @lucene.experimental
 */
@LuceneTestCase.Nightly
public class Test10KPulsings extends LuceneTestCase {
  public void test10kPulsed() throws Exception {
    // we always run this test with pulsing codec.
    Codec cp = _TestUtil.alwaysPostingsFormat(new Pulsing40PostingsFormat(1));
    
    File f = _TestUtil.getTempDir("10kpulsed");
    MockDirectoryWrapper dir = newFSDirectory(f);
    dir.setCheckIndexOnClose(false); // we do this ourselves explicitly
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setCodec(cp));
    
    Document document = new Document();
    FieldType ft = new FieldType(TextField.TYPE_STORED);
    
    switch(_TestUtil.nextInt(random(), 0, 2)) {
      case 0: ft.setIndexOptions(IndexOptions.DOCS_ONLY); break;
      case 1: ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS); break;
      default: ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS); break; 
    }

    Field field = newField("field", "", ft);
    document.add(field);
    
    NumberFormat df = new DecimalFormat("00000", new DecimalFormatSymbols(Locale.ENGLISH));

    for (int i = 0; i < 10050; i++) {
      field.setStringValue(df.format(i));
      iw.addDocument(document);
    }
    
    IndexReader ir = iw.getReader();
    iw.close();

    TermsEnum te = MultiFields.getTerms(ir, "field").iterator(null);
    DocsEnum de = null;
    
    for (int i = 0; i < 10050; i++) {
      String expected = df.format(i);
      assertEquals(expected, te.next().utf8ToString());
      de = _TestUtil.docs(random(), te, null, de, false);
      assertTrue(de.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, de.nextDoc());
    }
    ir.close();

    _TestUtil.checkIndex(dir);
    dir.close();
  }
  
  /** a variant, that uses pulsing, but uses a high TF to force pass thru to the underlying codec
   */
  public void test10kNotPulsed() throws Exception {
    // we always run this test with pulsing codec.
    int freqCutoff = _TestUtil.nextInt(random(), 1, 10);
    Codec cp = _TestUtil.alwaysPostingsFormat(new Pulsing40PostingsFormat(freqCutoff));
    
    File f = _TestUtil.getTempDir("10knotpulsed");
    MockDirectoryWrapper dir = newFSDirectory(f);
    dir.setCheckIndexOnClose(false); // we do this ourselves explicitly
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setCodec(cp));
    
    Document document = new Document();
    FieldType ft = new FieldType(TextField.TYPE_STORED);
    
    switch(_TestUtil.nextInt(random(), 0, 2)) {
      case 0: ft.setIndexOptions(IndexOptions.DOCS_ONLY); break;
      case 1: ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS); break;
      default: ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS); break; 
    }

    Field field = newField("field", "", ft);
    document.add(field);
    
    NumberFormat df = new DecimalFormat("00000", new DecimalFormatSymbols(Locale.ENGLISH));

    final int freq = freqCutoff + 1;
    
    for (int i = 0; i < 10050; i++) {
      StringBuilder sb = new StringBuilder();
      for (int j = 0; j < freq; j++) {
        sb.append(df.format(i));
        sb.append(' '); // whitespace
      }
      field.setStringValue(sb.toString());
      iw.addDocument(document);
    }
    
    IndexReader ir = iw.getReader();
    iw.close();

    TermsEnum te = MultiFields.getTerms(ir, "field").iterator(null);
    DocsEnum de = null;
    
    for (int i = 0; i < 10050; i++) {
      String expected = df.format(i);
      assertEquals(expected, te.next().utf8ToString());
      de = _TestUtil.docs(random(), te, null, de, false);
      assertTrue(de.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, de.nextDoc());
    }
    ir.close();

    _TestUtil.checkIndex(dir);
    dir.close();
  }
}
