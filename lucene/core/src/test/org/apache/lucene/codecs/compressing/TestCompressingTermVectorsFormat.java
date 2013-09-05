package org.apache.lucene.codecs.compressing;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BaseTermVectorsFormatTestCase;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

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

@Repeat(iterations=5) // give it a chance to test various compression modes with different chunk sizes
public class TestCompressingTermVectorsFormat extends BaseTermVectorsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return CompressingCodec.randomInstance(random());
  }
  
  // https://issues.apache.org/jira/browse/LUCENE-5156
  public void testNoOrds() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(true);
    doc.add(new Field("foo", "this is a test", ft));
    iw.addDocument(doc);
    AtomicReader ir = getOnlySegmentReader(iw.getReader());
    Terms terms = ir.getTermVector(0, "foo");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator(null);
    assertEquals(SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("this")));
    try {
      termsEnum.ord();
      fail();
    } catch (UnsupportedOperationException expected) {
      // expected exception
    }
    
    try {
      termsEnum.seekExact(0);
      fail();
    } catch (UnsupportedOperationException expected) {
      // expected exception
    }
    ir.close();
    iw.close();
    dir.close();
  }
}
