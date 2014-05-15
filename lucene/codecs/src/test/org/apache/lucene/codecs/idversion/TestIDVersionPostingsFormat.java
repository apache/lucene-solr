package org.apache.lucene.codecs.idversion;

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

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Basic tests for IDVersionPostingsFormat
 */
public class TestIDVersionPostingsFormat extends LuceneTestCase {

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(makeIDField("id0", 100));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    IDVersionSegmentTermsEnum termsEnum = (IDVersionSegmentTermsEnum) MultiFields.getTerms(r, "id").iterator(null);
    assertTrue(termsEnum.seekExact(new BytesRef("id0"), 50));
    assertFalse(termsEnum.seekExact(new BytesRef("id0"), 101));
    r.close();

    w.close();
    dir.close();
  }

  // nocommit need testRandom

  private static Field makeIDField(String id, long version) {
    Field field = newTextField("id", "", Field.Store.NO);
    Token token = new Token(id, 0, id.length());
    BytesRef payload = new BytesRef(8);
    IDVersionPostingsFormat.longToBytes(100, payload);
    token.setPayload(payload);
    field.setTokenStream(new CannedTokenStream(token));
    return field;
  }
}
