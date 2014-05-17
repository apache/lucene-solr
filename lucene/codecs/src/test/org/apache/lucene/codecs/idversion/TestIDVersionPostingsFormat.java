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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PerThreadPKLookup;
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
    doc = new Document();
    doc.add(makeIDField("id1", 110));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    IDVersionSegmentTermsEnum termsEnum = (IDVersionSegmentTermsEnum) r.leaves().get(0).reader().fields().terms("id").iterator(null);
    assertTrue(termsEnum.seekExact(new BytesRef("id0"), 50));
    assertTrue(termsEnum.seekExact(new BytesRef("id0"), 100));
    assertFalse(termsEnum.seekExact(new BytesRef("id0"), 101));
    assertTrue(termsEnum.seekExact(new BytesRef("id1"), 50));
    assertTrue(termsEnum.seekExact(new BytesRef("id1"), 110));
    assertFalse(termsEnum.seekExact(new BytesRef("id1"), 111));
    r.close();

    w.close();
    dir.close();
  }

  // nocommit vary the style of iD; sometimes fixed-length ids, timestamp, zero filled, seuqential, random, etc.

  public void testRandom() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    // nocommit randomize the block sizes:
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    // nocommit put back
    //RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    IndexWriter w = new IndexWriter(dir, iwc);
    int numDocs = atLeast(1000);
    Map<String,Long> idValues = new HashMap<String,Long>();
    int docUpto = 0;
    if (VERBOSE) {
      System.out.println("TEST: numDocs=" + numDocs);
    }
    long version = 0;
    while (docUpto < numDocs) {
      // nocommit add deletes in
      // nocommit randomRealisticUniode / full binary
      String idValue = TestUtil.randomSimpleString(random());
      if (idValues.containsKey(idValue)) {
        continue;
      }
      //long version = random().nextLong() & 0x7fffffffffffffffL;
      version++;
      idValues.put(idValue, version);
      if (VERBOSE) {
        System.out.println("  " + idValue + " -> " + version);
      }
      Document doc = new Document();
      doc.add(makeIDField(idValue, version));
      w.addDocument(doc);
      docUpto++;
    }

    //IndexReader r = w.getReader();
    IndexReader r = DirectoryReader.open(w, true);
    PerThreadVersionPKLookup lookup = new PerThreadVersionPKLookup(r, "id");

    List<Map.Entry<String,Long>> idValuesList = new ArrayList<>(idValues.entrySet());
    int iters = numDocs * 5;
    for(int iter=0;iter<iters;iter++) {
      String idValue;

      if (random().nextBoolean()) {
        idValue = idValuesList.get(random().nextInt(numDocs)).getKey();
      } else {
        idValue = TestUtil.randomSimpleString(random());
      }

      BytesRef idValueBytes = new BytesRef(idValue);

      Long expectedVersion = idValues.get(idValue);

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " id=" + idValue + " expectedVersion=" + expectedVersion);
      }
      
      if (expectedVersion == null) {
        assertEquals("term should not have been found (doesn't exist)", -1, lookup.lookup(idValueBytes));
      } else {
        if (random().nextBoolean()) {
          if (VERBOSE) {
            System.out.println("  lookup exact version (should be found)");
          }
          assertTrue("term should have been found (version too old)", lookup.lookup(idValueBytes, expectedVersion.longValue()) != -1);
        } else {
          if (VERBOSE) {
            System.out.println("  lookup version+1 (should not be found)");
          }
          assertEquals("term should not have been found (version newer)", -1, lookup.lookup(idValueBytes, expectedVersion.longValue()+1));
        }
      }
    }

    r.close();
    w.close();
    dir.close();
  }

  private static class PerThreadVersionPKLookup extends PerThreadPKLookup {
    public PerThreadVersionPKLookup(IndexReader r, String field) throws IOException {
      super(r, field);
    }

    /** Returns docID if found, else -1. */
    public int lookup(BytesRef id, long version) throws IOException {
      for(int seg=0;seg<numSegs;seg++) {
        if (((IDVersionSegmentTermsEnum) termsEnums[seg]).seekExact(id, version)) {
          if (VERBOSE) {
            System.out.println("  found in seg=" + termsEnums[seg]);
          }
          docsEnums[seg] = termsEnums[seg].docs(liveDocs[seg], docsEnums[seg], 0);
          int docID = docsEnums[seg].nextDoc();
          if (docID != DocsEnum.NO_MORE_DOCS) {
            return docBases[seg] + docID;
          }
          assert hasDeletions;
        }
      }

      return -1;
    }
  }

  /** Produces a single token from the provided value, with the provided payload. */
  private static class StringAndPayloadField extends Field {

    public static final FieldType TYPE = new FieldType();

    static {
      TYPE.setIndexed(true);
      TYPE.setOmitNorms(true);
      TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
      TYPE.setTokenized(true);
      TYPE.freeze();
    }

    private final BytesRef payload;

    public StringAndPayloadField(String name, String value, BytesRef payload) {
      super(name, value, TYPE);
      this.payload = payload;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) throws IOException {
      SingleTokenWithPayloadTokenStream ts;
      if (reuse instanceof SingleTokenWithPayloadTokenStream) {
        ts = (SingleTokenWithPayloadTokenStream) reuse;
      } else {
        ts = new SingleTokenWithPayloadTokenStream();
      }
      ts.setValue((String) fieldsData, payload);
      return ts;
    }
  }

  private static final class SingleTokenWithPayloadTokenStream extends TokenStream {

    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private final PayloadAttribute payloadAttribute = addAttribute(PayloadAttribute.class);
    private boolean used = false;
    private String value = null;
    private BytesRef payload;
    
    /** Creates a new TokenStream that returns a String+payload as single token.
     * <p>Warning: Does not initialize the value, you must call
     * {@link #setValue(String)} afterwards!
     */
    SingleTokenWithPayloadTokenStream() {
    }
    
    /** Sets the string value. */
    void setValue(String value, BytesRef payload) {
      this.value = value;
      this.payload = payload;
    }

    @Override
    public boolean incrementToken() {
      if (used) {
        return false;
      }
      clearAttributes();
      termAttribute.append(value);
      payloadAttribute.setPayload(payload);
      used = true;
      return true;
    }

    @Override
    public void reset() {
      used = false;
    }

    @Override
    public void close() {
      value = null;
      payload = null;
    }
  }

  private static Field makeIDField(String id, long version) {
    BytesRef payload = new BytesRef(8);
    payload.length = 8;
    IDVersionPostingsFormat.longToBytes(version, payload);
    return new StringAndPayloadField("id", id, payload);

    /*
    Field field = newTextField("id", "", Field.Store.NO);
    Token token = new Token(id, 0, id.length());
    token.setPayload(payload);
    field.setTokenStream(new CannedTokenStream(token));
    return field;
    */
  }
}
