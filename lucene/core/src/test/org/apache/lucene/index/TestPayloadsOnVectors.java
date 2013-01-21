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
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

public class TestPayloadsOnVectors extends LuceneTestCase {

  /** some docs have payload att, some not */
  public void testMixupDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorPayloads(true);
    customType.setStoreTermVectorOffsets(random().nextBoolean());
    Field field = new Field("field", "", customType);
    TokenStream ts = new MockTokenizer(new StringReader("here we go"), MockTokenizer.WHITESPACE, true);
    assertFalse(ts.hasAttribute(PayloadAttribute.class));
    field.setTokenStream(ts);
    doc.add(field);
    writer.addDocument(doc);
    
    Token withPayload = new Token("withPayload", 0, 11);
    withPayload.setPayload(new BytesRef("test"));
    ts = new CannedTokenStream(withPayload);
    assertTrue(ts.hasAttribute(PayloadAttribute.class));
    field.setTokenStream(ts);
    writer.addDocument(doc);
    
    ts = new MockTokenizer(new StringReader("another"), MockTokenizer.WHITESPACE, true);
    assertFalse(ts.hasAttribute(PayloadAttribute.class));
    field.setTokenStream(ts);
    writer.addDocument(doc);
    
    DirectoryReader reader = writer.getReader();
    Terms terms = reader.getTermVector(1, "field");
    assert terms != null;
    TermsEnum termsEnum = terms.iterator(null);
    assertTrue(termsEnum.seekExact(new BytesRef("withPayload"), true));
    DocsAndPositionsEnum de = termsEnum.docsAndPositions(null, null);
    assertEquals(0, de.nextDoc());
    assertEquals(0, de.nextPosition());
    assertEquals(new BytesRef("test"), de.getPayload());
    writer.close();
    reader.close();
    dir.close();
  }
  
  /** some field instances have payload att, some not */
  public void testMixupMultiValued() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorPayloads(true);
    customType.setStoreTermVectorOffsets(random().nextBoolean());
    Field field = new Field("field", "", customType);
    TokenStream ts = new MockTokenizer(new StringReader("here we go"), MockTokenizer.WHITESPACE, true);
    assertFalse(ts.hasAttribute(PayloadAttribute.class));
    field.setTokenStream(ts);
    doc.add(field);
    Field field2 = new Field("field", "", customType);
    Token withPayload = new Token("withPayload", 0, 11);
    withPayload.setPayload(new BytesRef("test"));
    ts = new CannedTokenStream(withPayload);
    assertTrue(ts.hasAttribute(PayloadAttribute.class));
    field2.setTokenStream(ts);
    doc.add(field2);
    Field field3 = new Field("field", "", customType);
    ts = new MockTokenizer(new StringReader("nopayload"), MockTokenizer.WHITESPACE, true);
    assertFalse(ts.hasAttribute(PayloadAttribute.class));
    field3.setTokenStream(ts);
    doc.add(field3);
    writer.addDocument(doc);
    DirectoryReader reader = writer.getReader();
    Terms terms = reader.getTermVector(0, "field");
    assert terms != null;
    TermsEnum termsEnum = terms.iterator(null);
    assertTrue(termsEnum.seekExact(new BytesRef("withPayload"), true));
    DocsAndPositionsEnum de = termsEnum.docsAndPositions(null, null);
    assertEquals(0, de.nextDoc());
    assertEquals(3, de.nextPosition());
    assertEquals(new BytesRef("test"), de.getPayload());
    writer.close();
    reader.close();
    dir.close();
  }
  
  public void testPayloadsWithoutPositions() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(false);
    customType.setStoreTermVectorPayloads(true);
    customType.setStoreTermVectorOffsets(random().nextBoolean());
    doc.add(new Field("field", "foo", customType));
    try {
      writer.addDocument(doc);
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
    writer.close();
    dir.close();
  }
  
  // custom impl to test cases that are forbidden by the default OffsetAttribute impl
  static class PermissiveOffsetAttributeImpl extends AttributeImpl implements OffsetAttribute {

    int start, end;

    @Override
    public int startOffset() {
      return start;
    }

    @Override
    public int endOffset() {
      return end;
    }

    @Override
    public void setOffset(int startOffset, int endOffset) {
      // no check!
      start = startOffset;
      end = endOffset;
    }

    @Override
    public void clear() {
      start = end = 0;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }

      if (other instanceof PermissiveOffsetAttributeImpl) {
        PermissiveOffsetAttributeImpl o = (PermissiveOffsetAttributeImpl) other;
        return o.start == start && o.end == end;
      }

      return false;
    }

    @Override
    public int hashCode() {
      return start + 31 * end;
    }

    @Override
    public void copyTo(AttributeImpl target) {
      OffsetAttribute t = (OffsetAttribute) target;
      t.setOffset(start, end);
    }

  }

  static BytesRef randomPayload() {
    final int len = random().nextInt(5);
    if (len == 0) {
      return null;
    }
    final BytesRef payload = new BytesRef(len);
    random().nextBytes(payload.bytes);
    payload.length = len;
    return payload;
  }

  class RandomTokenStream extends TokenStream {

    final String[] terms;
    final int[] positionsIncrements;
    final int[] positions;
    final int[] startOffsets, endOffsets;
    final BytesRef[] payloads;

    final Map<Integer, Set<Integer>> positionToTerms;
    final Map<Integer, Set<Integer>> startOffsetToTerms;

    final CharTermAttribute termAtt;
    final PositionIncrementAttribute piAtt;
    final OffsetAttribute oAtt;
    final PayloadAttribute pAtt;
    int i = 0;

    RandomTokenStream(int len, String[] sampleTerms, boolean weird) {
      terms = new String[len];
      positionsIncrements = new int[len];
      positions = new int[len];
      startOffsets = new int[len];
      endOffsets = new int[len];
      payloads = new BytesRef[len];
      for (int i = 0; i < len; ++i) {
        terms[i] = RandomPicks.randomFrom(random(), sampleTerms);
        if (weird) {
          positionsIncrements[i] = random().nextInt(1 << 18);
          startOffsets[i] = random().nextInt();
          endOffsets[i] = random().nextInt();
        } else if (i == 0) {
          positionsIncrements[i] = _TestUtil.nextInt(random(), 1, 1 << 5);
          startOffsets[i] = _TestUtil.nextInt(random(), 0, 1 << 16);
          endOffsets[i] = startOffsets[i] + _TestUtil.nextInt(random(), 0, rarely() ? 1 << 10 : 20);
        } else {
          positionsIncrements[i] = _TestUtil.nextInt(random(), 0, 1 << 5);
          startOffsets[i] = startOffsets[i-1] + _TestUtil.nextInt(random(), 0, 1 << 16);
          endOffsets[i] = startOffsets[i] + _TestUtil.nextInt(random(), 0, rarely() ? 1 << 10 : 20);
        }
      }
      for (int i = 0; i < len; ++i) {
        if (i == 0) {
          positions[i] = positionsIncrements[i] - 1;
        } else {
          positions[i] = positions[i - 1] + positionsIncrements[i];
        }
      }
      if (rarely()) {
        Arrays.fill(payloads, randomPayload());
      } else {
        for (int i = 0; i < len; ++i) {
          payloads[i] = randomPayload();
        }
      }

      positionToTerms = new HashMap<Integer, Set<Integer>>();
      startOffsetToTerms = new HashMap<Integer, Set<Integer>>();
      for (int i = 0; i < len; ++i) {
        if (!positionToTerms.containsKey(positions[i])) {
          positionToTerms.put(positions[i], new HashSet<Integer>(1));
        }
        positionToTerms.get(positions[i]).add(i);
        if (!startOffsetToTerms.containsKey(startOffsets[i])) {
          startOffsetToTerms.put(startOffsets[i], new HashSet<Integer>(1));
        }
        startOffsetToTerms.get(startOffsets[i]).add(i);
      }

      addAttributeImpl(new PermissiveOffsetAttributeImpl());

      termAtt = addAttribute(CharTermAttribute.class);
      piAtt = addAttribute(PositionIncrementAttribute.class);
      oAtt = addAttribute(OffsetAttribute.class);
      pAtt = addAttribute(PayloadAttribute.class);
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (i < terms.length) {
        termAtt.setLength(0).append(terms[i]);
        piAtt.setPositionIncrement(positionsIncrements[i]);
        oAtt.setOffset(startOffsets[i], endOffsets[i]);
        pAtt.setPayload(payloads[i]);
        ++i;
        return true;
      } else {
        return false;
      }
    }

  }

  static FieldType randomFieldType() {
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorPositions(random().nextBoolean());
    ft.setStoreTermVectorOffsets(random().nextBoolean());
    if (random().nextBoolean()) {
      ft.setStoreTermVectorPositions(true);
      ft.setStoreTermVectorPayloads(true);
    }
    ft.freeze();
    return ft;
  }

  public void testRandomVectors() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);
    String[] sampleTerms = new String[RandomInts.randomIntBetween(random(), 20, 50)];
    for (int i = 0; i < sampleTerms.length; ++i) {
      sampleTerms[i] = _TestUtil.randomUnicodeString(random());
    }
    FieldType ft = randomFieldType();
    // generate random documents and index them
    final String[] fieldNames = new String[_TestUtil.nextInt(random(), 1, 200)];
    for (int i = 0; i < fieldNames.length; ++i) {
      String fieldName;
      do {
        fieldName = _TestUtil.randomSimpleString(random());
      } while ("id".equals(fieldName));
      fieldNames[i] = fieldName;
    }
    final int numDocs = _TestUtil.nextInt(random(), 10, 100);
    @SuppressWarnings("unchecked")
    final Map<String, RandomTokenStream>[] fieldValues  = new Map[numDocs];
    for (int i = 0; i < numDocs; ++i) {
      fieldValues[i] = new HashMap<String, RandomTokenStream>();
      final int numFields = _TestUtil.nextInt(random(), 0, rarely() ? fieldNames.length : 5);
      for (int j = 0; j < numFields; ++j) {
        final String fieldName = fieldNames[(i+j*31) % fieldNames.length];
        final int tokenStreamLen = _TestUtil.nextInt(random(), 1, rarely() ? 300 : 5);
        fieldValues[i].put(fieldName, new RandomTokenStream(tokenStreamLen, sampleTerms, rarely()));
      }
    }

    // index them
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new IntField("id", i, Store.YES));
      for (Map.Entry<String, RandomTokenStream> entry : fieldValues[i].entrySet()) {
        doc.add(new Field(entry.getKey(), entry.getValue(), ft));
      }
      iw.addDocument(doc);
    }

    iw.commit();
    // make sure the format can merge
    iw.forceMerge(2);

    // read term vectors
    final DirectoryReader reader = DirectoryReader.open(dir);
    for (int i = 0; i < 100; ++i) {
      final int docID = random().nextInt(numDocs);
      final Map<String, RandomTokenStream> fvs = fieldValues[reader.document(docID).getField("id").numericValue().intValue()];
      final Fields fields = reader.getTermVectors(docID);
      if (fvs.isEmpty()) {
        assertNull(fields);
      } else {
        Set<String> fns = new HashSet<String>();
        for (String field : fields) {
          fns.add(field);
        }
        assertEquals(fields.size(), fns.size());
        assertEquals(fvs.keySet(), fns);
        for (String field : fields) {
          final RandomTokenStream tk = fvs.get(field);
          assert tk != null;
          final Terms terms = fields.terms(field);
          assertEquals(ft.storeTermVectorPositions(), terms.hasPositions());
          assertEquals(ft.storeTermVectorOffsets(), terms.hasOffsets());
          assertEquals(1, terms.getDocCount());
          final TermsEnum termsEnum = terms.iterator(null);
          while (termsEnum.next() != null) {
            assertEquals(1, termsEnum.docFreq());
            final DocsAndPositionsEnum docsAndPositionsEnum = termsEnum.docsAndPositions(null, null);
            final DocsEnum docsEnum = docsAndPositionsEnum == null ? termsEnum.docs(null, null) : docsAndPositionsEnum;
            if (ft.storeTermVectorOffsets() || ft.storeTermVectorPositions()) {
              assertNotNull(docsAndPositionsEnum);
            }
            assertEquals(0, docsEnum.nextDoc());
            if (terms.hasPositions() || terms.hasOffsets()) {
              final int freq = docsEnum.freq();
              assertTrue(freq >= 1);
              if (docsAndPositionsEnum != null) {
                for (int k = 0; k < freq; ++k) {
                  final int position = docsAndPositionsEnum.nextPosition();
                  final Set<Integer> indexes;
                  if (terms.hasPositions()) {
                    indexes = tk.positionToTerms.get(position);
                    assertNotNull(tk.positionToTerms.keySet().toString() + " does not contain " + position, indexes);
                  } else {
                    indexes = tk.startOffsetToTerms.get(docsAndPositionsEnum.startOffset());
                    assertNotNull(indexes);
                  }
                  if (terms.hasPositions()) {
                    boolean foundPosition = false;
                    for (int index : indexes) {
                      if (new BytesRef(tk.terms[index]).equals(termsEnum.term()) && tk.positions[index] == position) {
                        foundPosition = true;
                        break;
                      }
                    }
                    assertTrue(foundPosition);
                  }
                  if (terms.hasOffsets()) {
                    boolean foundOffset = false;
                    for (int index : indexes) {
                      if (new BytesRef(tk.terms[index]).equals(termsEnum.term()) && tk.startOffsets[index] == docsAndPositionsEnum.startOffset() && tk.endOffsets[index] == docsAndPositionsEnum.endOffset()) {
                        foundOffset = true;
                        break;
                      }
                    }
                    assertTrue(foundOffset);
                  }
                  if (terms.hasPayloads()) {
                    boolean foundPayload = false;
                    for (int index : indexes) {
                      if (new BytesRef(tk.terms[index]).equals(termsEnum.term()) && equals(tk.payloads[index], docsAndPositionsEnum.getPayload())) {
                        foundPayload = true;
                        break;
                      }
                    }
                    assertTrue(foundPayload);
                  }
                }
              }
            }
            assertEquals(DocsEnum.NO_MORE_DOCS, docsEnum.nextDoc());
          }
        }
      }
    }
    IOUtils.close(reader, iw, dir);
  }

  private static boolean equals(Object o1, Object o2) {
    if (o1 == null) {
      return o2 == null;
    } else {
      return o1.equals(o2);
    }
  }
}
