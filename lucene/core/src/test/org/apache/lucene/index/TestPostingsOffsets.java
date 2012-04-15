package org.apache.lucene.index;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockPayloadAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

// TODO: we really need to test indexingoffsets, but then getting only docs / docs + freqs.
// not all codecs store prx separate...
public class TestPostingsOffsets extends LuceneTestCase {
  IndexWriterConfig iwc;
  
  public void setUp() throws Exception {
    super.setUp();
    // Currently only SimpleText and Lucene40 can index offsets into postings:
    assumeTrue("codec does not support offsets", Codec.getDefault().getName().equals("SimpleText") || Codec.getDefault().getName().equals("Lucene40"));
    iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    
    if (Codec.getDefault().getName().equals("Lucene40")) {
      // pulsing etc are not implemented
      if (random().nextBoolean()) {
        iwc.setCodec(_TestUtil.alwaysPostingsFormat(new Lucene40PostingsFormat()));
      } else {
        iwc.setCodec(_TestUtil.alwaysPostingsFormat(new MemoryPostingsFormat()));
      }
    }
  }

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();

    FieldType ft = new FieldType(TextField.TYPE_UNSTORED);
    ft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Token[] tokens = new Token[] {
      makeToken("a", 1, 0, 6),
      makeToken("b", 1, 8, 9),
      makeToken("a", 1, 9, 17),
      makeToken("c", 1, 19, 50),
    };
    doc.add(new Field("content", new CannedTokenStream(tokens), ft));

    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    DocsAndPositionsEnum dp = MultiFields.getTermPositionsEnum(r, null, "content", new BytesRef("a"), true);
    assertNotNull(dp);
    assertEquals(0, dp.nextDoc());
    assertEquals(2, dp.freq());
    assertEquals(0, dp.nextPosition());
    assertEquals(0, dp.startOffset());
    assertEquals(6, dp.endOffset());
    assertEquals(2, dp.nextPosition());
    assertEquals(9, dp.startOffset());
    assertEquals(17, dp.endOffset());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dp.nextDoc());

    dp = MultiFields.getTermPositionsEnum(r, null, "content", new BytesRef("b"), true);
    assertNotNull(dp);
    assertEquals(0, dp.nextDoc());
    assertEquals(1, dp.freq());
    assertEquals(1, dp.nextPosition());
    assertEquals(8, dp.startOffset());
    assertEquals(9, dp.endOffset());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dp.nextDoc());

    dp = MultiFields.getTermPositionsEnum(r, null, "content", new BytesRef("c"), true);
    assertNotNull(dp);
    assertEquals(0, dp.nextDoc());
    assertEquals(1, dp.freq());
    assertEquals(3, dp.nextPosition());
    assertEquals(19, dp.startOffset());
    assertEquals(50, dp.endOffset());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dp.nextDoc());

    r.close();
    dir.close();
  }
  
  public void testSkipping() throws Exception {
    doTestNumbers(false);
  }
  
  public void testPayloads() throws Exception {
    doTestNumbers(true);
  }
  
  public void doTestNumbers(boolean withPayloads) throws Exception {
    Directory dir = newDirectory();
    Analyzer analyzer = withPayloads ? new MockPayloadAnalyzer() : new MockAnalyzer(random());
    iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    if (Codec.getDefault().getName().equals("Lucene40")) {
      // pulsing etc are not implemented
      if (random().nextBoolean()) {
        iwc.setCodec(_TestUtil.alwaysPostingsFormat(new Lucene40PostingsFormat()));
      } else {
        iwc.setCodec(_TestUtil.alwaysPostingsFormat(new MemoryPostingsFormat()));
      }
    }
    iwc.setMergePolicy(newLogMergePolicy()); // will rely on docids a bit for skipping
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    if (random().nextBoolean()) {
      ft.setStoreTermVectors(true);
      ft.setStoreTermVectorOffsets(random().nextBoolean());
      ft.setStoreTermVectorPositions(random().nextBoolean());
    }
    
    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new Field("numbers", English.intToEnglish(i), ft));
      doc.add(new Field("oddeven", (i % 2) == 0 ? "even" : "odd", ft));
      doc.add(new StringField("id", "" + i));
      w.addDocument(doc);
    }
    
    IndexReader reader = w.getReader();
    w.close();
    
    String terms[] = { "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "hundred" };
    
    for (String term : terms) {
      DocsAndPositionsEnum dp = MultiFields.getTermPositionsEnum(reader, null, "numbers", new BytesRef(term), true);
      int doc;
      while((doc = dp.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        String storedNumbers = reader.document(doc).get("numbers");
        int freq = dp.freq();
        for (int i = 0; i < freq; i++) {
          dp.nextPosition();
          int start = dp.startOffset();
          assert start >= 0;
          int end = dp.endOffset();
          assert end >= 0 && end >= start;
          // check that the offsets correspond to the term in the src text
          assertTrue(storedNumbers.substring(start, end).equals(term));
          if (withPayloads) {
            // check that we have a payload and it starts with "pos"
            assertTrue(dp.hasPayload());
            BytesRef payload = dp.getPayload();
            assertTrue(payload.utf8ToString().startsWith("pos:"));
          } // note: withPayloads=false doesnt necessarily mean we dont have them from MockAnalyzer!
        }
      }
    }
    
    // check we can skip correctly
    int numSkippingTests = atLeast(50);
    
    for (int j = 0; j < numSkippingTests; j++) {
      int num = _TestUtil.nextInt(random(), 100, Math.min(numDocs-1, 999));
      DocsAndPositionsEnum dp = MultiFields.getTermPositionsEnum(reader, null, "numbers", new BytesRef("hundred"), true);
      int doc = dp.advance(num);
      assertEquals(num, doc);
      int freq = dp.freq();
      for (int i = 0; i < freq; i++) {
        String storedNumbers = reader.document(doc).get("numbers");
        dp.nextPosition();
        int start = dp.startOffset();
        assert start >= 0;
        int end = dp.endOffset();
        assert end >= 0 && end >= start;
        // check that the offsets correspond to the term in the src text
        assertTrue(storedNumbers.substring(start, end).equals("hundred"));
        if (withPayloads) {
          // check that we have a payload and it starts with "pos"
          assertTrue(dp.hasPayload());
          BytesRef payload = dp.getPayload();
          assertTrue(payload.utf8ToString().startsWith("pos:"));
        } // note: withPayloads=false doesnt necessarily mean we dont have them from MockAnalyzer!
      }
    }
    
    // check that other fields (without offsets) work correctly
    
    for (int i = 0; i < numDocs; i++) {
      DocsEnum dp = MultiFields.getTermDocsEnum(reader, null, "id", new BytesRef("" + i), false);
      assertEquals(i, dp.nextDoc());
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, dp.nextDoc());
    }
    
    reader.close();
    dir.close();
  }

  public void testRandom() throws Exception {
    // token -> docID -> tokens
    final Map<String,Map<Integer,List<Token>>> actualTokens = new HashMap<String,Map<Integer,List<Token>>>();

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    final int numDocs = atLeast(20);
    //final int numDocs = atLeast(5);

    FieldType ft = new FieldType(TextField.TYPE_UNSTORED);

    // TODO: randomize what IndexOptions we use; also test
    // changing this up in one IW buffered segment...:
    ft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    if (random().nextBoolean()) {
      ft.setStoreTermVectors(true);
      ft.setStoreTermVectorOffsets(random().nextBoolean());
      ft.setStoreTermVectorPositions(random().nextBoolean());
    }

    for(int docCount=0;docCount<numDocs;docCount++) {
      Document doc = new Document();
      doc.add(new IntField("id", docCount));
      List<Token> tokens = new ArrayList<Token>();
      final int numTokens = atLeast(100);
      //final int numTokens = atLeast(20);
      int pos = -1;
      int offset = 0;
      //System.out.println("doc id=" + docCount);
      for(int tokenCount=0;tokenCount<numTokens;tokenCount++) {
        final String text;
        if (random().nextBoolean()) {
          text = "a";
        } else if (random().nextBoolean()) {
          text = "b";
        } else if (random().nextBoolean()) {
          text = "c";
        } else {
          text = "d";
        }       
        
        int posIncr = random().nextBoolean() ? 1 : random().nextInt(5);
        if (tokenCount == 0 && posIncr == 0) {
          posIncr = 1;
        }
        final int offIncr = random().nextBoolean() ? 0 : random().nextInt(5);
        final int tokenOffset = random().nextInt(5);

        final Token token = makeToken(text, posIncr, offset+offIncr, offset+offIncr+tokenOffset);
        if (!actualTokens.containsKey(text)) {
          actualTokens.put(text, new HashMap<Integer,List<Token>>());
        }
        final Map<Integer,List<Token>> postingsByDoc = actualTokens.get(text);
        if (!postingsByDoc.containsKey(docCount)) {
          postingsByDoc.put(docCount, new ArrayList<Token>());
        }
        postingsByDoc.get(docCount).add(token);
        tokens.add(token);
        pos += posIncr;
        // stuff abs position into type:
        token.setType(""+pos);
        offset += offIncr + tokenOffset;
        //System.out.println("  " + token + " posIncr=" + token.getPositionIncrement() + " pos=" + pos + " off=" + token.startOffset() + "/" + token.endOffset() + " (freq=" + postingsByDoc.get(docCount).size() + ")");
      }
      doc.add(new Field("content", new CannedTokenStream(tokens.toArray(new Token[tokens.size()])), ft));
      w.addDocument(doc);
    }
    final DirectoryReader r = w.getReader();
    w.close();

    final String[] terms = new String[] {"a", "b", "c", "d"};
    for(IndexReader reader : r.getSequentialSubReaders()) {
      // TODO: improve this
      AtomicReader sub = (AtomicReader) reader;
      //System.out.println("\nsub=" + sub);
      final TermsEnum termsEnum = sub.fields().terms("content").iterator(null);
      DocsEnum docs = null;
      DocsAndPositionsEnum docsAndPositions = null;
      DocsAndPositionsEnum docsAndPositionsAndOffsets = null;
      final int docIDToID[] = FieldCache.DEFAULT.getInts(sub, "id", false);
      for(String term : terms) {
        //System.out.println("  term=" + term);
        if (termsEnum.seekExact(new BytesRef(term), random().nextBoolean())) {
          docs = termsEnum.docs(null, docs, true);
          assertNotNull(docs);
          int doc;
          //System.out.println("    doc/freq");
          while((doc = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            final List<Token> expected = actualTokens.get(term).get(docIDToID[doc]);
            //System.out.println("      doc=" + docIDToID[doc] + " docID=" + doc + " " + expected.size() + " freq");
            assertNotNull(expected);
            assertEquals(expected.size(), docs.freq());
          }

          docsAndPositions = termsEnum.docsAndPositions(null, docsAndPositions, false);
          assertNotNull(docsAndPositions);
          //System.out.println("    doc/freq/pos");
          while((doc = docsAndPositions.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            final List<Token> expected = actualTokens.get(term).get(docIDToID[doc]);
            //System.out.println("      doc=" + docIDToID[doc] + " " + expected.size() + " freq");
            assertNotNull(expected);
            assertEquals(expected.size(), docsAndPositions.freq());
            for(Token token : expected) {
              int pos = Integer.parseInt(token.type());
              //System.out.println("        pos=" + pos);
              assertEquals(pos, docsAndPositions.nextPosition());
            }
          }

          docsAndPositionsAndOffsets = termsEnum.docsAndPositions(null, docsAndPositions, true);
          assertNotNull(docsAndPositionsAndOffsets);
          //System.out.println("    doc/freq/pos/offs");
          while((doc = docsAndPositions.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            final List<Token> expected = actualTokens.get(term).get(docIDToID[doc]);
            //System.out.println("      doc=" + docIDToID[doc] + " " + expected.size() + " freq");
            assertNotNull(expected);
            assertEquals(expected.size(), docsAndPositions.freq());
            for(Token token : expected) {
              int pos = Integer.parseInt(token.type());
              //System.out.println("        pos=" + pos);
              assertEquals(pos, docsAndPositions.nextPosition());
              assertEquals(token.startOffset(), docsAndPositions.startOffset());
              assertEquals(token.endOffset(), docsAndPositions.endOffset());
            }
          }
        }
      }        
      // TODO: test advance:
    }
    r.close();
    dir.close();
  }

  private Token makeToken(String text, int posIncr, int startOffset, int endOffset) {
    final Token t = new Token();
    t.append(text);
    t.setPositionIncrement(posIncr);
    t.setOffset(startOffset, endOffset);
    return t;
  }
}
