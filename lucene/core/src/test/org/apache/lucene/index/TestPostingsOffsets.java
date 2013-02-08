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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockPayloadAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util._TestUtil;

// TODO: we really need to test indexingoffsets, but then getting only docs / docs + freqs.
// not all codecs store prx separate...
// TODO: fix sep codec to index offsets so we can greatly reduce this list!
@SuppressCodecs({"Lucene3x", "MockFixedIntBlock", "MockVariableIntBlock", "MockSep", "MockRandom"})
public class TestPostingsOffsets extends LuceneTestCase {
  IndexWriterConfig iwc;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
  }

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();

    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    if (random().nextBoolean()) {
      ft.setStoreTermVectors(true);
      ft.setStoreTermVectorPositions(random().nextBoolean());
      ft.setStoreTermVectorOffsets(random().nextBoolean());
    }
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

    DocsAndPositionsEnum dp = MultiFields.getTermPositionsEnum(r, null, "content", new BytesRef("a"));
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

    dp = MultiFields.getTermPositionsEnum(r, null, "content", new BytesRef("b"));
    assertNotNull(dp);
    assertEquals(0, dp.nextDoc());
    assertEquals(1, dp.freq());
    assertEquals(1, dp.nextPosition());
    assertEquals(8, dp.startOffset());
    assertEquals(9, dp.endOffset());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dp.nextDoc());

    dp = MultiFields.getTermPositionsEnum(r, null, "content", new BytesRef("c"));
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
      doc.add(new StringField("id", "" + i, Field.Store.NO));
      w.addDocument(doc);
    }
    
    IndexReader reader = w.getReader();
    w.close();
    
    String terms[] = { "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "hundred" };
    
    for (String term : terms) {
      DocsAndPositionsEnum dp = MultiFields.getTermPositionsEnum(reader, null, "numbers", new BytesRef(term));
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
            assertNotNull(dp.getPayload());
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
      DocsAndPositionsEnum dp = MultiFields.getTermPositionsEnum(reader, null, "numbers", new BytesRef("hundred"));
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
          assertNotNull(dp.getPayload());
          BytesRef payload = dp.getPayload();
          assertTrue(payload.utf8ToString().startsWith("pos:"));
        } // note: withPayloads=false doesnt necessarily mean we dont have them from MockAnalyzer!
      }
    }
    
    // check that other fields (without offsets) work correctly
    
    for (int i = 0; i < numDocs; i++) {
      DocsEnum dp = MultiFields.getTermDocsEnum(reader, null, "id", new BytesRef("" + i), 0);
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

    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);

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
      doc.add(new IntField("id", docCount, Field.Store.NO));
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
    for(AtomicReaderContext ctx : r.leaves()) {
      // TODO: improve this
      AtomicReader sub = ctx.reader();
      //System.out.println("\nsub=" + sub);
      final TermsEnum termsEnum = sub.fields().terms("content").iterator(null);
      DocsEnum docs = null;
      DocsAndPositionsEnum docsAndPositions = null;
      DocsAndPositionsEnum docsAndPositionsAndOffsets = null;
      final FieldCache.Ints docIDToID = FieldCache.DEFAULT.getInts(sub, "id", false);
      for(String term : terms) {
        //System.out.println("  term=" + term);
        if (termsEnum.seekExact(new BytesRef(term), random().nextBoolean())) {
          docs = termsEnum.docs(null, docs);
          assertNotNull(docs);
          int doc;
          //System.out.println("    doc/freq");
          while((doc = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            final List<Token> expected = actualTokens.get(term).get(docIDToID.get(doc));
            //System.out.println("      doc=" + docIDToID.get(doc) + " docID=" + doc + " " + expected.size() + " freq");
            assertNotNull(expected);
            assertEquals(expected.size(), docs.freq());
          }

          // explicitly exclude offsets here
          docsAndPositions = termsEnum.docsAndPositions(null, docsAndPositions, DocsAndPositionsEnum.FLAG_PAYLOADS);
          assertNotNull(docsAndPositions);
          //System.out.println("    doc/freq/pos");
          while((doc = docsAndPositions.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            final List<Token> expected = actualTokens.get(term).get(docIDToID.get(doc));
            //System.out.println("      doc=" + docIDToID.get(doc) + " " + expected.size() + " freq");
            assertNotNull(expected);
            assertEquals(expected.size(), docsAndPositions.freq());
            for(Token token : expected) {
              int pos = Integer.parseInt(token.type());
              //System.out.println("        pos=" + pos);
              assertEquals(pos, docsAndPositions.nextPosition());
            }
          }

          docsAndPositionsAndOffsets = termsEnum.docsAndPositions(null, docsAndPositions);
          assertNotNull(docsAndPositionsAndOffsets);
          //System.out.println("    doc/freq/pos/offs");
          while((doc = docsAndPositionsAndOffsets.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            final List<Token> expected = actualTokens.get(term).get(docIDToID.get(doc));
            //System.out.println("      doc=" + docIDToID.get(doc) + " " + expected.size() + " freq");
            assertNotNull(expected);
            assertEquals(expected.size(), docsAndPositionsAndOffsets.freq());
            for(Token token : expected) {
              int pos = Integer.parseInt(token.type());
              //System.out.println("        pos=" + pos);
              assertEquals(pos, docsAndPositionsAndOffsets.nextPosition());
              assertEquals(token.startOffset(), docsAndPositionsAndOffsets.startOffset());
              assertEquals(token.endOffset(), docsAndPositionsAndOffsets.endOffset());
            }
          }
        }
      }        
      // TODO: test advance:
    }
    r.close();
    dir.close();
  }
  
  public void testWithUnindexedFields() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter riw = new RandomIndexWriter(random(), dir, iwc);
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      // ensure at least one doc is indexed with offsets
      if (i < 99 && random().nextInt(2) == 0) {
        // stored only
        FieldType ft = new FieldType();
        ft.setIndexed(false);
        ft.setStored(true);
        doc.add(new Field("foo", "boo!", ft));
      } else {
        FieldType ft = new FieldType(TextField.TYPE_STORED);
        ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        if (random().nextBoolean()) {
          // store some term vectors for the checkindex cross-check
          ft.setStoreTermVectors(true);
          ft.setStoreTermVectorPositions(true);
          ft.setStoreTermVectorOffsets(true);
        }
        doc.add(new Field("foo", "bar", ft));
      }
      riw.addDocument(doc);
    }
    CompositeReader ir = riw.getReader();
    SlowCompositeReaderWrapper slow = new SlowCompositeReaderWrapper(ir);
    FieldInfos fis = slow.getFieldInfos();
    assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, fis.fieldInfo("foo").getIndexOptions());
    slow.close();
    ir.close();
    riw.close();
    dir.close();
  }
  
  public void testAddFieldTwice() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType customType3 = new FieldType(TextField.TYPE_STORED);
    customType3.setStoreTermVectors(true);
    customType3.setStoreTermVectorPositions(true);
    customType3.setStoreTermVectorOffsets(true);    
    customType3.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    doc.add(new Field("content3", "here is more content with aaa aaa aaa", customType3));
    doc.add(new Field("content3", "here is more content with aaa aaa aaa", customType3));
    iw.addDocument(doc);
    iw.close();
    dir.close(); // checkindex
  }
  
  // NOTE: the next two tests aren't that good as we need an EvilToken...
  public void testNegativeOffsets() throws Exception {
    try {
      checkTokens(new Token[] { 
          makeToken("foo", 1, -1, -1)
      });
      fail();
    } catch (IllegalArgumentException expected) {
      //expected
    }
  }
  
  public void testIllegalOffsets() throws Exception {
    try {
      checkTokens(new Token[] { 
          makeToken("foo", 1, 1, 0)
      });
      fail();
    } catch (IllegalArgumentException expected) {
      //expected
    }
  }
   
  public void testBackwardsOffsets() throws Exception {
    try {
      checkTokens(new Token[] { 
         makeToken("foo", 1, 0, 3),
         makeToken("foo", 1, 4, 7),
         makeToken("foo", 0, 3, 6)
      });
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  public void testStackedTokens() throws Exception {
    checkTokens(new Token[] { 
        makeToken("foo", 1, 0, 3),
        makeToken("foo", 0, 0, 3),
        makeToken("foo", 0, 0, 3)
      });
  }

  public void testLegalbutVeryLargeOffsets() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    Document doc = new Document();
    Token t1 = new Token("foo", 0, Integer.MAX_VALUE-500);
    if (random().nextBoolean()) {
      t1.setPayload(new BytesRef("test"));
    }
    Token t2 = new Token("foo", Integer.MAX_VALUE-500, Integer.MAX_VALUE);
    TokenStream tokenStream = new CannedTokenStream(
        new Token[] { t1, t2 }
    );
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    // store some term vectors for the checkindex cross-check
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorPositions(true);
    ft.setStoreTermVectorOffsets(true);
    Field field = new Field("foo", tokenStream, ft);
    doc.add(field);
    iw.addDocument(doc);
    iw.close();
    dir.close();
  }
  // TODO: more tests with other possibilities
  
  private void checkTokens(Token[] tokens) throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter riw = new RandomIndexWriter(random(), dir, iwc);
    boolean success = false;
    try {
      FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
      ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
      // store some term vectors for the checkindex cross-check
      ft.setStoreTermVectors(true);
      ft.setStoreTermVectorPositions(true);
      ft.setStoreTermVectorOffsets(true);
     
      Document doc = new Document();
      doc.add(new Field("body", new CannedTokenStream(tokens), ft));
      riw.addDocument(doc);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(riw, dir);
      } else {
        IOUtils.closeWhileHandlingException(riw, dir);
      }
    }
  }

  private Token makeToken(String text, int posIncr, int startOffset, int endOffset) {
    final Token t = new Token();
    t.append(text);
    t.setPositionIncrement(posIncr);
    t.setOffset(startOffset, endOffset);
    return t;
  }
}
