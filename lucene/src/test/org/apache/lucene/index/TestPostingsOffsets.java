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

import org.apache.lucene.analysis.CannedAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Assume;

public class TestPostingsOffsets extends LuceneTestCase {

  public void testBasic() throws Exception {

    // Currently only SimpleText can index offsets into postings:
    Assume.assumeTrue(Codec.getDefault().getName().equals("SimpleText"));

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random, dir);
    Document doc = new Document();

    FieldType ft = new FieldType(TextField.TYPE_UNSTORED);
    ft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Token[] tokens = new Token[] {
      makeToken("a", 1, 0, 6),
      makeToken("b", 1, 8, 9),
      makeToken("a", 1, 9, 17),
      makeToken("c", 1, 19, 50),
    };
    doc.add(new Field("content", new CannedAnalyzer.CannedTokenizer(tokens), ft));

    w.addDocument(doc, new CannedAnalyzer(tokens));
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
    assertEquals(DocsEnum.NO_MORE_DOCS, dp.nextDoc());

    dp = MultiFields.getTermPositionsEnum(r, null, "content", new BytesRef("b"), true);
    assertNotNull(dp);
    assertEquals(0, dp.nextDoc());
    assertEquals(1, dp.freq());
    assertEquals(1, dp.nextPosition());
    assertEquals(8, dp.startOffset());
    assertEquals(9, dp.endOffset());
    assertEquals(DocsEnum.NO_MORE_DOCS, dp.nextDoc());

    dp = MultiFields.getTermPositionsEnum(r, null, "content", new BytesRef("c"), true);
    assertNotNull(dp);
    assertEquals(0, dp.nextDoc());
    assertEquals(1, dp.freq());
    assertEquals(3, dp.nextPosition());
    assertEquals(19, dp.startOffset());
    assertEquals(50, dp.endOffset());
    assertEquals(DocsEnum.NO_MORE_DOCS, dp.nextDoc());

    r.close();
    dir.close();
  }

  public void testRandom() throws Exception {
    // Currently only SimpleText can index offsets into postings:
    Assume.assumeTrue(Codec.getDefault().getName().equals("SimpleText"));

    // token -> docID -> tokens
    final Map<String,Map<Integer,List<Token>>> actualTokens = new HashMap<String,Map<Integer,List<Token>>>();

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random, dir);

    final int numDocs = atLeast(20);
    //final int numDocs = atLeast(5);

    FieldType ft = new FieldType(TextField.TYPE_UNSTORED);

    // TODO: randomize what IndexOptions we use; also test
    // changing this up in one IW buffered segment...:
    ft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    if (random.nextBoolean()) {
      ft.setStoreTermVectors(true);
      ft.setStoreTermVectorOffsets(random.nextBoolean());
      ft.setStoreTermVectorPositions(random.nextBoolean());
    }

    for(int docCount=0;docCount<numDocs;docCount++) {
      Document doc = new Document();
      doc.add(new NumericField("id", docCount));
      List<Token> tokens = new ArrayList<Token>();
      final int numTokens = atLeast(100);
      //final int numTokens = atLeast(20);
      int pos = -1;
      int offset = 0;
      //System.out.println("doc id=" + docCount);
      for(int tokenCount=0;tokenCount<numTokens;tokenCount++) {
        final String text;
        if (random.nextBoolean()) {
          text = "a";
        } else if (random.nextBoolean()) {
          text = "b";
        } else if (random.nextBoolean()) {
          text = "c";
        } else {
          text = "d";
        }       
        
        int posIncr = random.nextBoolean() ? 1 : random.nextInt(5);
        if (tokenCount == 0 && posIncr == 0) {
          posIncr = 1;
        }
        final int offIncr = random.nextBoolean() ? 0 : random.nextInt(5);
        final int tokenOffset = random.nextInt(5);

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
      doc.add(new Field("content", new CannedAnalyzer.CannedTokenizer(tokens.toArray(new Token[tokens.size()])), ft));
      w.addDocument(doc);
    }
    final IndexReader r = w.getReader();
    w.close();

    final String[] terms = new String[] {"a", "b", "c", "d"};
    for(IndexReader sub : r.getSequentialSubReaders()) {
      //System.out.println("\nsub=" + sub);
      final TermsEnum termsEnum = sub.fields().terms("content").iterator(null);
      DocsEnum docs = null;
      DocsAndPositionsEnum docsAndPositions = null;
      DocsAndPositionsEnum docsAndPositionsAndOffsets = null;
      final int docIDToID[] = FieldCache.DEFAULT.getInts(sub, "id", false);
      for(String term : terms) {
        //System.out.println("  term=" + term);
        if (termsEnum.seekExact(new BytesRef(term), random.nextBoolean())) {
          docs = termsEnum.docs(null, docs, true);
          assertNotNull(docs);
          int doc;
          //System.out.println("    doc/freq");
          while((doc = docs.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
            final List<Token> expected = actualTokens.get(term).get(docIDToID[doc]);
            //System.out.println("      doc=" + docIDToID[doc] + " docID=" + doc + " " + expected.size() + " freq");
            assertNotNull(expected);
            assertEquals(expected.size(), docs.freq());
          }

          docsAndPositions = termsEnum.docsAndPositions(null, docsAndPositions, false);
          assertNotNull(docsAndPositions);
          //System.out.println("    doc/freq/pos");
          while((doc = docsAndPositions.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
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
          while((doc = docsAndPositions.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
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
