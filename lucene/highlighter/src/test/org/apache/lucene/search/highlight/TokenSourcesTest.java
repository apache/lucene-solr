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
package org.apache.lucene.search.highlight;

import java.io.IOException;
import java.util.Arrays;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BaseTermVectorsFormatTestCase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;

// LUCENE-2874

/** Tests {@link org.apache.lucene.search.highlight.TokenSources} and
 *  {@link org.apache.lucene.search.highlight.TokenStreamFromTermVector}
 * indirectly from that.
 */
public class TokenSourcesTest extends BaseTokenStreamTestCase {
  private static final String FIELD = "text";

  private static final class OverlappingTokenStream extends TokenStream {
    private Token[] tokens;

    private int i = -1;

    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
    private final PositionIncrementAttribute positionIncrementAttribute = addAttribute(PositionIncrementAttribute.class);

    @Override
    public boolean incrementToken() {
      this.i++;
      if (this.i >= this.tokens.length) {
        return false;
      }
      clearAttributes();
      termAttribute.setEmpty().append(this.tokens[i]);
      offsetAttribute.setOffset(this.tokens[i].startOffset(),
          this.tokens[i].endOffset());
      positionIncrementAttribute.setPositionIncrement(this.tokens[i]
          .getPositionIncrement());
      return true;
    }

    @Override
    public void reset() {
      this.i = -1;
      this.tokens = new Token[] {
          new Token("the", 0, 3),
          new Token("{fox}", 0, 7),
          new Token("fox", 4, 7),
          new Token("did", 8, 11),
          new Token("not", 12, 15),
          new Token("jump", 16, 20)};
      this.tokens[1].setPositionIncrement(0);
    }
  }

  public void testOverlapWithOffset() throws IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox did not jump";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(null));
    try {
      final Document document = new Document();
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectors(true);
      // no positions!
      customType.setStoreTermVectorOffsets(true);
      document.add(new Field(FIELD, new OverlappingTokenStream(), customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = DirectoryReader.open(directory);
    assertEquals(1, indexReader.numDocs());
    final IndexSearcher indexSearcher = newSearcher(indexReader);
    try {
      final DisjunctionMaxQuery query = new DisjunctionMaxQuery(
          Arrays.<Query>asList(
              new SpanTermQuery(new Term(FIELD, "{fox}")),
              new SpanTermQuery(new Term(FIELD, "fox"))),
          1);
        // final Query phraseQuery = new SpanNearQuery(new SpanQuery[] {
        // new SpanTermQuery(new Term(FIELD, "{fox}")),
        // new SpanTermQuery(new Term(FIELD, "fox")) }, 0, true);

      TopDocs hits = indexSearcher.search(query, 1);
      assertEquals(1, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(query));
      final TokenStream tokenStream =
          TokenSources.getTermVectorTokenStreamOrNull(FIELD, indexReader.getTermVectors(0), -1);
      assertEquals("<B>the fox</B> did not jump",
          highlighter.getBestFragment(tokenStream, TEXT));
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  public void testOverlapWithPositionsAndOffset()
      throws IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox did not jump";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(null));
    try {
      final Document document = new Document();
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectors(true);
      customType.setStoreTermVectorOffsets(true);
      customType.setStoreTermVectorPositions(true);
      document.add(new Field(FIELD, new OverlappingTokenStream(), customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = DirectoryReader.open(directory);
    try {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      final DisjunctionMaxQuery query = new DisjunctionMaxQuery(
          Arrays.<Query>asList(
              new SpanTermQuery(new Term(FIELD, "{fox}")),
              new SpanTermQuery(new Term(FIELD, "fox"))),
          1);
      // final Query phraseQuery = new SpanNearQuery(new SpanQuery[] {
      // new SpanTermQuery(new Term(FIELD, "{fox}")),
      // new SpanTermQuery(new Term(FIELD, "fox")) }, 0, true);

      TopDocs hits = indexSearcher.search(query, 1);
      assertEquals(1, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(query));
      final TokenStream tokenStream =
          TokenSources.getTermVectorTokenStreamOrNull(FIELD, indexReader.getTermVectors(0), -1);
      assertEquals("<B>the fox</B> did not jump",
          highlighter.getBestFragment(tokenStream, TEXT));
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  public void testOverlapWithOffsetExactPhrase()
      throws IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox did not jump";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(null));
    try {
      final Document document = new Document();
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectors(true);
      // no positions!
      customType.setStoreTermVectorOffsets(true);
      document.add(new Field(FIELD, new OverlappingTokenStream(), customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = DirectoryReader.open(directory);
    try {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      // final DisjunctionMaxQuery query = new DisjunctionMaxQuery(1);
      // query.add(new SpanTermQuery(new Term(FIELD, "{fox}")));
      // query.add(new SpanTermQuery(new Term(FIELD, "fox")));
      final Query phraseQuery = new SpanNearQuery(new SpanQuery[] {
          new SpanTermQuery(new Term(FIELD, "the")),
          new SpanTermQuery(new Term(FIELD, "fox"))}, 0, true);

      TopDocs hits = indexSearcher.search(phraseQuery, 1);
      assertEquals(1, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(phraseQuery));
      final TokenStream tokenStream =
          TokenSources.getTermVectorTokenStreamOrNull(FIELD, indexReader.getTermVectors(0), -1);
      assertEquals("<B>the fox</B> did not jump",
          highlighter.getBestFragment(tokenStream, TEXT));
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  public void testOverlapWithPositionsAndOffsetExactPhrase()
      throws IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox did not jump";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(null));
    try {
      final Document document = new Document();
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectors(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectorOffsets(true);
      document.add(new Field(FIELD, new OverlappingTokenStream(), customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = DirectoryReader.open(directory);
    try {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      // final DisjunctionMaxQuery query = new DisjunctionMaxQuery(1);
      // query.add(new SpanTermQuery(new Term(FIELD, "the")));
      // query.add(new SpanTermQuery(new Term(FIELD, "fox")));
      final Query phraseQuery = new SpanNearQuery(new SpanQuery[] {
          new SpanTermQuery(new Term(FIELD, "the")),
          new SpanTermQuery(new Term(FIELD, "fox"))}, 0, true);

      TopDocs hits = indexSearcher.search(phraseQuery, 1);
      assertEquals(1, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(phraseQuery));
      final TokenStream tokenStream =
          TokenSources.getTermVectorTokenStreamOrNull(FIELD, indexReader.getTermVectors(0), -1);
      assertEquals("<B>the fox</B> did not jump",
          highlighter.getBestFragment(tokenStream, TEXT));
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  public void testTermVectorWithoutOffsetsDoesntWork()
      throws IOException, InvalidTokenOffsetsException {
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(null));
    try {
      final Document document = new Document();
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectors(true);
      customType.setStoreTermVectorOffsets(false);
      customType.setStoreTermVectorPositions(true);
      document.add(new Field(FIELD, new OverlappingTokenStream(), customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = DirectoryReader.open(directory);
    try {
      assertEquals(1, indexReader.numDocs());
      final TokenStream tokenStream =
          TokenSources.getTermVectorTokenStreamOrNull(FIELD, indexReader.getTermVectors(0), -1);
      assertNull(tokenStream);
    }
    finally {
      indexReader.close();
      directory.close();
    }
  }

  int curOffset;

  /** Just make a token with the text, and set the payload
   *  to the text as well.  Offets increment "naturally". */
  private Token getToken(String text) {
    Token t = new Token(text, curOffset, curOffset+text.length());
    t.setPayload(new BytesRef(text));
    curOffset++;
    return t;
  }

  // LUCENE-5294
  public void testPayloads() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldType myFieldType = new FieldType(TextField.TYPE_NOT_STORED);
    myFieldType.setStoreTermVectors(true);
    myFieldType.setStoreTermVectorOffsets(true);
    myFieldType.setStoreTermVectorPositions(true);
    myFieldType.setStoreTermVectorPayloads(true);

    curOffset = 0;

    Token[] tokens = new Token[] {
      getToken("foxes"),
      getToken("can"),
      getToken("jump"),
      getToken("high")
    };

    Document doc = new Document();
    doc.add(new Field("field", new CannedTokenStream(tokens), myFieldType));
    writer.addDocument(doc);
  
    IndexReader reader = writer.getReader();
    writer.close();
    assertEquals(1, reader.numDocs());

    TokenStream ts = TokenSources.getTermVectorTokenStreamOrNull("field", reader.getTermVectors(0), -1);

    CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
    PositionIncrementAttribute posIncAtt = ts.getAttribute(PositionIncrementAttribute.class);
    OffsetAttribute offsetAtt = ts.getAttribute(OffsetAttribute.class);
    PayloadAttribute payloadAtt = ts.addAttribute(PayloadAttribute.class);

    ts.reset();
    for(Token token : tokens) {
      assertTrue(ts.incrementToken());
      assertEquals(token.toString(), termAtt.toString());
      assertEquals(token.getPositionIncrement(), posIncAtt.getPositionIncrement());
      assertEquals(token.getPayload(), payloadAtt.getPayload());
      assertEquals(token.startOffset(), offsetAtt.startOffset());
      assertEquals(token.endOffset(), offsetAtt.endOffset());
    }

    assertFalse(ts.incrementToken());

    reader.close();
    dir.close();
  }

  @Repeat(iterations = 10)
  //@Seed("947083AB20AB2D4F")
  public void testRandomizedRoundTrip() throws Exception {
    final int distinct = TestUtil.nextInt(random(), 1, 10);

    String[] terms = new String[distinct];
    BytesRef[] termBytes = new BytesRef[distinct];
    for (int i = 0; i < distinct; ++i) {
      terms[i] = TestUtil.randomRealisticUnicodeString(random());
      termBytes[i] = new BytesRef(terms[i]);
    }

    final BaseTermVectorsFormatTestCase.RandomTokenStream rTokenStream =
        new BaseTermVectorsFormatTestCase.RandomTokenStream(TestUtil.nextInt(random(), 1, 10), terms, termBytes, false);
    //check to see if the token streams might have non-deterministic testable result
    final boolean storeTermVectorPositions = random().nextBoolean();
    final int[] startOffsets = rTokenStream.getStartOffsets();
    final int[] positionsIncrements = rTokenStream.getPositionsIncrements();
    for (int i = 1; i < positionsIncrements.length; i++) {
      if (storeTermVectorPositions && positionsIncrements[i] != 0) {
        continue;
      }
      //TODO should RandomTokenStream ensure endOffsets for tokens at same position and same startOffset are greater
      // than previous token's endOffset?  That would increase the testable possibilities.
      if (startOffsets[i] == startOffsets[i-1]) {
        if (VERBOSE)
          System.out.println("Skipping test because can't easily validate random token-stream is correct.");
        return;
      }
    }

    //sanity check itself
    assertTokenStreamContents(rTokenStream,
        rTokenStream.getTerms(), rTokenStream.getStartOffsets(), rTokenStream.getEndOffsets(),
        rTokenStream.getPositionsIncrements());

    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldType myFieldType = new FieldType(TextField.TYPE_NOT_STORED);
    myFieldType.setStoreTermVectors(true);
    myFieldType.setStoreTermVectorOffsets(true);
    myFieldType.setStoreTermVectorPositions(storeTermVectorPositions);
    //payloads require positions; it will throw an error otherwise
    myFieldType.setStoreTermVectorPayloads(storeTermVectorPositions && random().nextBoolean());

    Document doc = new Document();
    doc.add(new Field("field", rTokenStream, myFieldType));
    writer.addDocument(doc);

    IndexReader reader = writer.getReader();
    writer.close();
    assertEquals(1, reader.numDocs());

    TokenStream vectorTokenStream =
        TokenSources.getTermVectorTokenStreamOrNull("field", reader.getTermVectors(0), -1);

    //sometimes check payloads
    PayloadAttribute payloadAttribute = null;
    if (myFieldType.storeTermVectorPayloads() && usually()) {
      payloadAttribute = vectorTokenStream.addAttribute(PayloadAttribute.class);
    }
    assertTokenStreamContents(vectorTokenStream,
        rTokenStream.getTerms(), rTokenStream.getStartOffsets(), rTokenStream.getEndOffsets(),
        myFieldType.storeTermVectorPositions() ? rTokenStream.getPositionsIncrements() : null);
    //test payloads
    if (payloadAttribute != null) {
      vectorTokenStream.reset();
      for (int i = 0; vectorTokenStream.incrementToken(); i++) {
        assertEquals(rTokenStream.getPayloads()[i], payloadAttribute.getPayload());
      }
    }

    reader.close();
    dir.close();
  }

  public void testMaxStartOffsetConsistency() throws IOException {
    FieldType tvFieldType = new FieldType(TextField.TYPE_NOT_STORED);
    tvFieldType.setStoreTermVectors(true);
    tvFieldType.setStoreTermVectorOffsets(true);
    tvFieldType.setStoreTermVectorPositions(true);

    Directory dir = newDirectory();

    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setEnableChecks(false);//we don't necessarily consume the whole stream because of limiting by startOffset
    Document doc = new Document();
    final String TEXT = " f gg h";
    doc.add(new Field("fld_tv", analyzer.tokenStream("fooFld", TEXT), tvFieldType));
    doc.add(new TextField("fld_notv", analyzer.tokenStream("barFld", TEXT)));

    IndexReader reader;
    try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
      writer.addDocument(doc);
      reader = writer.getReader();
    }
    try {
      Fields tvFields = reader.getTermVectors(0);
      for (int maxStartOffset = -1; maxStartOffset <= TEXT.length(); maxStartOffset++) {
        TokenStream tvStream = TokenSources.getTokenStream("fld_tv", tvFields, TEXT, analyzer, maxStartOffset);
        TokenStream anaStream = TokenSources.getTokenStream("fld_notv", tvFields, TEXT, analyzer, maxStartOffset);

        //assert have same tokens, none of which has a start offset > maxStartOffset
        final OffsetAttribute tvOffAtt = tvStream.addAttribute(OffsetAttribute.class);
        final OffsetAttribute anaOffAtt = anaStream.addAttribute(OffsetAttribute.class);
        tvStream.reset();
        anaStream.reset();
        while (tvStream.incrementToken()) {
          assertTrue(anaStream.incrementToken());
          assertEquals(tvOffAtt.startOffset(), anaOffAtt.startOffset());
          if (maxStartOffset >= 0)
            assertTrue(tvOffAtt.startOffset() <= maxStartOffset);
        }
        assertTrue(anaStream.incrementToken() == false);
        tvStream.end();
        anaStream.end();
        tvStream.close();
        anaStream.close();
      }

    } finally {
      reader.close();
    }



    dir.close();
  }
}
