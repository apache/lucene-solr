package org.apache.lucene.search.highlight;

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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

// LUCENE-2874
public class TokenSourcesTest extends LuceneTestCase {
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
          new Token(new char[] {'t', 'h', 'e'}, 0, 3, 0, 3),
          new Token(new char[] {'{', 'f', 'o', 'x', '}'}, 0, 5, 0, 7),
          new Token(new char[] {'f', 'o', 'x'}, 0, 3, 4, 7),
          new Token(new char[] {'d', 'i', 'd'}, 0, 3, 8, 11),
          new Token(new char[] {'n', 'o', 't'}, 0, 3, 12, 15),
          new Token(new char[] {'j', 'u', 'm', 'p'}, 0, 4, 16, 20)};
      this.tokens[1].setPositionIncrement(0);
    }
  }

  public void testOverlapWithOffset() throws IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox did not jump";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    try {
      final Document document = new Document();
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectors(true);
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
      final DisjunctionMaxQuery query = new DisjunctionMaxQuery(1);
      query.add(new SpanTermQuery(new Term(FIELD, "{fox}")));
      query.add(new SpanTermQuery(new Term(FIELD, "fox")));
        // final Query phraseQuery = new SpanNearQuery(new SpanQuery[] {
        // new SpanTermQuery(new Term(FIELD, "{fox}")),
        // new SpanTermQuery(new Term(FIELD, "fox")) }, 0, true);

      TopDocs hits = indexSearcher.search(query, 1);
      assertEquals(1, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(query));
      final TokenStream tokenStream = TokenSources
          .getTokenStream(
              indexReader.getTermVector(0, FIELD),
              false);
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
        newIndexWriterConfig(TEST_VERSION_CURRENT, null));
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
      final DisjunctionMaxQuery query = new DisjunctionMaxQuery(1);
      query.add(new SpanTermQuery(new Term(FIELD, "{fox}")));
      query.add(new SpanTermQuery(new Term(FIELD, "fox")));
      // final Query phraseQuery = new SpanNearQuery(new SpanQuery[] {
      // new SpanTermQuery(new Term(FIELD, "{fox}")),
      // new SpanTermQuery(new Term(FIELD, "fox")) }, 0, true);

      TopDocs hits = indexSearcher.search(query, 1);
      assertEquals(1, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(query));
      final TokenStream tokenStream = TokenSources
          .getTokenStream(
              indexReader.getTermVector(0, FIELD),
              false);
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
        newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    try {
      final Document document = new Document();
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectors(true);
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
      final TokenStream tokenStream = TokenSources
          .getTokenStream(
              indexReader.getTermVector(0, FIELD),
              false);
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
        newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    try {
      final Document document = new Document();
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectors(true);
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
      final TokenStream tokenStream = TokenSources
          .getTokenStream(
              indexReader.getTermVector(0, FIELD),
              false);
      assertEquals("<B>the fox</B> did not jump",
          highlighter.getBestFragment(tokenStream, TEXT));
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  public void testTermVectorWithoutOffsetsThrowsException()
      throws IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox did not jump";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, null));
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
      final TokenStream tokenStream = TokenSources
          .getTokenStream(
              indexReader.getTermVector(0, FIELD),
              false);
      fail("TokenSources.getTokenStream should throw IllegalArgumentException if term vector has no offsets");
    }
    catch (IllegalArgumentException e) {
      // expected
    }
    finally {
      indexReader.close();
      directory.close();
    }
  }


}
