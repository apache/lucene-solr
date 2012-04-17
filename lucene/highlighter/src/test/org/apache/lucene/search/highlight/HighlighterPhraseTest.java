package org.apache.lucene.search.highlight;

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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;

public class HighlighterPhraseTest extends LuceneTestCase {
  private static final String FIELD = "text";
  public void testConcurrentPhrase() throws CorruptIndexException,
      LockObtainFailedException, IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox jumped";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    try {
      final Document document = new Document();
      FieldType customType = new FieldType(TextField.TYPE_UNSTORED);
      customType.setStoreTermVectorOffsets(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectors(true);
      document.add(new Field(FIELD, new TokenStreamConcurrent(), customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = IndexReader.open(directory);
    try {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      final PhraseQuery phraseQuery = new PhraseQuery();
      phraseQuery.add(new Term(FIELD, "fox"));
      phraseQuery.add(new Term(FIELD, "jumped"));
      phraseQuery.setSlop(0);
      TopDocs hits = indexSearcher.search(phraseQuery, 1);
      assertEquals(1, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(phraseQuery));

      final TokenStream tokenStream = TokenSources
          .getTokenStream(indexReader.getTermVector(
              0, FIELD), false);
      assertEquals(highlighter.getBestFragment(new TokenStreamConcurrent(),
          TEXT), highlighter.getBestFragment(tokenStream, TEXT));
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  public void testConcurrentSpan() throws CorruptIndexException,
      LockObtainFailedException, IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox jumped";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    try {
      final Document document = new Document();

      FieldType customType = new FieldType(TextField.TYPE_UNSTORED);
      customType.setStoreTermVectorOffsets(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectors(true);
      document.add(new Field(FIELD, new TokenStreamConcurrent(), customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = IndexReader.open(directory);
    try {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      final Query phraseQuery = new SpanNearQuery(new SpanQuery[] {
          new SpanTermQuery(new Term(FIELD, "fox")),
          new SpanTermQuery(new Term(FIELD, "jumped")) }, 0, true);
      final FixedBitSet bitset = new FixedBitSet(indexReader.maxDoc());
      indexSearcher.search(phraseQuery, new Collector() {
        private int baseDoc;

        @Override
        public boolean acceptsDocsOutOfOrder() {
          return true;
        }

        @Override
        public void collect(int i) throws IOException {
          bitset.set(this.baseDoc + i);
        }

        @Override
        public void setNextReader(AtomicReaderContext context)
            throws IOException {
          this.baseDoc = context.docBase;
        }

        @Override
        public void setScorer(org.apache.lucene.search.Scorer scorer)
            throws IOException {
          // Do Nothing
        }
      });
      assertEquals(1, bitset.cardinality());
      final int maxDoc = indexReader.maxDoc();
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(phraseQuery));
      for (int position = bitset.nextSetBit(0); position >= 0 && position < maxDoc-1; position = bitset
          .nextSetBit(position + 1)) {
        assertEquals(0, position);
        final TokenStream tokenStream = TokenSources.getTokenStream(
            indexReader.getTermVector(position,
                FIELD), false);
        assertEquals(highlighter.getBestFragment(new TokenStreamConcurrent(),
            TEXT), highlighter.getBestFragment(tokenStream, TEXT));
      }
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  public void testSparsePhrase() throws CorruptIndexException,
      LockObtainFailedException, IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox did not jump";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    try {
      final Document document = new Document();

      FieldType customType = new FieldType(TextField.TYPE_UNSTORED);
      customType.setStoreTermVectorOffsets(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectors(true);
      document.add(new Field(FIELD, new TokenStreamSparse(), customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = IndexReader.open(directory);
    try {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      final PhraseQuery phraseQuery = new PhraseQuery();
      phraseQuery.add(new Term(FIELD, "did"));
      phraseQuery.add(new Term(FIELD, "jump"));
      phraseQuery.setSlop(0);
      TopDocs hits = indexSearcher.search(phraseQuery, 1);
      assertEquals(0, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(phraseQuery));
      final TokenStream tokenStream = TokenSources
          .getTokenStream(indexReader.getTermVector(
              0, FIELD), false);
      assertEquals(
          highlighter.getBestFragment(new TokenStreamSparse(), TEXT),
          highlighter.getBestFragment(tokenStream, TEXT));
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  public void testSparsePhraseWithNoPositions() throws CorruptIndexException,
      LockObtainFailedException, IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox did not jump";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    try {
      final Document document = new Document();

      FieldType customType = new FieldType(TextField.TYPE_STORED);
      customType.setStoreTermVectorOffsets(true);
      customType.setStoreTermVectors(true);
      document.add(new Field(FIELD, TEXT, customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = IndexReader.open(directory);
    try {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      final PhraseQuery phraseQuery = new PhraseQuery();
      phraseQuery.add(new Term(FIELD, "did"));
      phraseQuery.add(new Term(FIELD, "jump"));
      phraseQuery.setSlop(1);
      TopDocs hits = indexSearcher.search(phraseQuery, 1);
      assertEquals(1, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(phraseQuery));
      final TokenStream tokenStream = TokenSources.getTokenStream(
          indexReader.getTermVector(0, FIELD), true);
      assertEquals("the fox <B>did</B> not <B>jump</B>", highlighter
          .getBestFragment(tokenStream, TEXT));
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  public void testSparseSpan() throws CorruptIndexException,
      LockObtainFailedException, IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox did not jump";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    try {
      final Document document = new Document();
      FieldType customType = new FieldType(TextField.TYPE_UNSTORED);
      customType.setStoreTermVectorOffsets(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectors(true);
      document.add(new Field(FIELD, new TokenStreamSparse(), customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = IndexReader.open(directory);
    try {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      final Query phraseQuery = new SpanNearQuery(new SpanQuery[] {
          new SpanTermQuery(new Term(FIELD, "did")),
          new SpanTermQuery(new Term(FIELD, "jump")) }, 0, true);

      TopDocs hits = indexSearcher.search(phraseQuery, 1);
      assertEquals(0, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(phraseQuery));
      final TokenStream tokenStream = TokenSources
          .getTokenStream(indexReader.getTermVector(
              0, FIELD), false);
      assertEquals(
          highlighter.getBestFragment(new TokenStreamSparse(), TEXT),
          highlighter.getBestFragment(tokenStream, TEXT));
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  private static final class TokenStreamSparse extends TokenStream {
    private Token[] tokens;

    private int i = -1;

    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
    private final PositionIncrementAttribute positionIncrementAttribute = addAttribute(PositionIncrementAttribute.class);

    public TokenStreamSparse() {
      reset();
    }

    @Override
    public boolean incrementToken() throws IOException {
      this.i++;
      if (this.i >= this.tokens.length) {
        return false;
      }
      clearAttributes();
      termAttribute.setEmpty().append(this.tokens[i]);
      offsetAttribute.setOffset(this.tokens[i].startOffset(), this.tokens[i]
          .endOffset());
      positionIncrementAttribute.setPositionIncrement(this.tokens[i]
          .getPositionIncrement());
      return true;
    }

    @Override
    public void reset() {
      this.i = -1;
      this.tokens = new Token[] {
          new Token(new char[] { 't', 'h', 'e' }, 0, 3, 0, 3),
          new Token(new char[] { 'f', 'o', 'x' }, 0, 3, 4, 7),
          new Token(new char[] { 'd', 'i', 'd' }, 0, 3, 8, 11),
          new Token(new char[] { 'j', 'u', 'm', 'p' }, 0, 4, 16, 20) };
      this.tokens[3].setPositionIncrement(2);
    }
  }

  private static final class TokenStreamConcurrent extends TokenStream {
    private Token[] tokens;

    private int i = -1;

    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
    private final PositionIncrementAttribute positionIncrementAttribute = addAttribute(PositionIncrementAttribute.class);

    public TokenStreamConcurrent() {
      reset();
    }

    @Override
    public boolean incrementToken() throws IOException {
      this.i++;
      if (this.i >= this.tokens.length) {
        return false;
      }
      clearAttributes();
      termAttribute.setEmpty().append(this.tokens[i]);
      offsetAttribute.setOffset(this.tokens[i].startOffset(), this.tokens[i]
          .endOffset());
      positionIncrementAttribute.setPositionIncrement(this.tokens[i]
          .getPositionIncrement());
      return true;
    }

    @Override
    public void reset() {
      this.i = -1;
      this.tokens = new Token[] {
          new Token(new char[] { 't', 'h', 'e' }, 0, 3, 0, 3),
          new Token(new char[] { 'f', 'o', 'x' }, 0, 3, 4, 7),
          new Token(new char[] { 'j', 'u', 'm', 'p' }, 0, 4, 8, 14),
          new Token(new char[] { 'j', 'u', 'm', 'p', 'e', 'd' }, 0, 6, 8, 14) };
      this.tokens[3].setPositionIncrement(0);
    }
  }

}
