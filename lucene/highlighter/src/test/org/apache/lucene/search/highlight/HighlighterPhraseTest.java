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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;

public class HighlighterPhraseTest extends LuceneTestCase {
  private static final String FIELD = "text";

  public void testConcurrentPhrase() throws IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox jumped";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    try {
      final Document document = new Document();
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectorOffsets(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectors(true);
      document.add(new Field(FIELD, new TokenStreamConcurrent(), customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = DirectoryReader.open(directory);
    try {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      final PhraseQuery phraseQuery = new PhraseQuery(FIELD, "fox", "jumped");
      TopDocs hits = indexSearcher.search(phraseQuery, 1);
      assertEquals(1, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(phraseQuery));

      final TokenStream tokenStream =
          TokenSources.getTermVectorTokenStreamOrNull(FIELD, indexReader.getTermVectors(0), -1);
      assertEquals(highlighter.getBestFragment(new TokenStreamConcurrent(),
          TEXT), highlighter.getBestFragment(tokenStream, TEXT));
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  public void testConcurrentSpan() throws IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox jumped";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    try {
      final Document document = new Document();

      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectorOffsets(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectors(true);
      document.add(new Field(FIELD, new TokenStreamConcurrent(), customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = DirectoryReader.open(directory);
    try {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      final Query phraseQuery = new SpanNearQuery(new SpanQuery[] {
          new SpanTermQuery(new Term(FIELD, "fox")),
          new SpanTermQuery(new Term(FIELD, "jumped")) }, 0, true);
      final FixedBitSet bitset = new FixedBitSet(indexReader.maxDoc());
      indexSearcher.search(phraseQuery, new SimpleCollector() {
        private int baseDoc;

        @Override
        public void collect(int i) {
          bitset.set(this.baseDoc + i);
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) throws IOException {
          this.baseDoc = context.docBase;
        }

        @Override
        public void setScorer(org.apache.lucene.search.Scorer scorer) {
          // Do Nothing
        }

        @Override
        public boolean needsScores() {
          return false;
        }
      });
      assertEquals(1, bitset.cardinality());
      final int maxDoc = indexReader.maxDoc();
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(phraseQuery));
      for (int position = bitset.nextSetBit(0); position < maxDoc-1; position = bitset
          .nextSetBit(position + 1)) {
        assertEquals(0, position);
        final TokenStream tokenStream =
            TokenSources.getTermVectorTokenStreamOrNull(FIELD, indexReader.getTermVectors(position), -1);
        assertEquals(highlighter.getBestFragment(new TokenStreamConcurrent(),
            TEXT), highlighter.getBestFragment(tokenStream, TEXT));
      }
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  public void testSparsePhrase() throws IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox did not jump";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    try {
      final Document document = new Document();

      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectorOffsets(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectors(true);
      document.add(new Field(FIELD, new TokenStreamSparse(), customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = DirectoryReader.open(directory);
    try {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      final PhraseQuery phraseQuery = new PhraseQuery(FIELD, "did", "jump");
      TopDocs hits = indexSearcher.search(phraseQuery, 1);
      assertEquals(0, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(phraseQuery));
      final TokenStream tokenStream =
          TokenSources.getTermVectorTokenStreamOrNull(FIELD, indexReader.getTermVectors(0), -1);
      assertEquals(
          highlighter.getBestFragment(new TokenStreamSparse(), TEXT),
          highlighter.getBestFragment(tokenStream, TEXT));
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  public void testSparsePhraseWithNoPositions() throws IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox did not jump";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
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
    final IndexReader indexReader = DirectoryReader.open(directory);
    try {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      final PhraseQuery phraseQuery = new PhraseQuery(1, FIELD, "did", "jump");
      TopDocs hits = indexSearcher.search(phraseQuery, 1);
      assertEquals(1, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(phraseQuery));
      final TokenStream tokenStream =
          TokenSources.getTermVectorTokenStreamOrNull(FIELD, indexReader.getTermVectors(0), -1);
      assertEquals("the fox <B>did</B> not <B>jump</B>", highlighter
          .getBestFragment(tokenStream, TEXT));
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  public void testSparseSpan() throws IOException, InvalidTokenOffsetsException {
    final String TEXT = "the fox did not jump";
    final Directory directory = newDirectory();
    final IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    try {
      final Document document = new Document();
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectorOffsets(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectors(true);
      document.add(new Field(FIELD, new TokenStreamSparse(), customType));
      indexWriter.addDocument(document);
    } finally {
      indexWriter.close();
    }
    final IndexReader indexReader = DirectoryReader.open(directory);
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
      final TokenStream tokenStream =
          TokenSources.getTermVectorTokenStreamOrNull(FIELD, indexReader.getTermVectors(0), -1);
      assertEquals(
          highlighter.getBestFragment(new TokenStreamSparse(), TEXT),
          highlighter.getBestFragment(tokenStream, TEXT));
    } finally {
      indexReader.close();
      directory.close();
    }
  }

  //shows the need to sum the increments in WeightedSpanTermExtractor
  public void testStopWords() throws IOException, InvalidTokenOffsetsException {
    MockAnalyzer stopAnalyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true,
        MockTokenFilter.ENGLISH_STOPSET);    
    final String TEXT = "the ab the the cd the the the ef the";
    final Directory directory = newDirectory();
    try (IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(stopAnalyzer))) {
      final Document document = new Document();
      document.add(newTextField(FIELD, TEXT, Store.YES));
      indexWriter.addDocument(document);
    }
    try (IndexReader indexReader = DirectoryReader.open(directory)) {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      //equivalent of "ab the the cd the the the ef"
      final PhraseQuery phraseQuery = new PhraseQuery.Builder()
          .add(new Term(FIELD, "ab"), 0)
          .add(new Term(FIELD, "cd"), 3)
          .add(new Term(FIELD, "ef"), 7).build();

      TopDocs hits = indexSearcher.search(phraseQuery, 100);
      assertEquals(1, hits.totalHits);
      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(phraseQuery));
      assertEquals(1, highlighter.getBestFragments(stopAnalyzer, FIELD, TEXT, 10).length);
    } finally {
      directory.close();
    }
  }
  
  //shows the need to require inOrder if getSlop() == 0, not if final slop == 0 
  //in WeightedSpanTermExtractor
  public void testInOrderWithStopWords() throws IOException, InvalidTokenOffsetsException {
    MockAnalyzer stopAnalyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true,
        MockTokenFilter.ENGLISH_STOPSET);        
    final String TEXT = "the cd the ab the the the the the the the ab the cd the";
    final Directory directory = newDirectory();
    try (IndexWriter indexWriter = new IndexWriter(directory,
        newIndexWriterConfig(stopAnalyzer))) {
      final Document document = new Document();
      document.add(newTextField(FIELD, TEXT, Store.YES));
      indexWriter.addDocument(document);
    }
    try (IndexReader indexReader = DirectoryReader.open(directory)) {
      assertEquals(1, indexReader.numDocs());
      final IndexSearcher indexSearcher = newSearcher(indexReader);
      //equivalent of "ab the cd"
      final PhraseQuery phraseQuery = new PhraseQuery.Builder()
          .add(new Term(FIELD, "ab"), 0)
          .add(new Term(FIELD, "cd"), 2).build();

      TopDocs hits = indexSearcher.search(phraseQuery, 100);
      assertEquals(1, hits.totalHits);

      final Highlighter highlighter = new Highlighter(
          new SimpleHTMLFormatter(), new SimpleHTMLEncoder(),
          new QueryScorer(phraseQuery));
      String[] frags = highlighter.getBestFragments(stopAnalyzer, FIELD, TEXT, 10);
      assertEquals(1, frags.length);
      assertTrue("contains <B>ab</B> the <B>cd</B>",
          (frags[0].contains("<B>ab</B> the <B>cd</B>")));
      assertTrue("does not contain <B>cd</B> the <B>ab</B>",
          (!frags[0].contains("<B>cd</B> the <B>ab</B>")));
    } finally {
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
    public boolean incrementToken() {
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
          new Token("the", 0, 3),
          new Token("fox", 4, 7),
          new Token("did", 8, 11),
          new Token("jump", 16, 20) };
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
    public boolean incrementToken() {
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
          new Token("the", 0, 3),
          new Token("fox", 4, 7),
          new Token("jump", 8, 14),
          new Token("jumped", 8, 14) };
      this.tokens[3].setPositionIncrement(0);
    }
  }

}
