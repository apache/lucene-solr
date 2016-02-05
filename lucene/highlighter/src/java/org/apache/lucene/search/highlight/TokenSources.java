/*
 * Created on 28-Oct-2004
 */
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.LimitTokenOffsetFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;

/**
 * Convenience methods for obtaining a {@link TokenStream} for use with the {@link Highlighter} - can obtain from
 * term vectors with offsets and positions or from an Analyzer re-parsing the stored content.
 *
 * @see TokenStreamFromTermVector
 */
public class TokenSources {

  private TokenSources() {}

  /**
   * Get a token stream from either un-inverting a term vector if possible, or by analyzing the text.
   *
   * WARNING: Don't call this if there is more than one value for this field.  If there are, and if there are term
   * vectors, then there is a single tokenstream with offsets suggesting all the field values were concatenated.
   *
   * @param field The field to either get term vectors from or to analyze the text from.
   * @param tvFields from {@link IndexReader#getTermVectors(int)}. Possibly null. For performance, this instance should
   *                 be re-used for the same document (e.g. when highlighting multiple fields).
   * @param text the text to analyze, failing term vector un-inversion
   * @param analyzer the analyzer to analyze {@code text} with, failing term vector un-inversion
   * @param maxStartOffset Terms with a startOffset greater than this aren't returned.  Use -1 for no limit.
   *                       Suggest using {@link Highlighter#getMaxDocCharsToAnalyze()} - 1.
   *
   * @return a token stream from either term vectors, or from analyzing the text. Never null.
   */
  public static TokenStream getTokenStream(String field, Fields tvFields, String text, Analyzer analyzer,
                                           int maxStartOffset) throws IOException {
    TokenStream tokenStream = getTermVectorTokenStreamOrNull(field, tvFields, maxStartOffset);
    if (tokenStream != null) {
      return tokenStream;
    }
    tokenStream = analyzer.tokenStream(field, text);
    if (maxStartOffset >= 0 && maxStartOffset < text.length() - 1) {
      tokenStream = new LimitTokenOffsetFilter(tokenStream, maxStartOffset);
    }
    return tokenStream;
  }

  /**
   * Get a token stream by un-inverting the term vector. This method returns null if {@code tvFields} is null
   * or if the field has no term vector, or if the term vector doesn't have offsets.  Positions are recommended on the
   * term vector but it isn't strictly required.
   *
   * @param field The field to get term vectors from.
   * @param tvFields from {@link IndexReader#getTermVectors(int)}. Possibly null. For performance, this instance should
   *                 be re-used for the same document (e.g. when highlighting multiple fields).
   * @param maxStartOffset Terms with a startOffset greater than this aren't returned.  Use -1 for no limit.
   *                       Suggest using {@link Highlighter#getMaxDocCharsToAnalyze()} - 1
   * @return a token stream from term vectors. Null if no term vectors with the right options.
   */
  public static TokenStream getTermVectorTokenStreamOrNull(String field, Fields tvFields, int maxStartOffset)
      throws IOException {
    if (tvFields == null) {
      return null;
    }
    final Terms tvTerms = tvFields.terms(field);
    if (tvTerms == null || !tvTerms.hasOffsets()) {
      return null;
    }
    return new TokenStreamFromTermVector(tvTerms, maxStartOffset);
  }

  /**
   * A convenience method that tries to first get a {@link TokenStreamFromTermVector} for the
   * specified docId, then, falls back to using the passed in
   * {@link org.apache.lucene.document.Document} to retrieve the TokenStream.
   * This is useful when you already have the document, but would prefer to use
   * the vector first.
   *
   * @param reader The {@link org.apache.lucene.index.IndexReader} to use to try
   *        and get the vector from
   * @param docId The docId to retrieve.
   * @param field The field to retrieve on the document
   * @param document The document to fall back on
   * @param analyzer The analyzer to use for creating the TokenStream if the
   *        vector doesn't exist
   * @return The {@link org.apache.lucene.analysis.TokenStream} for the
   *         {@link org.apache.lucene.index.IndexableField} on the
   *         {@link org.apache.lucene.document.Document}
   * @throws IOException if there was an error loading
   */
  @Deprecated // maintenance reasons LUCENE-6445
  public static TokenStream getAnyTokenStream(IndexReader reader, int docId,
      String field, Document document, Analyzer analyzer) throws IOException {
    TokenStream ts = null;

    Fields vectors = reader.getTermVectors(docId);
    if (vectors != null) {
      Terms vector = vectors.terms(field);
      if (vector != null) {
        ts = getTokenStream(vector);
      }
    }

    // No token info stored so fall back to analyzing raw content
    if (ts == null) {
      ts = getTokenStream(document, field, analyzer);
    }
    return ts;
  }

  /**
   * A convenience method that tries a number of approaches to getting a token
   * stream. The cost of finding there are no termVectors in the index is
   * minimal (1000 invocations still registers 0 ms). So this "lazy" (flexible?)
   * approach to coding is probably acceptable
   * 
   * @return null if field not stored correctly
   * @throws IOException If there is a low-level I/O error
   */
  @Deprecated // maintenance reasons LUCENE-6445
  public static TokenStream getAnyTokenStream(IndexReader reader, int docId,
      String field, Analyzer analyzer) throws IOException {
    TokenStream ts = null;

    Fields vectors = reader.getTermVectors(docId);
    if (vectors != null) {
      Terms vector = vectors.terms(field);
      if (vector != null) {
        ts = getTokenStream(vector);
      }
    }

    // No token info stored so fall back to analyzing raw content
    if (ts == null) {
      ts = getTokenStream(reader, docId, field, analyzer);
    }
    return ts;
  }

  /** Simply calls {@link #getTokenStream(org.apache.lucene.index.Terms)} now. */
  @Deprecated // maintenance reasons LUCENE-6445
  public static TokenStream getTokenStream(Terms vector,
                                           boolean tokenPositionsGuaranteedContiguous) throws IOException {
    return getTokenStream(vector);
  }

  /**
   * Returns a token stream generated from a {@link Terms}. This
   * can be used to feed the highlighter with a pre-parsed token
   * stream.  The {@link Terms} must have offsets available. If there are no positions available,
   * all tokens will have position increments reflecting adjacent tokens, or coincident when terms
   * share a start offset. If there are stopwords filtered from the index, you probably want to ensure
   * term vectors have positions so that phrase queries won't match across stopwords.
   *
   * @throws IllegalArgumentException if no offsets are available
   */
  @Deprecated // maintenance reasons LUCENE-6445
  public static TokenStream getTokenStream(final Terms tpv) throws IOException {

    if (!tpv.hasOffsets()) {
      throw new IllegalArgumentException("Highlighting requires offsets from the TokenStream.");
      //TokenStreamFromTermVector can handle a lack of offsets if there are positions. But
      // highlighters require offsets, so we insist here.
    }

    return new TokenStreamFromTermVector(tpv, -1); // TODO propagate maxStartOffset; see LUCENE-6445
  }

  /**
   * Returns a {@link TokenStream} with positions and offsets constructed from
   * field termvectors.  If the field has no termvectors or offsets
   * are not included in the termvector, return null.  See {@link #getTokenStream(org.apache.lucene.index.Terms)}
   * for an explanation of what happens when positions aren't present.
   *
   * @param reader the {@link IndexReader} to retrieve term vectors from
   * @param docId the document to retrieve termvectors for
   * @param field the field to retrieve termvectors for
   * @return a {@link TokenStream}, or null if offsets are not available
   * @throws IOException If there is a low-level I/O error
   *
   * @see #getTokenStream(org.apache.lucene.index.Terms)
   */
  @Deprecated // maintenance reasons LUCENE-6445
  public static TokenStream getTokenStreamWithOffsets(IndexReader reader, int docId,
                                                      String field) throws IOException {

    Fields vectors = reader.getTermVectors(docId);
    if (vectors == null) {
      return null;
    }

    Terms vector = vectors.terms(field);
    if (vector == null) {
      return null;
    }

    if (!vector.hasOffsets()) {
      return null;
    }
    
    return getTokenStream(vector);
  }

  @Deprecated // maintenance reasons LUCENE-6445
  public static TokenStream getTokenStream(IndexReader reader, int docId,
      String field, Analyzer analyzer) throws IOException {
    Document doc = reader.document(docId);
    return getTokenStream(doc, field, analyzer);
  }

  @Deprecated // maintenance reasons LUCENE-6445
  public static TokenStream getTokenStream(Document doc, String field,
      Analyzer analyzer) {
    String contents = doc.get(field);
    if (contents == null) {
      throw new IllegalArgumentException("Field " + field
          + " in document is not stored and cannot be analyzed");
    }
    return getTokenStream(field, contents, analyzer);
  }

  @Deprecated // maintenance reasons LUCENE-6445
  public static TokenStream getTokenStream(String field, String contents,
      Analyzer analyzer) {
    return analyzer.tokenStream(field, contents);
  }

}
