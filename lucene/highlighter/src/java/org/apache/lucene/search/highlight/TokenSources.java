/*
 * Created on 28-Oct-2004
 */
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

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.index.Terms;

/**
 * Hides implementation issues associated with obtaining a TokenStream for use
 * with the higlighter - can obtain from TermFreqVectors with offsets and
 * (optionally) positions or from Analyzer class reparsing the stored content.
 */
public class TokenSources {
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

  public static TokenStream getAnyTokenStream(IndexReader reader, int docId,
      String field, StoredDocument document, Analyzer analyzer) throws IOException {
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
  @Deprecated
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
  public static TokenStream getTokenStream(final Terms tpv) throws IOException {

    if (!tpv.hasOffsets()) {
      throw new IllegalArgumentException("Highlighting requires offsets from the TokenStream.");
      //TokenStreamFromTermVector can handle a lack of offsets if there are positions. But
      // highlighters require offsets, so we insist here.
    }

    return new TokenStreamFromTermVector(tpv);
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

  // convenience method
  public static TokenStream getTokenStream(IndexReader reader, int docId,
      String field, Analyzer analyzer) throws IOException {
    StoredDocument doc = reader.document(docId);
    return getTokenStream(doc, field, analyzer);
  }
  
  public static TokenStream getTokenStream(StoredDocument doc, String field,
      Analyzer analyzer) {
    String contents = doc.get(field);
    if (contents == null) {
      throw new IllegalArgumentException("Field " + field
          + " in document is not stored and cannot be analyzed");
    }
    return getTokenStream(field, contents, analyzer);
  }

  // convenience method
  public static TokenStream getTokenStream(String field, String contents,
      Analyzer analyzer) {
    try {
      return analyzer.tokenStream(field, contents);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

}
