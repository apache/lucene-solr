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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Hides implementation issues associated with obtaining a TokenStream for use
 * with the higlighter - can obtain from TermFreqVectors with offsets and
 * (optionally) positions or from Analyzer class reparsing the stored content.
 */
public class TokenSources {
  /**
   * A convenience method that tries to first get a TermPositionVector for the
   * specified docId, then, falls back to using the passed in
   * {@link org.apache.lucene.document.Document} to retrieve the TokenStream.
   * This is useful when you already have the document, but would prefer to use
   * the vector first.
   * 
   * @param reader The {@link org.apache.lucene.index.IndexReader} to use to try
   *        and get the vector from
   * @param docId The docId to retrieve.
   * @param field The field to retrieve on the document
   * @param doc The document to fall back on
   * @param analyzer The analyzer to use for creating the TokenStream if the
   *        vector doesn't exist
   * @return The {@link org.apache.lucene.analysis.TokenStream} for the
   *         {@link org.apache.lucene.index.IndexableField} on the
   *         {@link org.apache.lucene.document.Document}
   * @throws IOException if there was an error loading
   */

  public static TokenStream getAnyTokenStream(IndexReader reader, int docId,
      String field, Document doc, Analyzer analyzer) throws IOException {
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
      ts = getTokenStream(doc, field, analyzer);
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

  public static TokenStream getTokenStream(Terms vector) throws IOException {
    // assumes the worst and makes no assumptions about token position
    // sequences.
    return getTokenStream(vector, false);
  }

  /**
   * Low level api. Returns a token stream generated from a {@link Terms}. This
   * can be used to feed the highlighter with a pre-parsed token
   * stream.  The {@link Terms} must have offsets available.
   * 
   * In my tests the speeds to recreate 1000 token streams using this method
   * are: - with TermVector offset only data stored - 420 milliseconds - with
   * TermVector offset AND position data stored - 271 milliseconds (nb timings
   * for TermVector with position data are based on a tokenizer with contiguous
   * positions - no overlaps or gaps) The cost of not using TermPositionVector
   * to store pre-parsed content and using an analyzer to re-parse the original
   * content: - reanalyzing the original content - 980 milliseconds
   * 
   * The re-analyze timings will typically vary depending on - 1) The complexity
   * of the analyzer code (timings above were using a
   * stemmer/lowercaser/stopword combo) 2) The number of other fields (Lucene
   * reads ALL fields off the disk when accessing just one document field - can
   * cost dear!) 3) Use of compression on field storage - could be faster due to
   * compression (less disk IO) or slower (more CPU burn) depending on the
   * content.
   * 
   * @param tokenPositionsGuaranteedContiguous true if the token position
   *        numbers have no overlaps or gaps. If looking to eek out the last
   *        drops of performance, set to true. If in doubt, set to false.
   *
   * @throws IllegalArgumentException if no offsets are available
   */
  public static TokenStream getTokenStream(Terms tpv,
      boolean tokenPositionsGuaranteedContiguous) 
  throws IOException {

    if (!tpv.hasOffsets()) {
      throw new IllegalArgumentException("Cannot create TokenStream from Terms without offsets");
    }

    if (!tokenPositionsGuaranteedContiguous && tpv.hasPositions()) {
      return new TokenStreamFromTermPositionVector(tpv);
    }

    // an object used to iterate across an array of tokens
    final class StoredTokenStream extends TokenStream {
      Token tokens[];

      int currentToken = 0;

      CharTermAttribute termAtt;

      OffsetAttribute offsetAtt;

      PositionIncrementAttribute posincAtt;

      StoredTokenStream(Token tokens[]) {
        this.tokens = tokens;
        termAtt = addAttribute(CharTermAttribute.class);
        offsetAtt = addAttribute(OffsetAttribute.class);
        posincAtt = addAttribute(PositionIncrementAttribute.class);
      }

      @Override
      public boolean incrementToken() {
        if (currentToken >= tokens.length) {
          return false;
        }
        Token token = tokens[currentToken++];
        clearAttributes();
        termAtt.setEmpty().append(token);
        offsetAtt.setOffset(token.startOffset(), token.endOffset());
        posincAtt
            .setPositionIncrement(currentToken <= 1
                || tokens[currentToken - 1].startOffset() > tokens[currentToken - 2]
                    .startOffset() ? 1 : 0);
        return true;
      }
    }
    // code to reconstruct the original sequence of Tokens
    TermsEnum termsEnum = tpv.iterator(null);
    int totalTokens = 0;
    while(termsEnum.next() != null) {
      totalTokens += (int) termsEnum.totalTermFreq();
    }
    Token tokensInOriginalOrder[] = new Token[totalTokens];
    ArrayList<Token> unsortedTokens = null;
    termsEnum = tpv.iterator(null);
    BytesRef text;
    DocsAndPositionsEnum dpEnum = null;
    while ((text = termsEnum.next()) != null) {

      dpEnum = termsEnum.docsAndPositions(null, dpEnum);
      if (dpEnum == null) {
        throw new IllegalArgumentException(
            "Required TermVector Offset information was not found");
      }
      final String term = text.utf8ToString();

      dpEnum.nextDoc();
      final int freq = dpEnum.freq();
      for(int posUpto=0;posUpto<freq;posUpto++) {
        final int pos = dpEnum.nextPosition();
        if (dpEnum.startOffset() < 0) {
          throw new IllegalArgumentException(
              "Required TermVector Offset information was not found");
        }
        final Token token = new Token(term,
                                      dpEnum.startOffset(),
                                      dpEnum.endOffset());
        if (tokenPositionsGuaranteedContiguous && pos != -1) {
          // We have positions stored and a guarantee that the token position
          // information is contiguous

          // This may be fast BUT wont work if Tokenizers used which create >1
          // token in same position or
          // creates jumps in position numbers - this code would fail under those
          // circumstances

          // tokens stored with positions - can use this to index straight into
          // sorted array
          tokensInOriginalOrder[pos] = token;
        } else {
          // tokens NOT stored with positions or not guaranteed contiguous - must
          // add to list and sort later
          if (unsortedTokens == null) {
            unsortedTokens = new ArrayList<Token>();
          }
          unsortedTokens.add(token);
        }
      }
    }

    // If the field has been stored without position data we must perform a sort
    if (unsortedTokens != null) {
      tokensInOriginalOrder = unsortedTokens.toArray(new Token[unsortedTokens
          .size()]);
      ArrayUtil.mergeSort(tokensInOriginalOrder, new Comparator<Token>() {
        @Override
        public int compare(Token t1, Token t2) {
          if (t1.startOffset() == t2.startOffset()) return t1.endOffset()
              - t2.endOffset();
          else return t1.startOffset() - t2.startOffset();
        }
      });
    }
    return new StoredTokenStream(tokensInOriginalOrder);
  }

  /**
   * Returns a {@link TokenStream} with positions and offsets constructed from
   * field termvectors.  If the field has no termvectors, or positions or offsets
   * are not included in the termvector, return null.
   * @param reader the {@link IndexReader} to retrieve term vectors from
   * @param docId the document to retrieve termvectors for
   * @param field the field to retrieve termvectors for
   * @return a {@link TokenStream}, or null if positions and offsets are not available
   * @throws IOException If there is a low-level I/O error
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

    if (!vector.hasPositions() || !vector.hasOffsets()) {
      return null;
    }
    
    return getTokenStream(vector);
  }

  // convenience method
  public static TokenStream getTokenStream(IndexReader reader, int docId,
      String field, Analyzer analyzer) throws IOException {
    Document doc = reader.document(docId);
    return getTokenStream(doc, field, analyzer);
  }

  public static TokenStream getTokenStream(Document doc, String field,
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
      return analyzer.tokenStream(field, new StringReader(contents));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

}
