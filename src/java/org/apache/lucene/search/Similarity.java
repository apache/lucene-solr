package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.IOException;
import org.apache.lucene.index.Term;
import org.apache.lucene.document.Field;

/** Internal class used for scoring.
 * <p>Public only so that the indexing code can compute and store the
 * normalization byte for each document. */
public abstract class Similarity {

  private static final float[] NORM_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++)
      NORM_TABLE[i] = byteToFloat((byte)i);
  }

  private static Similarity similarity;

  private Similarity() {}			  // no public constructor

  /**
   * Sets the <code>Similarity</code> implementation to use.
   *
   * @param sim an instance of a class that implements  <code>Similarity</code
   */
  public static void setDefaultSimilarity(Similarity sim) {
    similarity = sim;
  }

  /** Computes the normalization value for a document given the total number of
   * terms contained in a field.  These values are stored in an index and used
   * by the search code.
   *
   * <p>The formula used is: <code>1.0f / Math.sqrt(numTerms)</code>
   *
   * @see Field#setBoost(float)
   */
  public static float normalizeLength(int numTerms) {
    return (float)(1.0 / Math.sqrt(numTerms));
  }
  
  /** Decodes a normalization factor stored in an index.
   * @see #encodeNorm(float)
   */
  public static float decodeNorm(byte b) {
    return NORM_TABLE[b & 0xFF];
  }

  /** Encodes a normalization factor for storage in an index.  
   *
   * <p>The encoding uses a five-bit exponent and three-bit mantissa, thus
   * representing values from around 7x10^9 to 2x10^-9 with about one
   * significant decimal digit of accuracy.  Zero is also represented.
   * Negative numbers are rounded up to zero.  Values too large to represent
   * are rounded down to the largest representable value.  Positive values too
   * small to represent are rounded up to the smallest positive representable
   * value.
   *
   * @see Field#setBoost(float)
   */
  public static byte encodeNorm(float f) {
    return floatToByte(f);
  }

  private static float byteToFloat(byte b) {
    if (b == 0)                                   // zero is a special case
      return 0.0f;
    int mantissa = b & 7;
    int exponent = (b >> 3) & 31;
    int bits = ((exponent+(63-15)) << 24) | (mantissa << 21);
    return Float.intBitsToFloat(bits);
  }
   
  private static byte floatToByte(float f) {
    if (f < 0.0f)                                 // round negatives up to zero
      f = 0.0f;

    if (f == 0.0f)                                // zero is a special case
      return 0;

    int bits = Float.floatToIntBits(f);           // parse float into parts
    int mantissa = (bits & 0xffffff) >> 21;
    int exponent = (((bits >> 24) & 0x7f) - 63) + 15;

    if (exponent > 31) {                          // overflow: use max value
      exponent = 31;
      mantissa = 7;
    }

    if (exponent < 1) {                           // underflow: use min value
      exponent = 1;
      mantissa = 0;
    }

    return (byte)((exponent << 3) | mantissa);    // pack into a byte
   }

  static final float tf(int freq) {
    return (float)Math.sqrt(freq);
  }

  static final float tf(float freq) {
    return (float)Math.sqrt(freq);
  }
    
  static final float idf(Term term, Searcher searcher) throws IOException {
    // Use maxDoc() instead of numDocs() because its proportional to docFreq(),
    // i.e., when one is inaccurate, so is the other, and in the same way.
    return idf(searcher.docFreq(term), searcher.maxDoc());
  }

  static final float idf(int docFreq, int numDocs) {
    return (float)(Math.log(numDocs/(double)(docFreq+1)) + 1.0);
  }
    
  static final float coord(int overlap, int maxOverlap) {
    return overlap / (float)maxOverlap;
  }
}
