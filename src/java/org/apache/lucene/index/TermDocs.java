package org.apache.lucene.index;

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
import org.apache.lucene.document.Document;

/** TermDocs provides an interface for enumerating &lt;document, frequency&gt;
  pairs for a term.  <p> The document portion names each document containing
  the term.  Documents are indicated by number.  The frequency portion gives
  the number of times the term occurred in each document.  <p> The pairs are
  ordered by document number.

  @see IndexReader#termDocs
  */

public interface TermDocs {
  /** Returns the current document number.  <p> This is invalid until {@link
      #next()} is called for the first time.*/
  public int doc();

  /** Returns the frequency of the term within the current document.  <p> This
    is invalid until {@link #next()} is called for the first time.*/
  public int freq();

  /** Moves to the next pair in the enumeration.  <p> Returns true iff there is
    such a next pair in the enumeration. */
  public boolean next() throws IOException;

  /** Attempts to read multiple entries from the enumeration, up to length of
   * <i>docs</i>.  Document numbers are stored in <i>docs</i>, and term
   * frequencies are stored in <i>freqs</i>.  The <i>freqs</i> array must be as
   * long as the <i>docs</i> array.
   *
   * <p>Returns the number of entries read.  Zero is only returned when the
   * stream has been exhausted.  */
  public int read(int[] docs, int[] freqs) throws IOException;

  /** Skips entries to the first beyond the current whose document number is
   * greater than or equal to <i>target</i>. <p>Returns true iff there is such
   * an entry.  <p>Behaves as if written: <pre>
   *   public boolean skipTo(int target) {
   *     do {
   *       if (!next())
   * 	     return false;
   *     } while (target > doc());
   *     return true;
   *   }
   * </pre>
   * Some implementations are considerably more efficient than that.
   */
  public boolean skipTo(int target) throws IOException;

  /** Frees associated resources. */
  public void close() throws IOException;
}


