package org.apache.lucene.analysis;

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
import java.util.Hashtable;

/** Removes stop words from a token stream. */

public final class StopFilter extends TokenFilter {

  private Hashtable table;

  /** Constructs a filter which removes words from the input
    TokenStream that are named in the array of words. */
  public StopFilter(TokenStream in, String[] stopWords) {
    input = in;
    table = makeStopTable(stopWords);
  }

  /** Constructs a filter which removes words from the input
    TokenStream that are named in the Hashtable. */
  public StopFilter(TokenStream in, Hashtable stopTable) {
    input = in;
    table = stopTable;
  }
  
  /** Builds a Hashtable from an array of stop words, appropriate for passing
    into the StopFilter constructor.  This permits this table construction to
    be cached once when an Analyzer is constructed. */
  public final static Hashtable makeStopTable(String[] stopWords) {
    Hashtable stopTable = new Hashtable(stopWords.length);
    for (int i = 0; i < stopWords.length; i++)
      stopTable.put(stopWords[i], stopWords[i]);
    return stopTable;
  }

  /** Returns the next input Token whose termText() is not a stop word. */
  public final Token next() throws IOException {
    // return the first non-stop word found
    for (Token token = input.next(); token != null; token = input.next())
      if (table.get(token.termText) == null)
	return token;
    // reached EOS -- return null
    return null;
  }
}
