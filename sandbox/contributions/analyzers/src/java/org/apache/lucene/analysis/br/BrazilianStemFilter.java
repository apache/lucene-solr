package org.apache.lucene.analysis.br;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2004 The Apache Software Foundation.  All rights
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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;

import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

/**
 * Based on (copied) the GermanStemFilter
 *
 * @author João Kramer
 *         <p/>
 *         <p/>
 *         A filter that stemms german words. It supports a table of words that should
 *         not be stemmed at all.
 * @author Gerhard Schwarz
 */
public final class BrazilianStemFilter extends TokenFilter {

  /**
   * The actual token in the input stream.
   */
  private Token token = null;
  private BrazilianStemmer stemmer = null;
  private Set exclusions = null;

  public BrazilianStemFilter(TokenStream in) {
    super(in);
    stemmer = new BrazilianStemmer();
  }

  /**
   * Builds a BrazilianStemFilter that uses an exclusiontable.
   *
   * @deprecated
   */
  public BrazilianStemFilter(TokenStream in, Hashtable exclusiontable) {
    this(in);
    this.exclusions = new HashSet(exclusiontable.keySet());
  }

  public BrazilianStemFilter(TokenStream in, Set exclusiontable) {
    this(in);
    this.exclusions = exclusiontable;
  }

  /**
   * @return Returns the next token in the stream, or null at EOS.
   */
  public final Token next()
      throws IOException {
    if ((token = input.next()) == null) {
      return null;
    }
    // Check the exclusiontable.
    else if (exclusions != null && exclusions.contains(token.termText())) {
      return token;
    } else {
      String s = stemmer.stem(token.termText());
      // If not stemmed, dont waste the time creating a new token.
      if ((s != null) && !s.equals(token.termText())) {
        return new Token(s, 0, s.length(), token.type());
      }
      return token;
    }
  }
}


