package org.apache.lucene.analysis.snowball;

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

import java.lang.reflect.Method;

import net.sf.snowball.SnowballProgram;
import net.sf.snowball.ext.*;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;

/** A filter that stems words using a Snowball-generated stemmer.
 *
 * Available stemmers are listed in {@link net.sf.snowball.ext}.  The name of a
 * stemmer is the part of the class name before "Stemmer", e.g., the stemmer in
 * {@link EnglishStemmer} is named "English".
 */

public class SnowballFilter extends TokenFilter {
  private static final Object [] EMPTY_ARGS = new Object[0];

  private SnowballProgram stemmer;
  private Method stemMethod;

  /** Construct the named stemming filter.
   *
   * @param in the input tokens to stem
   * @param in the name of a stemmer
   */
  public SnowballFilter(TokenStream in, String name) {
    this.input = in;
    try {
      Class stemClass =
        Class.forName("net.sf.snowball.ext." + name + "Stemmer");
      stemmer = (SnowballProgram) stemClass.newInstance();
      stemMethod = stemClass.getMethod("stem", new Class[0]);
    } catch (Exception e) {
      throw new RuntimeException(e.toString());
    }
  }

  /** Returns the next input Token, after being stemmed */
  public final Token next() throws IOException {
    Token token = input.next();
    if (token == null)
      return null;
    stemmer.setCurrent(token.termText());
    try {
      stemMethod.invoke(stemmer, EMPTY_ARGS);
    } catch (Exception e) {
      throw new RuntimeException(e.toString());
    }
    return new Token(stemmer.getCurrent(),
                     token.startOffset(), token.endOffset(), token.type());
  }
}
