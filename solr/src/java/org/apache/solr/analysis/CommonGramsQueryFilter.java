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
package org.apache.solr.analysis;

import java.io.IOException;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.Token;

/**
 * Wrap a CommonGramsFilter optimizing phrase queries by only returning single
 * words when they are not a member of a bigram.
 * 
 * Example:
 * <ul>
 * <li>query input to CommonGramsFilter: "the rain in spain falls mainly"
 * <li>output of CommomGramsFilter/input to CommonGramsQueryFilter:
 * |"the, "the-rain"|"rain" "rain-in"|"in, "in-spain"|"spain"|"falls"|"mainly"
 * <li>output of CommonGramsQueryFilter:"the-rain", "rain-in" ,"in-spain",
 * "falls", "mainly"
 * </ul>
 */

/*
 * TODO: When org.apache.solr.analysis.BufferedTokenStream is changed to use the
 * 2.9 lucene TokenStream api, make necessary changes here.
 * See:http://hudson.zones
 * .apache.org/hudson/job/Lucene-trunk/javadoc//all/org/apache
 * /lucene/analysis/TokenStream.html and
 * http://svn.apache.org/viewvc/lucene/java
 * /trunk/src/java/org/apache/lucene/analysis/package.html?revision=718798
 */
public class CommonGramsQueryFilter extends BufferedTokenStream {
  //private CharArraySet commonWords;
  private Token prev;

  /**
   * Constructor
   * 
   * @param input must be a CommonGramsFilter!
   * 
   */

  public CommonGramsQueryFilter(CommonGramsFilter input) {
    super(input);
    prev = new Token();
  }
  
  public void reset() throws IOException {
    super.reset();
    prev = new Token();
  }
  
  /**
   * Output bigrams whenever possible to optimize queries. Only output unigrams
   * when they are not a member of a bigram. Example:
   * <ul>
   * <li>input: "the rain in spain falls mainly"
   * <li>output:"the-rain", "rain-in" ,"in-spain", "falls", "mainly"
   */

  public Token process(Token token) throws IOException {
    Token next = peek(1);
    /*
     * Deal with last token (next=null when current token is the last word) Last
     * token will be a unigram. If previous token was a bigram, then we already
     * output the last token as part of the unigram and should not additionally
     * output the unigram. <p> Example: If the end of the input to the
     * CommonGramsFilter is "...the plain" <ul> <li>current token = "plain"</li>
     * <li>next token = null</li> <li>previous token = "the-plain" (bigram)</li>
     * <li> Since the word "plain" was already output as part of the bigram we
     * don't output it.</li> </ul> Example: If the end of the input to the
     * CommonGramsFilter is "falls mainly" <ul> <li>current token =
     * "mainly"</li> <li>next token = null</li> <li>previous token = "falls"
     * (unigram)</li> <li>Since we haven't yet output the current token, we
     * output it</li> </ul>
     */

    // Deal with special case of last token
    if (next == null) {
      if (prev == null) {
        // This is the first and only token i.e. one word query
        return token;
      }
      if (prev != null && prev.type() != "gram") {
        // If previous token was a unigram, output the current token
        return token;
      } else {
        // If previous token was a bigram, we already output it and this token
        // was output as part of the bigram so we are done.
        return null;
      }
    }

    /*
     * Possible cases are: |token |next 1|word |gram 2|word |word The
     * CommonGramsFilter we are wrapping always outputs the unigram word prior
     * to outputting an optional bigram: "the sound of" gets output as |"the",
     * "the_sound"|"sound", "sound_of" For case 1 we consume the gram from the
     * input stream and output it rather than the current token This means that
     * the call to super.next() which reads a token from input and passes it on
     * to this process method will always get a token of type word
     */
    if (next != null && next.type() == "gram") {
      // consume "next" token from list and output it
      token = read();
      // use this to clone the token because clone requires all these args but
      // won't take the token.type
      // see
      // http://hudson.zones.apache.org/hudson/job/Lucene-trunk/javadoc//all/org/apache/lucene/analysis/Token.html
      prev.reinit(token.termBuffer(), 0, token.termLength(), token
          .startOffset(), token.endOffset(), token.type());
      token.setPositionIncrement(1);
      return token;
    }

    // if the next token is not a bigram, then output the token
    // see note above regarding this method of copying token to prev
    prev.reinit(token.termBuffer(), 0, token.termLength(), token.startOffset(),
        token.endOffset(), token.type());
    assert token.type() == "word";
    return token;
  }
}
