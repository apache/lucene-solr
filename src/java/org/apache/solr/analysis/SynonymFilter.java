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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

/** SynonymFilter handles multi-token synonyms with variable position increment offsets.
 * <p>
 * The matched tokens from the input stream may be optionally passed through (includeOrig=true)
 * or discarded.  If the original tokens are included, the position increments may be modified
 * to retain absolute positions after merging with the synonym tokenstream.
 * <p>
 * Generated synonyms will start at the same position as the first matched source token.
 *
 * @author yonik
 * @version $Id$
 */
public class SynonymFilter extends TokenFilter {

  private final SynonymMap map;  // Map<String, SynonymMap>
  private final boolean ignoreCase;
  private Iterator replacement;  // iterator over generated tokens

  public SynonymFilter(TokenStream in, SynonymMap map, boolean ignoreCase) {
    super(in);
    this.map = map;
    this.ignoreCase = ignoreCase;
  }


  /*
   * Need to worry about multiple scenarios:
   *  - need to go for the longest match
   *    a b => foo      #shouldn't match if "a b" is followed by "c d"
   *    a b c d => bar
   *  - need to backtrack - retry matches for tokens already read
   *     a b c d => foo
   *       b c => bar
   *     If the input stream is "a b c x", one will consume "a b c d"
   *     trying to match the first rule... all but "a" should be
   *     pushed back so a match may be made on "b c".
   *  - don't try and match generated tokens (thus need separate queue)
   *    matching is not recursive.
   *  - handle optional generation of original tokens in all these cases,
   *    merging token streams to preserve token positions.
   *  - preserve original positionIncrement of first matched token
   */


  public Token next() throws IOException {
    while (true) {
      // if there are any generated tokens, return them... don't try any
      // matches against them, as we specifically don't want recursion.
      if (replacement!=null && replacement.hasNext()) {
        return (Token)replacement.next();
      }

      // common case fast-path of first token not matching anything
      Token firstTok = nextTok();
      if (firstTok ==null) return null;
      String str = ignoreCase ? firstTok.termText().toLowerCase() : firstTok.termText();
      Object o = map.submap!=null ? map.submap.get(str) : null;
      if (o == null) return firstTok;

      // OK, we matched a token, so find the longest match.

      // since matched is only used for matches >= 2, defer creation until now
      if (matched==null) matched=new LinkedList();

      SynonymMap result = match((SynonymMap)o);

      if (result==null) {
        // no match, simply return the first token read.
        return firstTok;
      }

      // reuse, or create new one each time?
      ArrayList generated = new ArrayList(result.synonyms.length + matched.size() + 1);

      //
      // there was a match... let's generate the new tokens, merging
      // in the matched tokens (position increments need adjusting)
      //
      Token lastTok = matched.isEmpty() ? firstTok : (Token)matched.getLast();
      boolean includeOrig = result.includeOrig();

      Token origTok = includeOrig ? firstTok : null;
      int origPos = firstTok.getPositionIncrement();  // position of origTok in the original stream
      int repPos=0; // curr position in replacement token stream
      int pos=0;  // current position in merged token stream

      for (int i=0; i<result.synonyms.length; i++) {
        Token repTok = result.synonyms[i];
        Token newTok = new Token(repTok.termText(), firstTok.startOffset(), lastTok.endOffset(), firstTok.type());
        repPos += repTok.getPositionIncrement();
        if (i==0) repPos=origPos;  // make position of first token equal to original

        // if necessary, insert original tokens and adjust position increment
        while (origTok != null && origPos <= repPos) {
          origTok.setPositionIncrement(origPos-pos);
          generated.add(origTok);
          pos += origTok.getPositionIncrement();
          origTok = matched.isEmpty() ? null : (Token)matched.removeFirst();
          if (origTok != null) origPos += origTok.getPositionIncrement();
        }

        newTok.setPositionIncrement(repPos - pos);
        generated.add(newTok);
        pos += newTok.getPositionIncrement();
      }

      // finish up any leftover original tokens
      while (origTok!=null) {
        origTok.setPositionIncrement(origPos-pos);
        generated.add(origTok);
        pos += origTok.getPositionIncrement();
        origTok = matched.isEmpty() ? null : (Token)matched.removeFirst();
        if (origTok != null) origPos += origTok.getPositionIncrement();
      }

      // what if we replaced a longer sequence with a shorter one?
      // a/0 b/5 =>  foo/0
      // should I re-create the gap on the next buffered token?

      replacement = generated.iterator();
      // Now return to the top of the loop to read and return the first
      // generated token.. The reason this is done is that we may have generated
      // nothing at all, and may need to continue with more matching logic.
    }
  }


  //
  // Defer creation of the buffer until the first time it is used to
  // optimize short fields with no matches.
  //
  private LinkedList buffer;
  private LinkedList matched;

  // TODO: use ArrayList for better performance?

  private Token nextTok() throws IOException {
    if (buffer!=null && !buffer.isEmpty()) {
      return (Token)buffer.removeFirst();
    } else {
      return input.next();
    }
  }

  private void pushTok(Token t) {
    if (buffer==null) buffer=new LinkedList();
    buffer.addFirst(t);
  }



  private SynonymMap match(SynonymMap map) throws IOException {
    SynonymMap result = null;

    if (map.submap != null) {
      Token tok = nextTok();
      if (tok != null) {
        // check for positionIncrement!=1?  if>1, should not match, if==0, check multiple at this level?
        String str = ignoreCase ? tok.termText().toLowerCase() : tok.termText();

        SynonymMap subMap = (SynonymMap)map.submap.get(str);

        if (subMap !=null) {
          // recurse
          result = match(subMap);
        }
        if (result != null) {
          matched.addFirst(tok);
        } else {
          // push back unmatched token
          pushTok(tok);
        }
      }
    }

    // if no longer sequence matched, so if this node has synonyms, it's the match.
    if (result==null && map.synonyms!=null) {
      result = map;
    }

    return result;
  }

}
