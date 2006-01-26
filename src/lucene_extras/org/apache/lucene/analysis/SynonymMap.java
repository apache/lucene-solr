/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.analysis;

import java.util.*;

/** Mapping rules for use with {@link SynonymFilter}
 *
 * @author yonik
 * @version $Id: SynonymMap.java,v 1.2 2005/12/13 05:15:08 yonik Exp $
 */
public class SynonymMap {
  Map submap; // recursive: Map<String, SynonymMap>
  Token[] synonyms;
  int flags;

  static final int INCLUDE_ORIG=0x01;

  public boolean includeOrig() { return (flags & INCLUDE_ORIG) != 0; }

  /**
   * @param singleMatch  List<String>, the sequence of strings to match
   * @param replacement  List<Token> the list of tokens to use on a match
   * @param includeOrig  sets a flag on this mapping signaling the generation of matched tokens in addition to the replacement tokens
   * @param mergeExisting merge the replacement tokens with any other mappings that exist
   */
  public void add(List singleMatch, List replacement, boolean includeOrig, boolean mergeExisting) {
    SynonymMap currMap = this;
    for (Iterator iter = singleMatch.iterator(); iter.hasNext();) {
      String str = (String)iter.next();
      if (currMap.submap==null) {
        currMap.submap = new HashMap(1);
      }

      SynonymMap map = (SynonymMap)currMap.submap.get(str);
      if (map==null) {
        map = new SynonymMap();
        currMap.submap.put(str, map);
      }

      currMap = map;
    }

    if (currMap.synonyms != null && !mergeExisting) {
      throw new RuntimeException("SynonymFilter: there is already a mapping for " + singleMatch);
    }
    List superset = currMap.synonyms==null ? replacement :
          mergeTokens(Arrays.asList(currMap.synonyms), replacement);
    currMap.synonyms = (Token[])superset.toArray(new Token[superset.size()]);
    if (includeOrig) currMap.flags |= INCLUDE_ORIG;
  }


  public String toString() {
    StringBuffer sb = new StringBuffer("<");
    if (synonyms!=null) {
      sb.append("[");
      for (int i=0; i<synonyms.length; i++) {
        if (i!=0) sb.append(',');
        sb.append(synonyms[i]);
      }
      if ((flags & INCLUDE_ORIG)!=0) {
        sb.append(",ORIG");
      }
      sb.append("],");
    }
    sb.append(submap);
    sb.append(">");
    return sb.toString();
  }



  /** Produces a List<Token> from a List<String> */
  public static List makeTokens(List strings) {
    List ret = new ArrayList(strings.size());
    for (Iterator iter = strings.iterator(); iter.hasNext();) {
      Token newTok = new Token((String)iter.next(),0,0,"SYNONYM");
      ret.add(newTok);
    }
    return ret;
  }


  /**
   * Merge two lists of tokens, producing a single list with manipulated positionIncrements so that
   * the tokens end up at the same position.
   *
   * Example:  [a b] merged with [c d] produces [a/b c/d]  ('/' denotes tokens in the same position)
   * Example:  [a,5 b,2] merged with [c d,4 e,4] produces [c a,5/d b,2 e,2]  (a,n means a has posInc=n)
   *
   */
  public static List mergeTokens(List lst1, List lst2) {
    ArrayList result = new ArrayList();
    if (lst1 ==null || lst2 ==null) {
      if (lst2 != null) result.addAll(lst2);
      if (lst1 != null) result.addAll(lst1);
      return result;
    }

    int pos=0;
    Iterator iter1=lst1.iterator();
    Iterator iter2=lst2.iterator();
    Token tok1 = iter1.hasNext() ? (Token)iter1.next() : null;
    Token tok2 = iter2.hasNext() ? (Token)iter2.next() : null;
    int pos1 = tok1!=null ? tok1.getPositionIncrement() : 0;
    int pos2 = tok2!=null ? tok2.getPositionIncrement() : 0;
    while(tok1!=null || tok2!=null) {
      while (tok1 != null && (pos1 <= pos2 || tok2==null)) {
        Token tok = new Token(tok1.termText, tok1.startOffset, tok1.endOffset, tok1.type);
        tok.setPositionIncrement(pos1-pos);
        result.add(tok);
        pos=pos1;
        tok1 = iter1.hasNext() ? (Token)iter1.next() : null;
        pos1 += tok1!=null ? tok1.getPositionIncrement() : 0;
      }
      while (tok2 != null && (pos2 <= pos1 || tok1==null)) {
        Token tok = new Token(tok2.termText, tok2.startOffset, tok2.endOffset, tok2.type);
        tok.setPositionIncrement(pos2-pos);
        result.add(tok);
        pos=pos2;
        tok2 = iter2.hasNext() ? (Token)iter2.next() : null;
        pos2 += tok2!=null ? tok2.getPositionIncrement() : 0;
      }
    }
    return result;
  }

}
