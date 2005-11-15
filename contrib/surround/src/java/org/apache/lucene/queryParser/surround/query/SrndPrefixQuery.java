package org.apache.lucene.queryParser.surround.query;
/**
 * Copyright 2005 The Apache Software Foundation
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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;


public class SrndPrefixQuery extends SimpleTerm {
  public SrndPrefixQuery(String prefix, boolean quoted, char truncator) {
    super(quoted);
    this.prefix = prefix;
    this.truncator = truncator;
  }

  private final String prefix;
  public String getPrefix() {return prefix;}
  
  private final char truncator;
  public char getSuffixOperator() {return truncator;}
  
  public Term getLucenePrefixTerm(String fieldName) {
    return new Term(fieldName, getPrefix());
  }
  
  public String toStringUnquoted() {return getPrefix();}
  
  protected void suffixToString(StringBuffer r) {r.append(getSuffixOperator());}
  
  public void visitMatchingTerms(
    IndexReader reader,
    String fieldName,
    MatchingTermVisitor mtv) throws IOException
  {
    /* inspired by PrefixQuery.rewrite(): */
    TermEnum enumerator = reader.terms(getLucenePrefixTerm(fieldName));
    boolean expanded = false;
    try {
      do {
        Term term = enumerator.term();
        if ((term != null)
            && term.text().startsWith(getPrefix())
            && term.field().equals(fieldName)) {
          mtv.visitMatchingTerm(term);
          expanded = true;
        } else {
          break;
        }
      } while (enumerator.next());
    } finally {
      enumerator.close();
    }
    if (! expanded) {
      System.out.println("No terms in " + fieldName + " field for: " + toString());
    }
  }
}
