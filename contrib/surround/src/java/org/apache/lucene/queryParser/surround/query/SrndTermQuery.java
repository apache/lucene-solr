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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

 
public class SrndTermQuery extends SimpleTerm {
  public SrndTermQuery(String termText, boolean quoted) {
    super(quoted);
    this.termText = termText;
  }

  private final String termText;
  public String getTermText() {return termText;}
        
  public Term getLuceneTerm(String fieldName) {
    return new Term(fieldName, getTermText());
  }
  
  public String toStringUnquoted() {return getTermText();}
  
  public void visitMatchingTerms(
    IndexReader reader,
    String fieldName,
    MatchingTermVisitor mtv) throws IOException
  {
    /* check term presence in index here for symmetry with other SimpleTerm's */
    TermEnum enumerator = reader.terms(getLuceneTerm(fieldName));
    try {
      Term it= enumerator.term(); /* same or following index term */
      if ((it != null)
          && it.text().equals(getTermText())
          && it.field().equals(fieldName)) {
        mtv.visitMatchingTerm(it);
      } else {
        System.out.println("No term in " + fieldName + " field for: " + toString());
      }
    } finally {
      enumerator.close();
    }
  }
}
  


