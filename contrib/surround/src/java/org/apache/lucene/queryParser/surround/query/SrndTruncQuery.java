package org.apache.lucene.queryParser.surround.query;
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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;

import java.util.regex.Pattern;
import java.util.regex.Matcher;


public class SrndTruncQuery extends SimpleTerm {
  public SrndTruncQuery(String truncated, char unlimited, char mask) {
    super(false); /* not quoted */
    this.truncated = truncated;
    this.unlimited = unlimited;
    this.mask = mask;
    truncatedToPrefixAndPattern();
  }
  
  private final String truncated;
  private final char unlimited;
  private final char mask;
  
  private String prefix;
  private Pattern pattern;
  
  
  public String getTruncated() {return truncated;}
  
  public String toStringUnquoted() {return getTruncated();}

  
  protected boolean matchingChar(char c) {
    return (c != unlimited) && (c != mask);
  }

  protected void appendRegExpForChar(char c, StringBuffer re) {
    if (c == unlimited)
      re.append(".*");
    else if (c == mask)
      re.append(".");
    else
      re.append(c);
  }
  
  protected void truncatedToPrefixAndPattern() {
    int i = 0;
    while ((i < truncated.length()) && matchingChar(truncated.charAt(i))) {
      i++;
    }
    prefix = truncated.substring(0, i);
    
    StringBuffer re = new StringBuffer();
    while (i < truncated.length()) {
      appendRegExpForChar(truncated.charAt(i), re);
      i++;
    }
    pattern = Pattern.compile(re.toString());
  }
  
  public void visitMatchingTerms(
    IndexReader reader,
    String fieldName,
    MatchingTermVisitor mtv) throws IOException
  {
    boolean expanded = false;
    int prefixLength = prefix.length();
    TermEnum enumerator = reader.terms(new Term(fieldName, prefix));
    Matcher matcher = pattern.matcher("");
    try {
      do {
        Term term = enumerator.term();
        if (term != null) {
          String text = term.text();
          if ((! text.startsWith(prefix)) || (! term.field().equals(fieldName))) {
            break;
          } else {
            matcher.reset( text.substring(prefixLength));
            if (matcher.matches()) {
              mtv.visitMatchingTerm(term);
              expanded = true;
            }
          }
        }
      } while (enumerator.next());
    } finally {
      enumerator.close();
      matcher.reset();
    }
    if (! expanded) {
      System.out.println("No terms in " + fieldName + " field for: " + toString());
    }
  }
}
