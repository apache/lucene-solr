package org.apache.lucene.search;

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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

/** Subclass of FilteredTermEnum for enumerating all terms that match the specified wildcard filter term.

  <p>Term enumerations are always ordered by Term.compareTo().  Each term in
  the enumeration is greater than all that precede it.  */
public class WildcardTermEnum extends FilteredTermEnum {
  Term searchTerm;
  String field = "";
  String text = "";
  String pre = "";
  int preLen = 0;
  boolean fieldMatch = false;
  boolean endEnum = false;
  
  /** Creates new WildcardTermEnum */
  public WildcardTermEnum(IndexReader reader, Term term) throws IOException {
      super(reader, term);
      searchTerm = term;
      field = searchTerm.field();
      text = searchTerm.text();

      int sidx = text.indexOf(WILDCARD_STRING);
      int cidx = text.indexOf(WILDCARD_CHAR);
      int idx = sidx;
      if (idx == -1) idx = cidx;
      else if (cidx >= 0) idx = Math.min(idx, cidx);

      pre = searchTerm.text().substring(0,idx);
      preLen = pre.length();
      text = text.substring(preLen);
      setEnum(reader.terms(new Term(searchTerm.field(), pre)));
  }
  
  final protected boolean termCompare(Term term) {
      if (field == term.field()) {
          String searchText = term.text();
          if (searchText.startsWith(pre)) {
            return wildcardEquals(text, 0, searchText, preLen);
          }
      }
      endEnum = true;
      return false;
  }
  
  final public float difference() {
    return 1.0f;
  }
  
  final public boolean endEnum() {
    return endEnum;
  }
  
  /********************************************
   * String equality with support for wildcards
   ********************************************/
  
  public static final char WILDCARD_STRING = '*';
  public static final char WILDCARD_CHAR = '?';
  
  public static final boolean wildcardEquals(String pattern, int patternIdx, String string, int stringIdx) {
    for ( int p = patternIdx; ; ++p ) {
      for ( int s = stringIdx; ; ++p, ++s ) {
        boolean sEnd = (s >= string.length());
        boolean pEnd = (p >= pattern.length());
        
        if (sEnd && pEnd) return true;
        if (sEnd || pEnd) break;
        if (pattern.charAt(p) == WILDCARD_CHAR) continue;
        if (pattern.charAt(p) == WILDCARD_STRING) {
          int i;
          ++p;
          for (i = string.length(); i >= s; --i)
            if (wildcardEquals(pattern, p, string, i))
              return true;
          break;
        }
        if (pattern.charAt(p) != string.charAt(s)) break;
      }
      return false;
    }
  }
  
  public void close() throws IOException {
      super.close();
      searchTerm = null;
      field = null;
      text = null;
  }
}
