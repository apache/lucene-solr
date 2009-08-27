package org.apache.lucene.search;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;

/** Implements the wildcard search query. Supported wildcards are <code>*</code>, which
 * matches any character sequence (including the empty one), and <code>?</code>,
 * which matches any single character. Note this query can be slow, as it
 * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
 * a Wildcard term should not start with one of the wildcards <code>*</code> or
 * <code>?</code>.
 * 
 * <p>This query uses the {@link
 * MultiTermQuery#CONSTANT_SCORE_AUTO_REWRITE_DEFAULT}
 * rewrite method.
 *
 * @see WildcardTermEnum */
public class WildcardQuery extends MultiTermQuery {
  private boolean termContainsWildcard;
  protected Term term;
    
  public WildcardQuery(Term term) {
    super(term); //will be removed in 3.0
    this.term = term;
    this.termContainsWildcard = (term.text().indexOf('*') != -1) || (term.text().indexOf('?') != -1);
  }

  protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
    return new WildcardTermEnum(reader, getTerm());
  }
  
  /**
   * Returns the pattern term.
   */
  public Term getTerm() {
    return term;
  }

  public Query rewrite(IndexReader reader) throws IOException {
    if (!termContainsWildcard)
      return new TermQuery(getTerm());
    else
      return super.rewrite(reader);
  }
  
  /** Prints a user-readable version of this query. */
  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    if (!term.field().equals(field)) {
      buffer.append(term.field());
      buffer.append(":");
    }
    buffer.append(term.text());
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  //@Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((term == null) ? 0 : term.hashCode());
    return result;
  }

  //@Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    WildcardQuery other = (WildcardQuery) obj;
    if (term == null) {
      if (other.term != null)
        return false;
    } else if (!term.equals(other.term))
      return false;
    return true;
  }

}
