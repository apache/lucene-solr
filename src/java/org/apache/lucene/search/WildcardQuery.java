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
import java.io.IOException;

/** Implements the wildcard search query. Supported wildcards are <code>*</code>, which
 * matches any character sequence (including the empty one), and <code>?</code>,
 * which matches any single character. Note this query can be slow, as it
 * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
 * a Wildcard term should not start with one of the wildcards <code>*</code> or
 * <code>?</code>.
 * 
 * @see WildcardTermEnum
 */
public class WildcardQuery extends MultiTermQuery {
  private boolean termContainsWildcard;
    
  public WildcardQuery(Term term) {
    super(term);
    this.termContainsWildcard = (term.text().indexOf('*') != -1) || (term.text().indexOf('?') != -1);
  }

  protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
    return new WildcardTermEnum(reader, getTerm());
  }

  public boolean equals(Object o) {
    if (o instanceof WildcardQuery)
      return super.equals(o);

    return false;
  }
  
  public Query rewrite(IndexReader reader) throws IOException {
      if (this.termContainsWildcard) {
          return super.rewrite(reader);
      }
      
      return new TermQuery(getTerm());
  }
}
