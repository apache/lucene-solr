package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import java.io.IOException;

/** Implements the wildcard search query. Supported wildcards are <code>*</code>, which
 * matches any character sequence (including the empty one), and <code>?</code>,
 * which matches any single character. Note this query can be slow, as it
 * needs to iterate over all terms. In order to prevent extremely slow WildcardQueries,
 * a Wildcard term must not start with one of the wildcards <code>*</code> or
 * <code>?</code>.
 * 
 * @see WildcardTermEnum
 */
public class WildcardQuery extends MultiTermQuery {
  public WildcardQuery(Term term) {
    super(term);
  }

  protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
    return new WildcardTermEnum(reader, getTerm());
  }

  public boolean equals(Object o) {
    if (o instanceof WildcardQuery)
      return super.equals(o);

    return false;
  }
}
