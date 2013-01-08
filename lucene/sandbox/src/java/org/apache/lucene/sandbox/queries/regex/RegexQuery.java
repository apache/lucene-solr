package org.apache.lucene.sandbox.queries.regex;

/*
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

import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.search.RegexpQuery; // javadoc
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;

/** Implements the regular expression term search query.
 * The expressions supported depend on the regular expression implementation
 * used by way of the {@link RegexCapabilities} interface.
 * <p>
 * NOTE: You may wish to consider using the regex query support 
 * in {@link RegexpQuery} instead, as it has better performance.
 * 
 * @see RegexTermsEnum
 */
public class RegexQuery extends MultiTermQuery implements RegexQueryCapable {

  private RegexCapabilities regexImpl = new JavaUtilRegexCapabilities();
  private Term term;

  /** Constructs a query for terms matching <code>term</code>. */
  public RegexQuery(Term term) {
    super(term.field());
    this.term = term;
  }
  
  public Term getTerm() {
    return term;
  }

  @Override
  public void setRegexImplementation(RegexCapabilities impl) {
    this.regexImpl = impl;
  }

  @Override
  public RegexCapabilities getRegexImplementation() {
    return regexImpl;
  }

  @Override
  protected FilteredTermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    return new RegexTermsEnum(terms.iterator(null), term, regexImpl);
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!term.field().equals(field)) {
      buffer.append(term.field());
      buffer.append(":");
    }
    buffer.append(term.text());
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((regexImpl == null) ? 0 : regexImpl.hashCode());
    result = prime * result + ((term == null) ? 0 : term.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    RegexQuery other = (RegexQuery) obj;
    if (regexImpl == null) {
      if (other.regexImpl != null) {
        return false;
      }
    } else if (!regexImpl.equals(other.regexImpl)) {
      return false;
    }

    if (term == null) {
      if (other.term != null) {
        return false;
      }
    } else if (!term.equals(other.term)) {
      return false;
    }
    
    return true;
  }
}
