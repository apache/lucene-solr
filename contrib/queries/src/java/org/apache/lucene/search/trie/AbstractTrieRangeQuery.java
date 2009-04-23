package org.apache.lucene.search.trie;

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

import java.io.IOException;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.FilteredTermEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.ToStringUtils;

abstract class AbstractTrieRangeQuery extends MultiTermQuery {

  AbstractTrieRangeQuery(final String field, final int precisionStep,
    Number min, Number max, final boolean minInclusive, final boolean maxInclusive
  ) {
    this.field = field.intern();
    this.precisionStep = precisionStep;
    this.min = min;
    this.max = max;
    this.minInclusive = minInclusive;
    this.maxInclusive = maxInclusive;
    setConstantScoreRewrite(true);
  }
  
  abstract void passRanges(TrieRangeTermEnum enumerator);

  //@Override
  protected FilteredTermEnum getEnum(final IndexReader reader) throws IOException {
    TrieRangeTermEnum enumerator = new TrieRangeTermEnum(this, reader);
    passRanges(enumerator);
    enumerator.init();
    return enumerator;
  }

  /** Returns the field name for this query */
  public String getField() { return field; }

  /** Returns <code>true</code> if the lower endpoint is inclusive */
  public boolean includesMin() { return minInclusive; }
  
  /** Returns <code>true</code> if the upper endpoint is inclusive */
  public boolean includesMax() { return maxInclusive; }

  //@Override
  public String toString(final String field) {
    final StringBuffer sb=new StringBuffer();
    if (!this.field.equals(field)) sb.append(this.field).append(':');
    return sb.append(minInclusive ? '[' : '{')
      .append((min==null) ? "*" : min.toString())
      .append(" TO ")
      .append((max==null) ? "*" : max.toString())
      .append(maxInclusive ? ']' : '}')
      .append(ToStringUtils.boost(getBoost()))
      .toString();
  }

  //@Override
  public final boolean equals(final Object o) {
    if (o==this) return true;
    if (o==null) return false;
    if (this.getClass().equals(o.getClass())) {
      AbstractTrieRangeQuery q=(AbstractTrieRangeQuery)o;
      return (
        field==q.field &&
        (q.min == null ? min == null : q.min.equals(min)) &&
        (q.max == null ? max == null : q.max.equals(max)) &&
        minInclusive==q.minInclusive &&
        maxInclusive==q.maxInclusive &&
        precisionStep==q.precisionStep &&
        getBoost()==q.getBoost()
      );
    }
    return false;
  }

  //@Override
  public final int hashCode() {
    int hash = Float.floatToIntBits(getBoost()) ^ field.hashCode();
    hash += precisionStep^0x64365465;
    if (min!=null) hash += min.hashCode()^0x14fa55fb;
    if (max!=null) hash += max.hashCode()^0x733fa5fe;
    return hash+
      (Boolean.valueOf(minInclusive).hashCode()^0x14fa55fb)+
      (Boolean.valueOf(maxInclusive).hashCode()^0x733fa5fe);
  }
  
  // TODO: Make this method accessible by *TrieRangeFilter,
  // can be removed, when moved to core.
  //@Override
  protected Filter getFilter() {
    return super.getFilter();
  }
  
  // members
  final String field;
  final int precisionStep;
  final Number min,max;
  final boolean minInclusive,maxInclusive;
}
