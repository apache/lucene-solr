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
package org.apache.solr.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.solr.search.BitsFilteredDocIdSet;
import org.apache.solr.search.SolrFilter;

import java.io.IOException;
import java.util.Map;


/**
 * RangeFilter over a ValueSource.
 */
public class ValueSourceRangeFilter extends SolrFilter {
  private final ValueSource valueSource;
  private final String lowerVal;
  private final String upperVal;
  private final boolean includeLower;
  private final boolean includeUpper;

  public ValueSourceRangeFilter(ValueSource valueSource,
                                String lowerVal,
                                String upperVal,
                                boolean includeLower,
                                boolean includeUpper) {
    this.valueSource = valueSource;
    this.lowerVal = lowerVal;
    this.upperVal = upperVal;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
//    this.includeLower = lowerVal != null && includeLower;
//    this.includeUpper = upperVal != null && includeUpper;
  }

  public ValueSource getValueSource() {
    return valueSource;
  }

  public String getLowerVal() {
    return lowerVal;
  }

  public String getUpperVal() {
    return upperVal;
  }

  public boolean isIncludeLower() {
    return includeLower;
  }

  public boolean isIncludeUpper() {
    return includeUpper;
  }


  @Override
  @SuppressWarnings({"rawtypes"})
  public DocIdSet getDocIdSet(final Map context, final LeafReaderContext readerContext, Bits acceptDocs) throws IOException {
    // NB the IndexSearcher parameter here can be null because Filter Weights don't
    // actually use it.
    Weight weight = createWeight(null, ScoreMode.COMPLETE, 1);
    return BitsFilteredDocIdSet.wrap(new DocIdSet() {
       @Override
       public DocIdSetIterator iterator() throws IOException {
         @SuppressWarnings({"unchecked"})
         Scorer scorer = valueSource.getValues(context, readerContext).getRangeScorer(weight, readerContext, lowerVal, upperVal, includeLower, includeUpper);
         return scorer == null ? null : scorer.iterator();
       }
       @Override
       public Bits bits() {
         return null;  // don't use random access
       }

       @Override
       public long ramBytesUsed() {
         return 0L;
       }
     }, acceptDocs);
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void createWeight(@SuppressWarnings({"rawtypes"})Map context, IndexSearcher searcher) throws IOException {
    valueSource.createWeight(context, searcher);
  }

  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    sb.append("frange(");
    sb.append(valueSource);
    sb.append("):");
    sb.append(includeLower ? '[' : '{');
    sb.append(lowerVal == null ? "*" : lowerVal);
    sb.append(" TO ");
    sb.append(upperVal == null ? "*" : upperVal);
    sb.append(includeUpper ? ']' : '}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ValueSourceRangeFilter)) return false;
    ValueSourceRangeFilter other = (ValueSourceRangeFilter)o;

    if (!this.valueSource.equals(other.valueSource)
        || this.includeLower != other.includeLower
        || this.includeUpper != other.includeUpper
    ) { return false; }
    if (this.lowerVal != null ? !this.lowerVal.equals(other.lowerVal) : other.lowerVal != null) return false;
    if (this.upperVal != null ? !this.upperVal.equals(other.upperVal) : other.upperVal != null) return false;
    return true;
  }

  @Override
  public int hashCode() {
    int h = valueSource.hashCode();
    h += lowerVal != null ? lowerVal.hashCode() : 0x572353db;
    h = (h << 16) | (h >>> 16);  // rotate to distinguish lower from upper
    h += (upperVal != null ? (upperVal.hashCode()) : 0xe16fe9e7);
    h += (includeLower ? 0xdaa47978 : 0)
    + (includeUpper ? 0x9e634b57 : 0);
    return h;
  }
}
