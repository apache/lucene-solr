package org.apache.lucene.search.spans;

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
import java.util.Collection;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.util.ToStringUtils;

/**
 * <p>Wrapper to allow {@link SpanQuery} objects participate in composite 
 * single-field SpanQueries by 'lying' about their search field. That is, 
 * the masked SpanQuery will function as normal, 
 * but {@link SpanQuery#getField()} simply hands back the value supplied 
 * in this class's constructor.</p>
 * 
 * <p>This can be used to support Queries like {@link SpanNearQuery} or 
 * {@link SpanOrQuery} across different fields, which is not ordinarily 
 * permitted.</p>
 * 
 * <p>This can be useful for denormalized relational data: for example, when 
 * indexing a document with conceptually many 'children': </p>
 * 
 * <pre>
 *  teacherid: 1
 *  studentfirstname: james
 *  studentsurname: jones
 *  
 *  teacherid: 2
 *  studenfirstname: james
 *  studentsurname: smith
 *  studentfirstname: sally
 *  studentsurname: jones
 * </pre>
 * 
 * <p>a SpanNearQuery with a slop of 0 can be applied across two 
 * {@link SpanTermQuery} objects as follows:
 * <pre>
 *    SpanQuery q1  = new SpanTermQuery(new Term("studentfirstname", "james"));
 *    SpanQuery q2  = new SpanTermQuery(new Term("studentsurname", "jones"));
 *    SpanQuery q2m new FieldMaskingSpanQuery(q2, "studentfirstname");
 *    Query q = new SpanNearQuery(new SpanQuery[]{q1, q2m}, -1, false);
 * </pre>
 * to search for 'studentfirstname:james studentsurname:jones' and find 
 * teacherid 1 without matching teacherid 2 (which has a 'james' in position 0 
 * and 'jones' in position 1). </p>
 * 
 * <p>Note: as {@link #getField()} returns the masked field, scoring will be 
 * done using the norms of the field name supplied. This may lead to unexpected
 * scoring behaviour.</p>
 */
public class FieldMaskingSpanQuery extends SpanQuery {
  private SpanQuery maskedQuery;
  private String field;
    
  public FieldMaskingSpanQuery(SpanQuery maskedQuery, String maskedField) {
    this.maskedQuery = maskedQuery;
    this.field = maskedField;
  }

  public String getField() {
    return field;
  }

  public SpanQuery getMaskedQuery() {
    return maskedQuery;
  }

  // :NOTE: getBoost and setBoost are not proxied to the maskedQuery
  // ...this is done to be more consistent with things like SpanFirstQuery
  
  public Spans getSpans(IndexReader reader) throws IOException {
    return maskedQuery.getSpans(reader);
  }

  /** @deprecated use {@link #extractTerms(Set)} instead. */
  public Collection getTerms() {
    return maskedQuery.getTerms();
  }
  
  public void extractTerms(Set terms) {
    maskedQuery.extractTerms(terms);
  }  

  public Weight createWeight(Searcher searcher) throws IOException {
    return maskedQuery.createWeight(searcher);
  }

  public Similarity getSimilarity(Searcher searcher) {
    return maskedQuery.getSimilarity(searcher);
  }

  public Query rewrite(IndexReader reader) throws IOException {
    FieldMaskingSpanQuery clone = null;

    SpanQuery rewritten = (SpanQuery) maskedQuery.rewrite(reader);
    if (rewritten != maskedQuery) {
      clone = (FieldMaskingSpanQuery) this.clone();
      clone.maskedQuery = rewritten;
    }

    if (clone != null) {
      return clone;
    } else {
      return this;
    }
  }

  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    buffer.append("mask(");
    buffer.append(maskedQuery.toString(field));
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    buffer.append(" as ");
    buffer.append(this.field);
    return buffer.toString();
  }
  
  public boolean equals(Object o) {
    if (!(o instanceof FieldMaskingSpanQuery))
      return false;
    FieldMaskingSpanQuery other = (FieldMaskingSpanQuery) o;
    return (this.getField().equals(other.getField())
            && (this.getBoost() == other.getBoost())
            && this.getMaskedQuery().equals(other.getMaskedQuery()));

  }
  
  public int hashCode() {
    return getMaskedQuery().hashCode()
      ^ getField().hashCode()
      ^ Float.floatToRawIntBits(getBoost());
  }
}
