package org.apache.lucene.queryparser.surround.query;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.lucene.search.Query;

/**
 * Forms an OR query of the provided query across multiple fields.
 */
public class FieldsQuery extends SrndQuery { /* mostly untested */
  private SrndQuery q;
  private List<String> fieldNames;
  private final char fieldOp;
  private final String OrOperatorName = "OR"; /* for expanded queries, not normally visible */
  
  public FieldsQuery(SrndQuery q, List<String> fieldNames, char fieldOp) {
    this.q = q;
    this.fieldNames = fieldNames;
    this.fieldOp = fieldOp;
  }
  
  public FieldsQuery(SrndQuery q, String fieldName, char fieldOp) {
    this.q = q;
    fieldNames = new ArrayList<String>();
    fieldNames.add(fieldName);
    this.fieldOp = fieldOp;
  }
  
  @Override
  public boolean isFieldsSubQueryAcceptable() {
    return false;
  }
  
  public Query makeLuceneQueryNoBoost(BasicQueryFactory qf) {
    if (fieldNames.size() == 1) { /* single field name: no new queries needed */
      return q.makeLuceneQueryFieldNoBoost(fieldNames.get(0), qf);
    } else { /* OR query over the fields */
      List<SrndQuery> queries = new ArrayList<SrndQuery>();
      Iterator<String> fni = getFieldNames().listIterator();
      SrndQuery qc;
      while (fni.hasNext()) {
        qc = q.clone();
        queries.add( new FieldsQuery( qc, fni.next(), fieldOp));
      }
      OrQuery oq = new OrQuery(queries,
                              true /* infix OR for field names */,
                              OrOperatorName);
      // System.out.println(getClass().toString() + ", fields expanded: " + oq.toString()); /* needs testing */
      return oq.makeLuceneQueryField(null, qf);
    }
  }

  @Override
  public Query makeLuceneQueryFieldNoBoost(String fieldName, BasicQueryFactory qf) {
    return makeLuceneQueryNoBoost(qf); /* use this.fieldNames instead of fieldName */
  }

  
  public List<String> getFieldNames() {return fieldNames;}

  public char getFieldOperator() { return fieldOp;}
  
  @Override
  public String toString() {
    StringBuilder r = new StringBuilder();
    r.append("(");
    fieldNamesToString(r);
    r.append(q.toString());
    r.append(")");
    return r.toString();
  }
  
  protected void fieldNamesToString(StringBuilder r) {
    Iterator<String> fni = getFieldNames().listIterator();
    while (fni.hasNext()) {
      r.append(fni.next());
      r.append(getFieldOperator());
    }
  }
}

