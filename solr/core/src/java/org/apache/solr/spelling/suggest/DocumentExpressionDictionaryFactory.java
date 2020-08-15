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
package org.apache.solr.spelling.suggest;

import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.suggest.DocumentValueSourceDictionary;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Factory for {@link org.apache.lucene.search.suggest.DocumentValueSourceDictionary}
 */
public class DocumentExpressionDictionaryFactory extends DictionaryFactory {

  /** Label for defining field to use for terms */
  public static final String FIELD = "field";
  
  /** Label for defining payloadField to use for terms (optional) */
  public static final String PAYLOAD_FIELD = "payloadField";
  
  /** Label for defining expression to evaluate the weight for the terms */
  public static final String WEIGHT_EXPRESSION = "weightExpression";
  
  /** Label used to define the name of the
   * sortField used in the {@link #WEIGHT_EXPRESSION} */
  public static final String SORT_FIELD = "sortField";
  
  @Override
  public Dictionary create(SolrCore core, SolrIndexSearcher searcher) {
    if(params == null) {
      // should not happen; implies setParams was not called
      throw new IllegalStateException("Value of params not set");
    }
    
    String field = (String) params.get(FIELD);
    String payloadField = (String) params.get(PAYLOAD_FIELD);
    String weightExpression = (String) params.get(WEIGHT_EXPRESSION);
    Set<SortField> sortFields = new HashSet<>();
    
    if (field == null) {
      throw new IllegalArgumentException(FIELD + " is a mandatory parameter");
    }
    
    if (weightExpression == null) {
      throw new IllegalArgumentException(WEIGHT_EXPRESSION + " is a mandatory parameter");
    }
    
    for(int i = 0; i < params.size(); i++) {
      if (params.getName(i).equals(SORT_FIELD)) {
        String sortFieldName = (String) params.getVal(i);

        sortFields.add(getSortField(core, sortFieldName));
      }
    }
   
    return new DocumentValueSourceDictionary(searcher.getIndexReader(), field, fromExpression(weightExpression,
        sortFields), payloadField);
  }

  public LongValuesSource fromExpression(String weightExpression, Set<SortField> sortFields) {
    Expression expression = null;
    try {
      expression = JavascriptCompiler.compile(weightExpression);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
    SimpleBindings bindings = new SimpleBindings();
    for (SortField sortField : sortFields) {
      bindings.add(sortField.getField(), fromSortField(sortField));
    }
    return expression.getDoubleValuesSource(bindings).toLongValuesSource();
  }

  private DoubleValuesSource fromSortField(SortField field) {
    switch(field.getType()) {
      case INT:
        return DoubleValuesSource.fromIntField(field.getField());
      case LONG:
        return DoubleValuesSource.fromLongField(field.getField());
      case FLOAT:
        return DoubleValuesSource.fromFloatField(field.getField());
      case DOUBLE:
        return DoubleValuesSource.fromDoubleField(field.getField());
      case SCORE:
        return DoubleValuesSource.SCORES;
      default:
        throw new UnsupportedOperationException();
    }
  }
  
  private SortField getSortField(SolrCore core, String sortFieldName) {
    return core.getLatestSchema().getField(sortFieldName).getSortField(true);
  }
  
}
