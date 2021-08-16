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
package org.apache.solr.schema;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.RankQParserPlugin;
import org.apache.solr.uninverting.UninvertingReader.Type;

/**
 * <p>
 * {@code RankField}s can be used to store scoring factors to improve document ranking. They should be used
 * in combination with {@link RankQParserPlugin}. To use:
 * </p>
 * <p>
 * Define the {@code RankField} {@code fieldType} in your schema:
 * </p>
 * <pre class="prettyprint">
 * &lt;fieldType name="rank" class="solr.RankField" /&gt;
 * </pre>
 * <p>
 * Add fields to the schema, i.e.:
 * </p>
 * <pre class="prettyprint">
 * &lt;field name="pagerank" type="rank" /&gt;
 * </pre>
 * 
 * Query using the {@link RankQParserPlugin}, for example
 * <pre class="prettyprint">
 * http://localhost:8983/solr/techproducts?q=memory _query_:{!rank f='pagerank', function='log' scalingFactor='1.2'}
 * </pre>
 * 
 * @see RankQParserPlugin
 * @lucene.experimental
 * @since 8.6
 */
public class RankField extends FieldType {
  
  /*
   * While the user can create multiple RankFields, internally we use a single Lucene field,
   * and we map the Solr field name to the "feature" in Lucene's FeatureField. This is mainly
   * to simplify the user experience.
   */
  public static final String INTERNAL_RANK_FIELD_NAME = "_rank_";

  @Override
  public Type getUninversionType(SchemaField sf) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
  }
  
  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
    super.init(schema, args);
    if (schema.getFieldOrNull(INTERNAL_RANK_FIELD_NAME) != null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "A field named \"" + INTERNAL_RANK_FIELD_NAME + "\" can't be defined in the schema");
    }
    for (int prop:new int[] {STORED, DOC_VALUES, OMIT_TF_POSITIONS, SORT_MISSING_FIRST, SORT_MISSING_LAST}) {
      if ((trueProperties & prop) != 0) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Property \"" + getPropertyName(prop) + "\" can't be set to true in RankFields");
      }
    }
    for (int prop:new int[] {UNINVERTIBLE, INDEXED, MULTIVALUED}) {
      if ((falseProperties & prop) != 0) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Property \"" + getPropertyName(prop) + "\" can't be set to false in RankFields");
      }
    }
    properties &= ~(UNINVERTIBLE | STORED | DOC_VALUES);
    
  }

  @Override
  protected IndexableField createField(String name, String val, IndexableFieldType type) {
    if (val == null || val.isEmpty()) {
      return null;
    }
    float featureValue;
    try {
      featureValue = Float.parseFloat(val);
    } catch (NumberFormatException nfe) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error while creating field '" + name + "' from value '" + val + "'. Expecting float.", nfe);
    }
    // Internally, we always use the same field
    return new FeatureField(INTERNAL_RANK_FIELD_NAME, name, featureValue);
  }
  
  @Override
  public Query getExistenceQuery(QParser parser, SchemaField field) {
    return new TermQuery(new Term(INTERNAL_RANK_FIELD_NAME, field.getName()));
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "Only a \"*\" term query can be done on RankFields");
  }
  
  @Override
  protected Query getSpecializedRangeQuery(QParser parser, SchemaField field, String part1, String part2,
      boolean minInclusive, boolean maxInclusive) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "Range queries not supported on RankFields");
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    // We could use FeatureField.newFeatureSort()
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
        "can not sort on a rank field: " + field.getName());
  }

}
