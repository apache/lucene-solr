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
package org.apache.solr.schema;

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.FieldCache;
import org.apache.solr.analysis.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.TextResponseWriter;
import org.apache.solr.request.XMLWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.function.*;

import java.io.IOException;
import java.util.Map;

/**
 * Provides field types to support for Lucene's Trie Range Queries.
 * See {@link org.apache.lucene.search.NumericRangeQuery} for more details.
 * It supports integer, float, long, double and date types.
 * <p/>
 * For each number being added to this field, multiple terms are generated as per the algorithm described in the above
 * link. The possible number of terms increases dramatically with higher precision steps (factor 2^precisionStep). For
 * the fast range search to work, trie fields must be indexed.
 * <p/>
 * Trie fields are sortable in numerical order and can be used in function queries.
 * <p/>
 * Note that if you use a precisionStep of 32 for int/float and 64 for long/double, then multiple terms will not be
 * generated, range search will be no faster than any other number field, but sorting will be possible.
 *
 * @version $Id$
 * @see org.apache.lucene.search.NumericRangeQuery
 * @since solr 1.4
 */
public class TrieField extends FieldType {
  public static final int DEFAULT_PRECISION_STEP = 8;

  protected int precisionStep = TrieField.DEFAULT_PRECISION_STEP;
  protected TrieTypes type;

  /**
   * Used for handling date types following the same semantics as DateField
   */
  private static final DateField dateField = new DateField();

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    String p = args.remove("precisionStep");
    if (p != null) {
      precisionStep = Integer.parseInt(p);
    }
    String t = args.remove("type");
    if (t == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Invalid type specified in schema.xml for field: " + args.get("name"));
    } else {
      try {
        type = TrieTypes.valueOf(t.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Invalid type specified in schema.xml for field: " + args.get("name"), e);
      }
    }
    
    CharFilterFactory[] filterFactories = new CharFilterFactory[0];
    TokenFilterFactory[] tokenFilterFactories = new TokenFilterFactory[0];
    analyzer = new TokenizerChain(filterFactories, new TrieTokenizerFactory(type, precisionStep), tokenFilterFactories);
    // for query time we only need one token, so we use the biggest possible precisionStep:
    queryAnalyzer = new TokenizerChain(filterFactories, new TrieTokenizerFactory(type, Integer.MAX_VALUE), tokenFilterFactories);
  }

  @Override
  public Object toObject(Fieldable f) {
    String s = f.stringValue();
    switch (type) {
      case INTEGER:
        return Integer.parseInt(s);
      case FLOAT:
        return Float.parseFloat(s);
      case LONG:
        return Long.parseLong(s);
      case DOUBLE:
        return Double.parseDouble(s);
      case DATE:
        return dateField.toObject(f);
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + f.name());
    }
  }

  public SortField getSortField(SchemaField field, boolean top) {
    switch (type) {
      case INTEGER:
        return new SortField(field.getName(), FieldCache.NUMERIC_UTILS_INT_PARSER, top);
      case FLOAT:
        return new SortField(field.getName(), FieldCache.NUMERIC_UTILS_FLOAT_PARSER, top);
      case DATE:
      case LONG:
        return new SortField(field.getName(), FieldCache.NUMERIC_UTILS_LONG_PARSER, top);
      case DOUBLE:
        return new SortField(field.getName(), FieldCache.NUMERIC_UTILS_DOUBLE_PARSER, top);
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + field.name);
    }
  }

  public ValueSource getValueSource(SchemaField field) {
    switch (type) {
      case INTEGER:
        return new IntFieldSource(field.getName(), FieldCache.NUMERIC_UTILS_INT_PARSER);
      case FLOAT:
        return new FloatFieldSource(field.getName(), FieldCache.NUMERIC_UTILS_FLOAT_PARSER);
      case DATE:
      case LONG:
        return new LongFieldSource(field.getName(), FieldCache.NUMERIC_UTILS_LONG_PARSER);
      case DOUBLE:
        return new DoubleFieldSource(field.getName(), FieldCache.NUMERIC_UTILS_DOUBLE_PARSER);
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + field.name);
    }
  }

  public void write(XMLWriter xmlWriter, String name, Fieldable f) throws IOException {
    xmlWriter.writeVal(name, toObject(f));
  }

  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException {
    writer.writeVal(name, toObject(f));
  }

  @Override
  public boolean isTokenized() {
    return true;
  }

  /**
   * @return the precisionStep used to index values into the field
   */
  public int getPrecisionStep() {
    return precisionStep;
  }

  /**
   * @return the type of this field
   */
  public TrieTypes getType() {
    return type;
  }

  @Override
  public Query getRangeQuery(QParser parser, String field, String min, String max, boolean minInclusive, boolean maxInclusive) {
    Query query = null;
    switch (type) {
      case INTEGER:
        query = NumericRangeQuery.newIntRange(field, precisionStep,
                min == null ? null : Integer.parseInt(min),
                max == null ? null : Integer.parseInt(max),
                minInclusive, maxInclusive);
        break;
      case FLOAT:
        query = NumericRangeQuery.newFloatRange(field, precisionStep,
                min == null ? null : Float.parseFloat(min),
                max == null ? null : Float.parseFloat(max),
                minInclusive, maxInclusive);
        break;
      case LONG:
        query = NumericRangeQuery.newLongRange(field, precisionStep,
                min == null ? null : Long.parseLong(min),
                max == null ? null : Long.parseLong(max),
                minInclusive, maxInclusive);
        break;
      case DOUBLE:
        query = NumericRangeQuery.newDoubleRange(field, precisionStep,
                min == null ? null : Double.parseDouble(min),
                max == null ? null : Double.parseDouble(max),
                minInclusive, maxInclusive);
        break;
      case DATE:
        query = NumericRangeQuery.newLongRange(field, precisionStep,
                min == null ? null : dateField.parseMath(null, min).getTime(),
                max == null ? null : dateField.parseMath(null, max).getTime(),
                minInclusive, maxInclusive);
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field");
    }

    return query;
  }

  @Override
  public String toInternal(String val) {
    return super.toInternal(val);
  }

  @Override
  public String toExternal(Fieldable f) {
    return super.toExternal(f);
  }

  @Override
  public String indexedToReadable(String indexedForm) {
    return super.indexedToReadable(indexedForm);
  }

  @Override
  public String storedToIndexed(Fieldable f) {
    return super.storedToIndexed(f);
  }

  public enum TrieTypes {
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    DATE
  }
}
