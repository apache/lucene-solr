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
import org.apache.lucene.search.trie.IntTrieRangeQuery;
import org.apache.lucene.search.trie.LongTrieRangeQuery;
import org.apache.lucene.search.trie.TrieUtils;
import org.apache.solr.analysis.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.TextResponseWriter;
import org.apache.solr.request.XMLWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.function.*;

import java.io.IOException;
import java.util.Map;

/**
 * Provides field types to support for Lucene's Trie Range Queries. See {@linkplain org.apache.lucene.search.trie
 * package description} for more details. It supports integer, float, long, double and date types.
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
 * @see org.apache.lucene.search.trie.TrieUtils
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
    analyzer = new TokenizerChain(filterFactories, new TrieIndexTokenizerFactory(type, precisionStep), tokenFilterFactories);
    queryAnalyzer = new TokenizerChain(filterFactories, new TrieQueryTokenizerFactory(type), tokenFilterFactories);
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
      case FLOAT:
        return TrieUtils.getIntSortField(field.getName(), top);
      case LONG:
      case DOUBLE:
      case DATE:
        return TrieUtils.getLongSortField(field.getName(), top);
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + field.name);
    }
  }

  public ValueSource getValueSource(SchemaField field) {
    switch (type) {
      case INTEGER:
        return new IntFieldSource(field.getName(), TrieUtils.FIELD_CACHE_INT_PARSER);
      case FLOAT:
        return new FloatFieldSource(field.getName(), TrieUtils.FIELD_CACHE_FLOAT_PARSER);
      case LONG:
        return new LongFieldSource(field.getName(), TrieUtils.FIELD_CACHE_LONG_PARSER);
      case DOUBLE:
        return new DoubleFieldSource(field.getName(), TrieUtils.FIELD_CACHE_DOUBLE_PARSER);
      case DATE:
        return new LongFieldSource(field.getName(), TrieUtils.FIELD_CACHE_LONG_PARSER);
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
        query = new IntTrieRangeQuery(field, precisionStep,
                min == null ? null : Integer.parseInt(min),
                max == null ? null : Integer.parseInt(max),
                minInclusive, maxInclusive);
        break;
      case FLOAT:
        query = new IntTrieRangeQuery(field, precisionStep,
                min == null ? null : TrieUtils.floatToSortableInt(Float.parseFloat(min)),
                max == null ? null : TrieUtils.floatToSortableInt(Float.parseFloat(max)),
                minInclusive, maxInclusive);
        break;
      case LONG:
        query = new LongTrieRangeQuery(field, precisionStep,
                min == null ? null : Long.parseLong(min),
                max == null ? null : Long.parseLong(max),
                minInclusive, maxInclusive);
        break;
      case DOUBLE:
        query = new LongTrieRangeQuery(field, precisionStep,
                min == null ? null : TrieUtils.doubleToSortableLong(Double.parseDouble(min)),
                max == null ? null : TrieUtils.doubleToSortableLong(Double.parseDouble(max)),
                minInclusive, maxInclusive);
        break;
      case DATE:
        query = new LongTrieRangeQuery(field, precisionStep,
                min == null ? null : dateField.parseMath(null, min).getTime(),
                max == null ? null : dateField.parseMath(null, max).getTime(),
                minInclusive, maxInclusive);
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field");
    }

    return query;
  }

  public enum TrieTypes {
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    DATE
  }
}
