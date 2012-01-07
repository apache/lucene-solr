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

package org.apache.solr.search;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.solr.analysis.ReversedWildcardFilter;
import org.apache.solr.analysis.ReversedWildcardFilterFactory;
import org.apache.solr.analysis.TokenFilterFactory;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;

// TODO: implement the analysis of simple fields with
// FieldType.toInternal() instead of going through the
// analyzer.  Should lead to faster query parsing.

/**
 * A variation on the Lucene QueryParser which knows about the field 
 * types and query time analyzers configured in Solr's schema.xml.
 *
 * <p>
 * This class also deviates from the Lucene QueryParser by using 
 * ConstantScore versions of RangeQuery and PrefixQuery to prevent 
 * TooManyClauses exceptions.
 * </p> 
 *
 * <p>
 * If the magic field name "<code>_val_</code>" is used in a term or 
 * phrase query, the value is parsed as a function.
 * </p>
 *
 * @see QueryParsing#parseFunction
 */
public class SolrQueryParser extends QueryParser {
  protected final IndexSchema schema;
  protected final QParser parser;
  protected final String defaultField;

  // implementation detail - caching ReversedWildcardFilterFactory based on type
  private Map<FieldType, ReversedWildcardFilterFactory> leadingWildcards;

   /**
   * Constructs a SolrQueryParser using the schema to understand the
   * formats and datatypes of each field.  Only the defaultSearchField
   * will be used from the IndexSchema (unless overridden),
   * &lt;solrQueryParser&gt; will not be used.
   *
   * @param schema Used for default search field name if defaultField is null and field information is used for analysis
   * @param defaultField default field used for unspecified search terms.  if null, the schema default field is used
   * @see IndexSchema#getDefaultSearchFieldName()
   */
  public SolrQueryParser(IndexSchema schema, String defaultField) {
    super(schema.getSolrConfig().luceneMatchVersion, defaultField == null ? schema.getDefaultSearchFieldName() : defaultField, schema.getQueryAnalyzer());
    this.schema = schema;
    this.parser  = null;
    this.defaultField = defaultField;
    setLowercaseExpandedTerms(false);
    setEnablePositionIncrements(true);
    setLowercaseExpandedTerms(false);
    setAllowLeadingWildcard(true);
  }

  public SolrQueryParser(QParser parser, String defaultField) {
    this(parser, defaultField, parser.getReq().getSchema().getQueryAnalyzer());
  }

  public SolrQueryParser(QParser parser, String defaultField, Analyzer analyzer) {
    super(parser.getReq().getSchema().getSolrConfig().luceneMatchVersion, defaultField, analyzer);
    this.schema = parser.getReq().getSchema();
    this.parser = parser;
    this.defaultField = defaultField;
    setLowercaseExpandedTerms(false);
    setEnablePositionIncrements(true);
    setLowercaseExpandedTerms(false);
    setAllowLeadingWildcard(true);
  }

  protected ReversedWildcardFilterFactory getReversedWildcardFilterFactory(FieldType fieldType) {
    if (leadingWildcards == null) leadingWildcards = new HashMap<FieldType, ReversedWildcardFilterFactory>();
    ReversedWildcardFilterFactory fac = leadingWildcards.get(fieldType);
    if (fac == null && leadingWildcards.containsKey(fac)) {
      return fac;
    }

    Analyzer a = fieldType.getAnalyzer();
    if (a instanceof TokenizerChain) {
      // examine the indexing analysis chain if it supports leading wildcards
      TokenizerChain tc = (TokenizerChain)a;
      TokenFilterFactory[] factories = tc.getTokenFilterFactories();
      for (TokenFilterFactory factory : factories) {
        if (factory instanceof ReversedWildcardFilterFactory) {
          fac = (ReversedWildcardFilterFactory)factory;
          break;
        }
      }
    }

    leadingWildcards.put(fieldType, fac);
    return fac;
  }

  
  private void checkNullField(String field) throws SolrException {
    if (field == null && defaultField == null) {
      throw new SolrException
        (SolrException.ErrorCode.BAD_REQUEST,
         "no field name specified in query and no defaultSearchField defined in schema.xml");
    }
  }

  protected String analyzeIfMultitermTermText(String field, String part, FieldType fieldType) {
    if (part == null) return part;

    SchemaField sf = schema.getFieldOrNull((field));
    if (sf == null || ! (fieldType instanceof TextField)) return part;
    return ((TextField)fieldType).analyzeMultiTerm(field, part, ((TextField)fieldType).getMultiTermAnalyzer());
  }

  @Override
  protected Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {
    checkNullField(field);
    // intercept magic field name of "_" to use as a hook for our
    // own functions.
    if (field.charAt(0) == '_') {
      if ("_val_".equals(field)) {
        if (parser==null) {
          return QueryParsing.parseFunction(queryText, schema);
        } else {
          QParser nested = parser.subQuery(queryText, "func");
          return nested.getQuery();
        }
      } else if ("_query_".equals(field) && parser != null) {
        return parser.subQuery(queryText, null).getQuery();
      }
    }
    SchemaField sf = schema.getFieldOrNull(field);
    if (sf != null) {
      FieldType ft = sf.getType();
      // delegate to type for everything except TextField
      if (ft instanceof TextField) {
        return super.getFieldQuery(field, queryText, quoted || ((TextField)ft).getAutoGeneratePhraseQueries());
      } else {
        return sf.getType().getFieldQuery(parser, sf, queryText);
      }
    }

    // default to a normal field query
    return super.getFieldQuery(field, queryText, quoted);
  }

  @Override
  protected Query getRangeQuery(String field, String part1, String part2, boolean inclusive) throws ParseException {
    checkNullField(field);
    SchemaField sf = schema.getField(field);
    return sf.getType().getRangeQuery(parser, sf, part1, part2, inclusive, inclusive);
  }

  @Override
  protected Query getPrefixQuery(String field, String termStr) throws ParseException {
    checkNullField(field);
    if (getLowercaseExpandedTerms()) {
      termStr = termStr.toLowerCase();
    }

    termStr = analyzeIfMultitermTermText(field, termStr, schema.getFieldType(field));

    // Solr has always used constant scoring for prefix queries.  This should return constant scoring by default.
    return newPrefixQuery(new Term(field, termStr));
  }
  @Override
  protected Query getWildcardQuery(String field, String termStr) throws ParseException {
    // *:* -> MatchAllDocsQuery
    if ("*".equals(field) && "*".equals(termStr)) {
      return newMatchAllDocsQuery();
    }
    FieldType fieldType = schema.getFieldType(field);
    termStr = analyzeIfMultitermTermText(field, termStr, fieldType);

    // can we use reversed wildcards in this field?
    ReversedWildcardFilterFactory factory = getReversedWildcardFilterFactory(fieldType);
    if (factory != null && factory.shouldReverse(termStr)) {
      int len = termStr.length();
      char[] chars = new char[len+1];
      chars[0] = factory.getMarkerChar();      
      termStr.getChars(0, len, chars, 1);
      ReversedWildcardFilter.reverse(chars, 1, len);
      termStr = new String(chars);
    }

    // Solr has always used constant scoring for wildcard queries.  This should return constant scoring by default.
    return newWildcardQuery(new Term(field, termStr));
  }
}
