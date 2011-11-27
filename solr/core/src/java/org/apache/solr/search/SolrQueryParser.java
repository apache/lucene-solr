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
import java.util.Map.Entry;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.BasicOperations;
import org.apache.lucene.util.automaton.SpecialOperations;
import org.apache.lucene.analysis.Analyzer;
import org.apache.solr.analysis.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;


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
 */
public class SolrQueryParser extends QueryParser {
  protected final IndexSchema schema;
  protected final QParser parser;
  protected final String defaultField;

  // implementation detail - caching ReversedWildcardFilterFactory based on type
  private Map<FieldType, ReversedWildcardFilterFactory> leadingWildcards;

  public SolrQueryParser(QParser parser, String defaultField) {
    this(parser, defaultField, parser.getReq().getSchema().getQueryAnalyzer());
  }

  public SolrQueryParser(QParser parser, String defaultField, Analyzer analyzer) {
    super(parser.getReq().getCore().getSolrConfig().luceneMatchVersion, defaultField, analyzer);
    this.schema = parser.getReq().getSchema();
    this.parser = parser;
    this.defaultField = defaultField;
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
    String out = TextField.analyzeMultiTerm(field, part, ((TextField)fieldType).getMultiTermAnalyzer()).utf8ToString();
    // System.out.println("INPUT="+part + " OUTPUT="+out);
    return out;
  }

  @Override
  protected Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {
    checkNullField(field);
    // intercept magic field name of "_" to use as a hook for our
    // own functions.
    if (field.charAt(0) == '_') {
      if ("_val_".equals(field)) {
        QParser nested = parser.subQuery(queryText, "func");
        return nested.getQuery();
      } else if ("_query_".equals(field) && parser != null) {
        return parser.subQuery(queryText, null).getQuery();
      }
    }
    SchemaField sf = schema.getFieldOrNull(field);
    if (sf != null) {
      FieldType ft = sf.getType();
      // delegate to type for everything except tokenized fields
      if (ft.isTokenized()) {
        return super.getFieldQuery(field, queryText, quoted || (ft instanceof TextField && ((TextField)ft).getAutoGeneratePhraseQueries()));
      } else {
        return sf.getType().getFieldQuery(parser, sf, queryText);
      }
    }

    // default to a normal field query
    return super.getFieldQuery(field, queryText, quoted);
  }

  @Override
  protected Query getRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) throws ParseException {
    checkNullField(field);
    SchemaField sf = schema.getField(field);
    return sf.getType().getRangeQuery(parser, sf, part1, part2, startInclusive, endInclusive);
  }

  @Override
  protected Query getPrefixQuery(String field, String termStr) throws ParseException {
    checkNullField(field);

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
    if (factory != null) {
      Term term = new Term(field, termStr);
      // fsa representing the query
      Automaton automaton = WildcardQuery.toAutomaton(term);
      // TODO: we should likely use the automaton to calculate shouldReverse, too.
      if (factory.shouldReverse(termStr)) {
        automaton = BasicOperations.concatenate(automaton, BasicAutomata.makeChar(factory.getMarkerChar()));
        SpecialOperations.reverse(automaton);
      } else { 
        // reverse wildcardfilter is active: remove false positives
        // fsa representing false positives (markerChar*)
        Automaton falsePositives = BasicOperations.concatenate(
            BasicAutomata.makeChar(factory.getMarkerChar()), 
            BasicAutomata.makeAnyString());
        // subtract these away
        automaton = BasicOperations.minus(automaton, falsePositives);
      }
      return new AutomatonQuery(term, automaton) {
        // override toString so its completely transparent
        @Override
        public String toString(String field) {
          StringBuilder buffer = new StringBuilder();
          if (!getField().equals(field)) {
            buffer.append(getField());
            buffer.append(":");
          }
          buffer.append(term.text());
          buffer.append(ToStringUtils.boost(getBoost()));
          return buffer.toString();
        }
      };
    }

    // Solr has always used constant scoring for wildcard queries.  This should return constant scoring by default.
    return newWildcardQuery(new Term(field, termStr));
  }

  @Override
  protected Query getRegexpQuery(String field, String termStr) throws ParseException
  {
    termStr = analyzeIfMultitermTermText(field, termStr, schema.getFieldType(field));
    return newRegexpQuery(new Term(field, termStr));
  }
}
