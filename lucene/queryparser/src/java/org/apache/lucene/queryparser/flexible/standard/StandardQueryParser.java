package org.apache.lucene.queryparser.flexible.standard;

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

import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TooManyListenersException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.QueryParserHelper;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.standard.config.FuzzyConfig;
import org.apache.lucene.queryparser.flexible.standard.config.NumericConfig;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;
import org.apache.lucene.queryparser.flexible.standard.parser.StandardSyntaxParser;
import org.apache.lucene.queryparser.flexible.standard.processors.StandardQueryNodeProcessorPipeline;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;

/**
 * <p>
 * This class is a helper that enables users to easily use the Lucene query
 * parser.
 * </p>
 * <p>
 * To construct a Query object from a query string, use the
 * {@link #parse(String, String)} method:
 * <ul>
 * StandardQueryParser queryParserHelper = new StandardQueryParser(); <br/>
 * Query query = queryParserHelper.parse("a AND b", "defaultField");
 * </ul>
 * <p>
 * To change any configuration before parsing the query string do, for example:
 * <p/>
 * <ul>
 * // the query config handler returned by {@link StandardQueryParser} is a
 * {@link StandardQueryConfigHandler} <br/>
 * queryParserHelper.getQueryConfigHandler().setAnalyzer(new
 * WhitespaceAnalyzer());
 * </ul>
 * <p>
 * The syntax for query strings is as follows (copied from the old QueryParser
 * javadoc):
 * <ul>
 * A Query is a series of clauses. A clause may be prefixed by:
 * <ul>
 * <li>a plus (<code>+</code>) or a minus (<code>-</code>) sign, indicating that
 * the clause is required or prohibited respectively; or
 * <li>a term followed by a colon, indicating the field to be searched. This
 * enables one to construct queries which search multiple fields.
 * </ul>
 * 
 * A clause may be either:
 * <ul>
 * <li>a term, indicating all the documents that contain this term; or
 * <li>a nested query, enclosed in parentheses. Note that this may be used with
 * a <code>+</code>/<code>-</code> prefix to require any of a set of terms.
 * </ul>
 * 
 * Thus, in BNF, the query grammar is:
 * 
 * <pre>
 *   Query  ::= ( Clause )*
 *   Clause ::= [&quot;+&quot;, &quot;-&quot;] [&lt;TERM&gt; &quot;:&quot;] ( &lt;TERM&gt; | &quot;(&quot; Query &quot;)&quot; )
 * </pre>
 * 
 * <p>
 * Examples of appropriately formatted queries can be found in the <a
 * href="{@docRoot}/org/apache/lucene/queryparser/classic/package-summary.html#package_description">
 * query syntax documentation</a>.
 * </p>
 * </ul>
 * <p>
 * The text parser used by this helper is a {@link StandardSyntaxParser}.
 * <p/>
 * <p>
 * The query node processor used by this helper is a
 * {@link StandardQueryNodeProcessorPipeline}.
 * <p/>
 * <p>
 * The builder used by this helper is a {@link StandardQueryTreeBuilder}.
 * <p/>
 * 
 * @see StandardQueryParser
 * @see StandardQueryConfigHandler
 * @see StandardSyntaxParser
 * @see StandardQueryNodeProcessorPipeline
 * @see StandardQueryTreeBuilder
 */
public class StandardQueryParser extends QueryParserHelper implements CommonQueryParserConfiguration {
  
  /**
   * Constructs a {@link StandardQueryParser} object.
   */
  public StandardQueryParser() {
    super(new StandardQueryConfigHandler(), new StandardSyntaxParser(),
        new StandardQueryNodeProcessorPipeline(null),
        new StandardQueryTreeBuilder());
    setEnablePositionIncrements(true);
  }
  
  /**
   * Constructs a {@link StandardQueryParser} object and sets an
   * {@link Analyzer} to it. The same as:
   * 
   * <ul>
   * StandardQueryParser qp = new StandardQueryParser();
   * qp.getQueryConfigHandler().setAnalyzer(analyzer);
   * </ul>
   * 
   * @param analyzer
   *          the analyzer to be used by this query parser helper
   */
  public StandardQueryParser(Analyzer analyzer) {
    this();
    
    this.setAnalyzer(analyzer);
  }
  
  @Override
  public String toString() {
    return "<StandardQueryParser config=\"" + this.getQueryConfigHandler()
        + "\"/>";
  }
  
  /**
   * Overrides {@link QueryParserHelper#parse(String, String)} so it casts the
   * return object to {@link Query}. For more reference about this method, check
   * {@link QueryParserHelper#parse(String, String)}.
   * 
   * @param query
   *          the query string
   * @param defaultField
   *          the default field used by the text parser
   * 
   * @return the object built from the query
   * 
   * @throws QueryNodeException
   *           if something wrong happens along the three phases
   */
  @Override
  public Query parse(String query, String defaultField)
      throws QueryNodeException {
    
    return (Query) super.parse(query, defaultField);
    
  }
  
  /**
   * Gets implicit operator setting, which will be either {@link Operator#AND}
   * or {@link Operator#OR}.
   */
  public StandardQueryConfigHandler.Operator getDefaultOperator() {
    return getQueryConfigHandler().get(ConfigurationKeys.DEFAULT_OPERATOR);
  }
  
  /**
   * Sets the boolean operator of the QueryParser. In default mode (
   * {@link Operator#OR}) terms without any modifiers are considered optional:
   * for example <code>capital of Hungary</code> is equal to
   * <code>capital OR of OR Hungary</code>.<br/>
   * In {@link Operator#AND} mode terms are considered to be in conjunction: the
   * above mentioned query is parsed as <code>capital AND of AND Hungary</code>
   */
  public void setDefaultOperator(StandardQueryConfigHandler.Operator operator) {
    getQueryConfigHandler().set(ConfigurationKeys.DEFAULT_OPERATOR, operator);
  }
  
  /**
   * Set to <code>true</code> to allow leading wildcard characters.
   * <p>
   * When set, <code>*</code> or <code>?</code> are allowed as the first
   * character of a PrefixQuery and WildcardQuery. Note that this can produce
   * very slow queries on big indexes.
   * <p>
   * Default: false.
   */
  @Override
  public void setLowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
    getQueryConfigHandler().set(ConfigurationKeys.LOWERCASE_EXPANDED_TERMS, lowercaseExpandedTerms);
  }
  
  /**
   * @see #setLowercaseExpandedTerms(boolean)
   */
  @Override
  public boolean getLowercaseExpandedTerms() {
    Boolean lowercaseExpandedTerms = getQueryConfigHandler().get(ConfigurationKeys.LOWERCASE_EXPANDED_TERMS);
    
    if (lowercaseExpandedTerms == null) {
      return true;
      
    } else {
      return lowercaseExpandedTerms;
    }
    
  }
  
  /**
   * Set to <code>true</code> to allow leading wildcard characters.
   * <p>
   * When set, <code>*</code> or <code>?</code> are allowed as the first
   * character of a PrefixQuery and WildcardQuery. Note that this can produce
   * very slow queries on big indexes.
   * <p>
   * Default: false.
   */
  @Override
  public void setAllowLeadingWildcard(boolean allowLeadingWildcard) {
    getQueryConfigHandler().set(ConfigurationKeys.ALLOW_LEADING_WILDCARD, allowLeadingWildcard);
  }
  
  /**
   * Set to <code>true</code> to enable position increments in result query.
   * <p>
   * When set, result phrase and multi-phrase queries will be aware of position
   * increments. Useful when e.g. a StopFilter increases the position increment
   * of the token that follows an omitted token.
   * <p>
   * Default: false.
   */
  @Override
  public void setEnablePositionIncrements(boolean enabled) {
    getQueryConfigHandler().set(ConfigurationKeys.ENABLE_POSITION_INCREMENTS, enabled);
  }
  
  /**
   * @see #setEnablePositionIncrements(boolean)
   */
  @Override
  public boolean getEnablePositionIncrements() {
    Boolean enablePositionsIncrements = getQueryConfigHandler().get(ConfigurationKeys.ENABLE_POSITION_INCREMENTS);
    
    if (enablePositionsIncrements == null) {
       return false;
       
    } else {
      return enablePositionsIncrements;
    }
    
  }
  
  /**
   * By default, it uses
   * {@link MultiTermQuery#CONSTANT_SCORE_AUTO_REWRITE_DEFAULT} when creating a
   * prefix, wildcard and range queries. This implementation is generally
   * preferable because it a) Runs faster b) Does not have the scarcity of terms
   * unduly influence score c) avoids any {@link TooManyListenersException}
   * exception. However, if your application really needs to use the
   * old-fashioned boolean queries expansion rewriting and the above points are
   * not relevant then use this change the rewrite method.
   */
  @Override
  public void setMultiTermRewriteMethod(MultiTermQuery.RewriteMethod method) {
    getQueryConfigHandler().set(ConfigurationKeys.MULTI_TERM_REWRITE_METHOD, method);
  }
  
  /**
   * @see #setMultiTermRewriteMethod(org.apache.lucene.search.MultiTermQuery.RewriteMethod)
   */
  @Override
  public MultiTermQuery.RewriteMethod getMultiTermRewriteMethod() {
    return getQueryConfigHandler().get(ConfigurationKeys.MULTI_TERM_REWRITE_METHOD);
  }
  
  /**
   * Set the fields a query should be expanded to when the field is
   * <code>null</code>
   * 
   * @param fields the fields used to expand the query
   */
  public void setMultiFields(CharSequence[] fields) {
    
    if (fields == null) {
      fields = new CharSequence[0];
    }

    getQueryConfigHandler().set(ConfigurationKeys.MULTI_FIELDS, fields);
    
  }
  
  /**
   * Returns the fields used to expand the query when the field for a
   * certain query is <code>null</code>
   * 
   * @param fields the fields used to expand the query
   */
  public void getMultiFields(CharSequence[] fields) {
    getQueryConfigHandler().get(ConfigurationKeys.MULTI_FIELDS);
  }

  /**
   * Set the prefix length for fuzzy queries. Default is 0.
   * 
   * @param fuzzyPrefixLength
   *          The fuzzyPrefixLength to set.
   */
  @Override
  public void setFuzzyPrefixLength(int fuzzyPrefixLength) {
    QueryConfigHandler config = getQueryConfigHandler();
    FuzzyConfig fuzzyConfig = config.get(ConfigurationKeys.FUZZY_CONFIG);
    
    if (fuzzyConfig == null) {
      fuzzyConfig = new FuzzyConfig();
      config.set(ConfigurationKeys.FUZZY_CONFIG, fuzzyConfig);
    }

    fuzzyConfig.setPrefixLength(fuzzyPrefixLength);
    
  }
  
  public void setNumericConfigMap(Map<String,NumericConfig> numericConfigMap) {
    getQueryConfigHandler().set(ConfigurationKeys.NUMERIC_CONFIG_MAP, numericConfigMap);
  }
  
  public Map<String,NumericConfig> getNumericConfigMap() {
    return getQueryConfigHandler().get(ConfigurationKeys.NUMERIC_CONFIG_MAP);
  }
  
  /**
   * Set locale used by date range parsing.
   */
  @Override
  public void setLocale(Locale locale) {
    getQueryConfigHandler().set(ConfigurationKeys.LOCALE, locale);
  }
  
  /**
   * Returns current locale, allowing access by subclasses.
   */
  @Override
  public Locale getLocale() {
    return getQueryConfigHandler().get(ConfigurationKeys.LOCALE);
  }
  
  @Override
  public void setTimeZone(TimeZone timeZone) {
    getQueryConfigHandler().set(ConfigurationKeys.TIMEZONE, timeZone);
  }
  
  @Override
  public TimeZone getTimeZone() {
    return getQueryConfigHandler().get(ConfigurationKeys.TIMEZONE);
  }
  
  /**
   * Sets the default slop for phrases. If zero, then exact phrase matches are
   * required. Default value is zero.
   * 
   * @deprecated renamed to {@link #setPhraseSlop(int)}
   */
  @Deprecated
  public void setDefaultPhraseSlop(int defaultPhraseSlop) {
    getQueryConfigHandler().set(ConfigurationKeys.PHRASE_SLOP, defaultPhraseSlop);
  }
  
  /**
   * Sets the default slop for phrases. If zero, then exact phrase matches are
   * required. Default value is zero.
   */
  @Override
  public void setPhraseSlop(int defaultPhraseSlop) {
    getQueryConfigHandler().set(ConfigurationKeys.PHRASE_SLOP, defaultPhraseSlop);
  }

  public void setAnalyzer(Analyzer analyzer) {
    getQueryConfigHandler().set(ConfigurationKeys.ANALYZER, analyzer);
  }
  
  @Override
  public Analyzer getAnalyzer() {
    return getQueryConfigHandler().get(ConfigurationKeys.ANALYZER);       
  }
  
  /**
   * @see #setAllowLeadingWildcard(boolean)
   */
  @Override
  public boolean getAllowLeadingWildcard() {
    Boolean allowLeadingWildcard = getQueryConfigHandler().get(ConfigurationKeys.ALLOW_LEADING_WILDCARD);
    
    if (allowLeadingWildcard == null) {
      return false;
      
    } else {
      return allowLeadingWildcard;
    }
  }
  
  /**
   * Get the minimal similarity for fuzzy queries.
   */
  @Override
  public float getFuzzyMinSim() {
    FuzzyConfig fuzzyConfig = getQueryConfigHandler().get(ConfigurationKeys.FUZZY_CONFIG);
    
    if (fuzzyConfig == null) {
      return FuzzyQuery.defaultMinSimilarity;
    } else {
      return fuzzyConfig.getMinSimilarity();
    }
  }
  
  /**
   * Get the prefix length for fuzzy queries.
   * 
   * @return Returns the fuzzyPrefixLength.
   */
  @Override
  public int getFuzzyPrefixLength() {
    FuzzyConfig fuzzyConfig = getQueryConfigHandler().get(ConfigurationKeys.FUZZY_CONFIG);
    
    if (fuzzyConfig == null) {
      return FuzzyQuery.defaultPrefixLength;
    } else {
      return fuzzyConfig.getPrefixLength();
    }
  }
  
  /**
   * Gets the default slop for phrases.
   */
  @Override
  public int getPhraseSlop() {
    Integer phraseSlop = getQueryConfigHandler().get(ConfigurationKeys.PHRASE_SLOP);
    
    if (phraseSlop == null) {
      return 0;
      
    } else {
      return phraseSlop;
    }
  }
  
  /**
   * Set the minimum similarity for fuzzy queries. Default is defined on
   * {@link FuzzyQuery#defaultMinSimilarity}.
   */
  @Override
  public void setFuzzyMinSim(float fuzzyMinSim) {
    QueryConfigHandler config = getQueryConfigHandler();
    FuzzyConfig fuzzyConfig = config.get(ConfigurationKeys.FUZZY_CONFIG);
    
    if (fuzzyConfig == null) {
      fuzzyConfig = new FuzzyConfig();
      config.set(ConfigurationKeys.FUZZY_CONFIG, fuzzyConfig);
    }

    fuzzyConfig.setMinSimilarity(fuzzyMinSim);
  }
  
  /**
   * Sets the boost used for each field.
   * 
   * @param boosts a collection that maps a field to its boost 
   */
  public void setFieldsBoost(Map<String, Float> boosts) {
    getQueryConfigHandler().set(ConfigurationKeys.FIELD_BOOST_MAP, boosts);
  }
  
  /**
   * Returns the field to boost map used to set boost for each field.
   * 
   * @return the field to boost map 
   */
  public Map<String, Float> getFieldsBoost() {
    return getQueryConfigHandler().get(ConfigurationKeys.FIELD_BOOST_MAP);
  }

  /**
   * Sets the default {@link Resolution} used for certain field when
   * no {@link Resolution} is defined for this field.
   * 
   * @param dateResolution the default {@link Resolution}
   */
  @Override
  public void setDateResolution(DateTools.Resolution dateResolution) {
    getQueryConfigHandler().set(ConfigurationKeys.DATE_RESOLUTION, dateResolution);
  }
  
  /**
   * Returns the default {@link Resolution} used for certain field when
   * no {@link Resolution} is defined for this field.
   * 
   * @return the default {@link Resolution}
   */
  public DateTools.Resolution getDateResolution() {
    return getQueryConfigHandler().get(ConfigurationKeys.DATE_RESOLUTION);
  }

  /**
   * Sets the {@link Resolution} used for each field
   * 
   * @param dateRes a collection that maps a field to its {@link Resolution}
   * 
   * @deprecated this method was renamed to {@link #setDateResolutionMap(Map)} 
   */
  @Deprecated
  public void setDateResolution(Map<CharSequence, DateTools.Resolution> dateRes) {
    setDateResolutionMap(dateRes);
  }
  
  /**
   * Returns the field to {@link Resolution} map used to normalize each date field.
   * 
   * @return the field to {@link Resolution} map
   */
  public Map<CharSequence, DateTools.Resolution> getDateResolutionMap() {
    return getQueryConfigHandler().get(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP);
  }
  
  /**
   * Sets the {@link Resolution} used for each field
   * 
   * @param dateRes a collection that maps a field to its {@link Resolution}
   */
  public void setDateResolutionMap(Map<CharSequence, DateTools.Resolution> dateRes) {
    getQueryConfigHandler().set(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP, dateRes);
  }
  
}
