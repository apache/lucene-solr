package org.apache.lucene.queryParser.standard;

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

import java.text.Collator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.core.QueryNodeException;
import org.apache.lucene.queryParser.core.config.FieldConfig;
import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.core.nodes.QueryNode;
import org.apache.lucene.queryParser.core.parser.SyntaxParser;
import org.apache.lucene.queryParser.core.processors.QueryNodeProcessor;
import org.apache.lucene.queryParser.standard.builders.StandardQueryBuilder;
import org.apache.lucene.queryParser.standard.builders.StandardQueryTreeBuilder;
import org.apache.lucene.queryParser.standard.config.AllowLeadingWildcardAttribute;
import org.apache.lucene.queryParser.standard.config.AnalyzerAttribute;
import org.apache.lucene.queryParser.standard.config.DateResolutionAttribute;
import org.apache.lucene.queryParser.standard.config.DefaultOperatorAttribute;
import org.apache.lucene.queryParser.standard.config.DefaultPhraseSlopAttribute;
import org.apache.lucene.queryParser.standard.config.LocaleAttribute;
import org.apache.lucene.queryParser.standard.config.LowercaseExpandedTermsAttribute;
import org.apache.lucene.queryParser.standard.config.MultiTermRewriteMethodAttribute;
import org.apache.lucene.queryParser.standard.config.PositionIncrementsAttribute;
import org.apache.lucene.queryParser.standard.config.RangeCollatorAttribute;
import org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryParser.standard.parser.StandardSyntaxParser;
import org.apache.lucene.queryParser.standard.processors.StandardQueryNodeProcessorPipeline;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;

/**
 * This class performs the query parsing using the new query parser
 * implementation, but keeps the old {@link QueryParser} API. <br/>
 * <br/>
 * This class should be used when the new query parser features are and the old
 * {@link QueryParser} API are needed at the same time. <br/>
 * 
 * @deprecated this class will be removed soon, it's a temporary class to be
 *             used along the transition from the old query parser to the new
 *             one
 */
@Deprecated
public class QueryParserWrapper {

  /**
   * The default operator for parsing queries. Use
   * {@link QueryParserWrapper#setDefaultOperator} to change it.
   */
  static public enum Operator { OR, AND }

  // the nested class:
  /** Alternative form of QueryParser.Operator.AND */
  public static final Operator AND_OPERATOR = Operator.AND;

  /** Alternative form of QueryParser.Operator.OR */
  public static final Operator OR_OPERATOR = Operator.OR;

  /**
   * Returns a String where those characters that QueryParser expects to be
   * escaped are escaped by a preceding <code>\</code>.
   */
  public static String escape(String s) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      // These characters are part of the query syntax and must be escaped
      if (c == '\\' || c == '+' || c == '-' || c == '!' || c == '(' || c == ')'
          || c == ':' || c == '^' || c == '[' || c == ']' || c == '\"'
          || c == '{' || c == '}' || c == '~' || c == '*' || c == '?'
          || c == '|' || c == '&') {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }

  private SyntaxParser syntaxParser = new StandardSyntaxParser();

  private StandardQueryConfigHandler config;

  private StandardQueryParser qpHelper;

  private QueryNodeProcessor processorPipeline;

  private StandardQueryBuilder builder = new StandardQueryTreeBuilder();

  private String defaultField;

  public QueryParserWrapper(String defaultField, Analyzer analyzer) {
    this.defaultField = defaultField;

    this.qpHelper = new StandardQueryParser();

    this.config = (StandardQueryConfigHandler) qpHelper.getQueryConfigHandler();

    this.qpHelper.setAnalyzer(analyzer);

    this.processorPipeline = new StandardQueryNodeProcessorPipeline(this.config);

  }

  StandardQueryParser getQueryParserHelper() {
    return qpHelper;
  }

  public String getField() {
    return this.defaultField;
  }

  public Analyzer getAnalyzer() {

    if (this.config != null
        && this.config.hasAttribute(AnalyzerAttribute.class)) {

      return this.config.getAttribute(AnalyzerAttribute.class).getAnalyzer();

    }

    return null;

  }

  /**
   * Sets the {@link StandardQueryBuilder} used to generate a {@link Query}
   * object from the parsed and processed query node tree.
   * 
   * @param builder the builder
   */
  public void setQueryBuilder(StandardQueryBuilder builder) {
    this.builder = builder;
  }

  /**
   * Sets the {@link QueryNodeProcessor} used to process the query node tree
   * generated by the
   * {@link org.apache.lucene.queryParser.standard.parser.StandardSyntaxParser}.
   * 
   * @param processor the processor
   */
  public void setQueryProcessor(QueryNodeProcessor processor) {
    this.processorPipeline = processor;
    this.processorPipeline.setQueryConfigHandler(this.config);

  }

  /**
   * Sets the {@link QueryConfigHandler} used by the {@link QueryNodeProcessor}
   * set to this object.
   * 
   * @param queryConfig the query config handler
   */
  public void setQueryConfig(StandardQueryConfigHandler queryConfig) {
    this.config = queryConfig;

    if (this.processorPipeline != null) {
      this.processorPipeline.setQueryConfigHandler(this.config);
    }

  }

  /**
   * Returns the query config handler used by this query parser
   * 
   * @return the query config handler
   */
  public QueryConfigHandler getQueryConfigHandler() {
    return this.config;
  }

  /**
   * Returns {@link QueryNodeProcessor} used to process the query node tree
   * generated by the
   * {@link org.apache.lucene.queryParser.standard.parser.StandardSyntaxParser}.
   * 
   * @return the query processor
   */
  public QueryNodeProcessor getQueryProcessor() {
    return this.processorPipeline;
  }

  public ParseException generateParseException() {
    return null;
  }

  public boolean getAllowLeadingWildcard() {

    if (this.config != null
        && this.config.hasAttribute(AllowLeadingWildcardAttribute.class)) {

      return this.config.getAttribute(AllowLeadingWildcardAttribute.class)
          .isAllowLeadingWildcard();

    }

    return false;

  }

  public MultiTermQuery.RewriteMethod getMultiTermRewriteMethod() {

    if (this.config != null
        && this.config.hasAttribute(MultiTermRewriteMethodAttribute.class)) {

      return this.config.getAttribute(MultiTermRewriteMethodAttribute.class)
          .getMultiTermRewriteMethod();

    }

    return MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;

  }

  public Resolution getDateResolution(String fieldName) {

    if (this.config != null) {
      FieldConfig fieldConfig = this.config.getFieldConfig(fieldName);

      if (fieldConfig != null) {

        if (this.config.hasAttribute(DateResolutionAttribute.class)) {

          return this.config.getAttribute(DateResolutionAttribute.class)
              .getDateResolution();

        }

      }

    }

    return null;

  }

  public boolean getEnablePositionIncrements() {

    if (this.config != null
        && this.config.hasAttribute(PositionIncrementsAttribute.class)) {

      return this.config.getAttribute(PositionIncrementsAttribute.class)
          .isPositionIncrementsEnabled();

    }

    return false;

  }

  public float getFuzzyMinSim() {
    return FuzzyQuery.defaultMinSimilarity;
  }

  public int getFuzzyPrefixLength() {
    return FuzzyQuery.defaultPrefixLength;
  }

  public Locale getLocale() {

    if (this.config != null && this.config.hasAttribute(LocaleAttribute.class)) {
      return this.config.getAttribute(LocaleAttribute.class).getLocale();
    }

    return Locale.getDefault();

  }

  public boolean getLowercaseExpandedTerms() {

    if (this.config != null
        && this.config.hasAttribute(LowercaseExpandedTermsAttribute.class)) {

      return this.config.getAttribute(LowercaseExpandedTermsAttribute.class)
          .isLowercaseExpandedTerms();

    }

    return true;

  }

  public int getPhraseSlop() {

    if (this.config != null
        && this.config.hasAttribute(AllowLeadingWildcardAttribute.class)) {

      return this.config.getAttribute(DefaultPhraseSlopAttribute.class)
          .getDefaultPhraseSlop();

    }

    return 0;

  }

  public Collator getRangeCollator() {

    if (this.config != null
        && this.config.hasAttribute(RangeCollatorAttribute.class)) {

      return this.config.getAttribute(RangeCollatorAttribute.class)
          .getRangeCollator();

    }

    return null;

  }

  public boolean getUseOldRangeQuery() {
    if (getMultiTermRewriteMethod() == MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE) {
      return true;
    } else {
      return false;
    }
  }

  public Query parse(String query) throws ParseException {

    try {
      QueryNode queryTree = this.syntaxParser.parse(query, getField());
      queryTree = this.processorPipeline.process(queryTree);
      return this.builder.build(queryTree);

    } catch (QueryNodeException e) {
      throw new ParseException("parse exception");
    }

  }

  public void setAllowLeadingWildcard(boolean allowLeadingWildcard) {
    this.qpHelper.setAllowLeadingWildcard(allowLeadingWildcard);
  }

  public void setMultiTermRewriteMethod(MultiTermQuery.RewriteMethod method) {
    this.qpHelper.setMultiTermRewriteMethod(method);
  }

  public void setDateResolution(Resolution dateResolution) {
    this.qpHelper.setDateResolution(dateResolution);
  }

  private Map<CharSequence, DateTools.Resolution> dateRes = new HashMap<CharSequence, DateTools.Resolution>();

  public void setDateResolution(String fieldName, Resolution dateResolution) {
    dateRes.put(fieldName, dateResolution);
    this.qpHelper.setDateResolution(dateRes);
  }

  public void setDefaultOperator(Operator op) {

    this.qpHelper
        .setDefaultOperator(OR_OPERATOR.equals(op) ? org.apache.lucene.queryParser.standard.config.DefaultOperatorAttribute.Operator.OR
            : org.apache.lucene.queryParser.standard.config.DefaultOperatorAttribute.Operator.AND);

  }

  public Operator getDefaultOperator() {

    if (this.config != null
        && this.config.hasAttribute(DefaultOperatorAttribute.class)) {

      return (this.config.getAttribute(DefaultOperatorAttribute.class)
          .getOperator() == org.apache.lucene.queryParser.standard.config.DefaultOperatorAttribute.Operator.AND) ? AND_OPERATOR
          : OR_OPERATOR;

    }

    return OR_OPERATOR;

  }

  public void setEnablePositionIncrements(boolean enable) {
    this.qpHelper.setEnablePositionIncrements(enable);
  }

  public void setFuzzyMinSim(float fuzzyMinSim) {
    // TODO Auto-generated method stub

  }

  public void setFuzzyPrefixLength(int fuzzyPrefixLength) {
    // TODO Auto-generated method stub

  }

  public void setLocale(Locale locale) {
    this.qpHelper.setLocale(locale);
  }

  public void setLowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
    this.qpHelper.setLowercaseExpandedTerms(lowercaseExpandedTerms);
  }

  public void setPhraseSlop(int phraseSlop) {
    this.qpHelper.setDefaultPhraseSlop(phraseSlop);
  }

  public void setRangeCollator(Collator rc) {
    this.qpHelper.setRangeCollator(rc);
  }

  public void setUseOldRangeQuery(boolean useOldRangeQuery) {
    if (useOldRangeQuery) {
      setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    } else {
      setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT);
    }
  }

  protected Query getPrefixQuery(String field, String termStr)
      throws ParseException {
    throw new UnsupportedOperationException();
  }

  protected Query getWildcardQuery(String field, String termStr)
      throws ParseException {
    throw new UnsupportedOperationException();
  }

  protected Query getFuzzyQuery(String field, String termStr,
      float minSimilarity) throws ParseException {
    throw new UnsupportedOperationException();
  }

  /** @deprecated Use {@link #getFieldQuery(String, String, boolean)} instead */
  @Deprecated
  protected Query getFieldQuery(String field, String queryText) throws ParseException {
    return getFieldQuery(field, queryText, true);
  }

  /**
   * @exception ParseException throw in overridden method to disallow
   */
  protected Query getFieldQuery(String field, String queryText, boolean quoted)
      throws ParseException {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  protected Query getBooleanQuery(List clauses, boolean disableCoord)
      throws ParseException {
    throw new UnsupportedOperationException();
  }

  /**
   * Base implementation delegates to {@link #getFieldQuery(String,String)}.
   * This method may be overridden, for example, to return a SpanNearQuery
   * instead of a PhraseQuery.
   * 
   * @exception ParseException throw in overridden method to disallow
   */
  protected Query getFieldQuery(String field, String queryText, int slop)
      throws ParseException {
    throw new UnsupportedOperationException();
  }

  /**
   * @exception ParseException throw in overridden method to disallow
   */
  protected Query getRangeQuery(String field, String part1, String part2,
      boolean inclusive) throws ParseException {
    throw new UnsupportedOperationException();
  }

}
