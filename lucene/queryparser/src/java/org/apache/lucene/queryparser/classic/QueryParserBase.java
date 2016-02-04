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
package org.apache.lucene.queryparser.classic;

import java.io.IOException;
import java.io.StringReader;
import java.text.DateFormat;
import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.queryparser.flexible.standard.CommonQueryParserConfiguration;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.TooManyClauses;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.util.automaton.RegExp;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_MAX_DETERMINIZED_STATES;

/** This class is overridden by QueryParser in QueryParser.jj
 * and acts to separate the majority of the Java code from the .jj grammar file. 
 */
public abstract class QueryParserBase extends QueryBuilder implements CommonQueryParserConfiguration {
  
  /** Do not catch this exception in your code, it means you are using methods that you should no longer use. */
  public static class MethodRemovedUseAnother extends Throwable {}

  static final int CONJ_NONE   = 0;
  static final int CONJ_AND    = 1;
  static final int CONJ_OR     = 2;

  static final int MOD_NONE    = 0;
  static final int MOD_NOT     = 10;
  static final int MOD_REQ     = 11;

  // make it possible to call setDefaultOperator() without accessing
  // the nested class:
  /** Alternative form of QueryParser.Operator.AND */
  public static final Operator AND_OPERATOR = Operator.AND;
  /** Alternative form of QueryParser.Operator.OR */
  public static final Operator OR_OPERATOR = Operator.OR;

  /** The actual operator that parser uses to combine query terms */
  Operator operator = OR_OPERATOR;

  boolean lowercaseExpandedTerms = true;
  MultiTermQuery.RewriteMethod multiTermRewriteMethod = MultiTermQuery.CONSTANT_SCORE_REWRITE;
  boolean allowLeadingWildcard = false;

  protected String field;
  int phraseSlop = 0;
  float fuzzyMinSim = FuzzyQuery.defaultMinSimilarity;
  int fuzzyPrefixLength = FuzzyQuery.defaultPrefixLength;
  Locale locale = Locale.getDefault();
  TimeZone timeZone = TimeZone.getDefault();

  // the default date resolution
  DateTools.Resolution dateResolution = null;
  // maps field names to date resolutions
  Map<String,DateTools.Resolution> fieldToDateResolution = null;

  //Whether or not to analyze range terms when constructing RangeQuerys
  // (For example, analyzing terms into collation keys for locale-sensitive RangeQuery)
  boolean analyzeRangeTerms = false;

  boolean autoGeneratePhraseQueries;
  int maxDeterminizedStates = DEFAULT_MAX_DETERMINIZED_STATES;

  // So the generated QueryParser(CharStream) won't error out
  protected QueryParserBase() {
    super(null);
  }

  /** Initializes a query parser.  Called by the QueryParser constructor
   *  @param f  the default field for query terms.
   *  @param a   used to find terms in the query text.
   */
  public void init(String f, Analyzer a) {
    setAnalyzer(a);
    field = f;
    setAutoGeneratePhraseQueries(false);
  }

  // the generated parser will create these in QueryParser
  public abstract void ReInit(CharStream stream);
  public abstract Query TopLevelQuery(String field) throws ParseException;


  /** Parses a query string, returning a {@link org.apache.lucene.search.Query}.
   *  @param query  the query string to be parsed.
   *  @throws ParseException if the parsing fails
   */
  public Query parse(String query) throws ParseException {
    ReInit(new FastCharStream(new StringReader(query)));
    try {
      // TopLevelQuery is a Query followed by the end-of-input (EOF)
      Query res = TopLevelQuery(field);
      return res!=null ? res : newBooleanQuery(false).build();
    }
    catch (ParseException | TokenMgrError tme) {
      // rethrow to include the original query:
      ParseException e = new ParseException("Cannot parse '" +query+ "': " + tme.getMessage());
      e.initCause(tme);
      throw e;
    } catch (BooleanQuery.TooManyClauses tmc) {
      ParseException e = new ParseException("Cannot parse '" +query+ "': too many boolean clauses");
      e.initCause(tmc);
      throw e;
    }
  }

  /**
   * @return Returns the default field.
   */
  public String getField() {
    return field;
  }

  /**
   * @see #setAutoGeneratePhraseQueries(boolean)
   */
  public final boolean getAutoGeneratePhraseQueries() {
    return autoGeneratePhraseQueries;
  }

  /**
   * Set to true if phrase queries will be automatically generated
   * when the analyzer returns more than one term from whitespace
   * delimited text.
   * NOTE: this behavior may not be suitable for all languages.
   * <p>
   * Set to false if phrase queries should only be generated when
   * surrounded by double quotes.
   */
  public final void setAutoGeneratePhraseQueries(boolean value) {
    this.autoGeneratePhraseQueries = value;
  }

   /**
   * Get the minimal similarity for fuzzy queries.
   */
  @Override
  public float getFuzzyMinSim() {
      return fuzzyMinSim;
  }

  /**
   * Set the minimum similarity for fuzzy queries.
   * Default is 2f.
   */
  @Override
  public void setFuzzyMinSim(float fuzzyMinSim) {
      this.fuzzyMinSim = fuzzyMinSim;
  }

   /**
   * Get the prefix length for fuzzy queries.
   * @return Returns the fuzzyPrefixLength.
   */
  @Override
  public int getFuzzyPrefixLength() {
    return fuzzyPrefixLength;
  }

  /**
   * Set the prefix length for fuzzy queries. Default is 0.
   * @param fuzzyPrefixLength The fuzzyPrefixLength to set.
   */
  @Override
  public void setFuzzyPrefixLength(int fuzzyPrefixLength) {
    this.fuzzyPrefixLength = fuzzyPrefixLength;
  }

  /**
   * Sets the default slop for phrases.  If zero, then exact phrase matches
   * are required.  Default value is zero.
   */
  @Override
  public void setPhraseSlop(int phraseSlop) {
    this.phraseSlop = phraseSlop;
  }

  /**
   * Gets the default slop for phrases.
   */
  @Override
  public int getPhraseSlop() {
    return phraseSlop;
  }


  /**
   * Set to <code>true</code> to allow leading wildcard characters.
   * <p>
   * When set, <code>*</code> or <code>?</code> are allowed as
   * the first character of a PrefixQuery and WildcardQuery.
   * Note that this can produce very slow
   * queries on big indexes.
   * <p>
   * Default: false.
   */
  @Override
  public void setAllowLeadingWildcard(boolean allowLeadingWildcard) {
    this.allowLeadingWildcard = allowLeadingWildcard;
  }

  /**
   * @see #setAllowLeadingWildcard(boolean)
   */
  @Override
  public boolean getAllowLeadingWildcard() {
    return allowLeadingWildcard;
  }

  /**
   * Sets the boolean operator of the QueryParser.
   * In default mode (<code>OR_OPERATOR</code>) terms without any modifiers
   * are considered optional: for example <code>capital of Hungary</code> is equal to
   * <code>capital OR of OR Hungary</code>.<br>
   * In <code>AND_OPERATOR</code> mode terms are considered to be in conjunction: the
   * above mentioned query is parsed as <code>capital AND of AND Hungary</code>
   */
  public void setDefaultOperator(Operator op) {
    this.operator = op;
  }


  /**
   * Gets implicit operator setting, which will be either AND_OPERATOR
   * or OR_OPERATOR.
   */
  public Operator getDefaultOperator() {
    return operator;
  }


  /**
   * Whether terms of wildcard, prefix, fuzzy and range queries are to be automatically
   * lower-cased or not.  Default is <code>true</code>.
   */
  @Override
  public void setLowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
    this.lowercaseExpandedTerms = lowercaseExpandedTerms;
  }

  /**
   * @see #setLowercaseExpandedTerms(boolean)
   */
  @Override
  public boolean getLowercaseExpandedTerms() {
    return lowercaseExpandedTerms;
  }

  /**
   * By default QueryParser uses {@link org.apache.lucene.search.MultiTermQuery#CONSTANT_SCORE_REWRITE}
   * when creating a {@link PrefixQuery}, {@link WildcardQuery} or {@link TermRangeQuery}. This implementation is generally preferable because it
   * a) Runs faster b) Does not have the scarcity of terms unduly influence score
   * c) avoids any {@link TooManyClauses} exception.
   * However, if your application really needs to use the
   * old-fashioned {@link BooleanQuery} expansion rewriting and the above
   * points are not relevant then use this to change
   * the rewrite method.
   */
  @Override
  public void setMultiTermRewriteMethod(MultiTermQuery.RewriteMethod method) {
    multiTermRewriteMethod = method;
  }


  /**
   * @see #setMultiTermRewriteMethod
   */
  @Override
  public MultiTermQuery.RewriteMethod getMultiTermRewriteMethod() {
    return multiTermRewriteMethod;
  }

  /**
   * Set locale used by date range parsing, lowercasing, and other
   * locale-sensitive operations.
   */
  @Override
  public void setLocale(Locale locale) {
    this.locale = locale;
  }

  /**
   * Returns current locale, allowing access by subclasses.
   */
  @Override
  public Locale getLocale() {
    return locale;
  }
  
  @Override
  public void setTimeZone(TimeZone timeZone) {
    this.timeZone = timeZone;
  }
  
  @Override
  public TimeZone getTimeZone() {
    return timeZone;
  }

  /**
   * Sets the default date resolution used by RangeQueries for fields for which no
   * specific date resolutions has been set. Field specific resolutions can be set
   * with {@link #setDateResolution(String, org.apache.lucene.document.DateTools.Resolution)}.
   *
   * @param dateResolution the default date resolution to set
   */
  @Override
  public void setDateResolution(DateTools.Resolution dateResolution) {
    this.dateResolution = dateResolution;
  }

  /**
   * Sets the date resolution used by RangeQueries for a specific field.
   *
   * @param fieldName field for which the date resolution is to be set
   * @param dateResolution date resolution to set
   */
  public void setDateResolution(String fieldName, DateTools.Resolution dateResolution) {
    if (fieldName == null) {
      throw new IllegalArgumentException("Field cannot be null.");
    }

    if (fieldToDateResolution == null) {
      // lazily initialize HashMap
      fieldToDateResolution = new HashMap<>();
    }

    fieldToDateResolution.put(fieldName, dateResolution);
  }

  /**
   * Returns the date resolution that is used by RangeQueries for the given field.
   * Returns null, if no default or field specific date resolution has been set
   * for the given field.
   *
   */
  public DateTools.Resolution getDateResolution(String fieldName) {
    if (fieldName == null) {
      throw new IllegalArgumentException("Field cannot be null.");
    }

    if (fieldToDateResolution == null) {
      // no field specific date resolutions set; return default date resolution instead
      return this.dateResolution;
    }

    DateTools.Resolution resolution = fieldToDateResolution.get(fieldName);
    if (resolution == null) {
      // no date resolutions set for the given field; return default date resolution instead
      resolution = this.dateResolution;
    }

    return resolution;
  }

  /**
   * Set whether or not to analyze range terms when constructing {@link TermRangeQuery}s.
   * For example, setting this to true can enable analyzing terms into 
   * collation keys for locale-sensitive {@link TermRangeQuery}.
   * 
   * @param analyzeRangeTerms whether or not terms should be analyzed for RangeQuerys
   */
  public void setAnalyzeRangeTerms(boolean analyzeRangeTerms) {
    this.analyzeRangeTerms = analyzeRangeTerms;
  }

  /**
   * @return whether or not to analyze range terms when constructing {@link TermRangeQuery}s.
   */
  public boolean getAnalyzeRangeTerms() {
    return analyzeRangeTerms;
  }

  /**
   * @param maxDeterminizedStates the maximum number of states that
   *   determinizing a regexp query can result in.  If the query results in any
   *   more states a TooComplexToDeterminizeException is thrown.
   */
  public void setMaxDeterminizedStates(int maxDeterminizedStates) {
    this.maxDeterminizedStates = maxDeterminizedStates;
  }

  /**
   * @return the maximum number of states that determinizing a regexp query
   *   can result in.  If the query results in any more states a
   *   TooComplexToDeterminizeException is thrown.
   */
  public int getMaxDeterminizedStates() {
    return maxDeterminizedStates;
  }

  protected void addClause(List<BooleanClause> clauses, int conj, int mods, Query q) {
    boolean required, prohibited;

    // If this term is introduced by AND, make the preceding term required,
    // unless it's already prohibited
    if (clauses.size() > 0 && conj == CONJ_AND) {
      BooleanClause c = clauses.get(clauses.size()-1);
      if (!c.isProhibited())
        clauses.set(clauses.size() - 1, new BooleanClause(c.getQuery(), Occur.MUST));
    }

    if (clauses.size() > 0 && operator == AND_OPERATOR && conj == CONJ_OR) {
      // If this term is introduced by OR, make the preceding term optional,
      // unless it's prohibited (that means we leave -a OR b but +a OR b-->a OR b)
      // notice if the input is a OR b, first term is parsed as required; without
      // this modification a OR b would parsed as +a OR b
      BooleanClause c = clauses.get(clauses.size()-1);
      if (!c.isProhibited())
        clauses.set(clauses.size() - 1, new BooleanClause(c.getQuery(), Occur.SHOULD));
    }

    // We might have been passed a null query; the term might have been
    // filtered away by the analyzer.
    if (q == null)
      return;

    if (operator == OR_OPERATOR) {
      // We set REQUIRED if we're introduced by AND or +; PROHIBITED if
      // introduced by NOT or -; make sure not to set both.
      prohibited = (mods == MOD_NOT);
      required = (mods == MOD_REQ);
      if (conj == CONJ_AND && !prohibited) {
        required = true;
      }
    } else {
      // We set PROHIBITED if we're introduced by NOT or -; We set REQUIRED
      // if not PROHIBITED and not introduced by OR
      prohibited = (mods == MOD_NOT);
      required   = (!prohibited && conj != CONJ_OR);
    }
    if (required && !prohibited)
      clauses.add(newBooleanClause(q, BooleanClause.Occur.MUST));
    else if (!required && !prohibited)
      clauses.add(newBooleanClause(q, BooleanClause.Occur.SHOULD));
    else if (!required && prohibited)
      clauses.add(newBooleanClause(q, BooleanClause.Occur.MUST_NOT));
    else
      throw new RuntimeException("Clause cannot be both required and prohibited");
  }

  /**
   * @exception org.apache.lucene.queryparser.classic.ParseException throw in overridden method to disallow
   */
  protected Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {
    return newFieldQuery(getAnalyzer(), field, queryText, quoted);
  }
  
  /**
   * @exception org.apache.lucene.queryparser.classic.ParseException throw in overridden method to disallow
   */
  protected Query newFieldQuery(Analyzer analyzer, String field, String queryText, boolean quoted)  throws ParseException {
    BooleanClause.Occur occur = operator == Operator.AND ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD;
    return createFieldQuery(analyzer, occur, field, queryText, quoted || autoGeneratePhraseQueries, phraseSlop);
  }



  /**
   * Base implementation delegates to {@link #getFieldQuery(String,String,boolean)}.
   * This method may be overridden, for example, to return
   * a SpanNearQuery instead of a PhraseQuery.
   *
   * @exception org.apache.lucene.queryparser.classic.ParseException throw in overridden method to disallow
   */
  protected Query getFieldQuery(String field, String queryText, int slop)
        throws ParseException {
    Query query = getFieldQuery(field, queryText, true);

    if (query instanceof PhraseQuery) {
      PhraseQuery.Builder builder = new PhraseQuery.Builder();
      builder.setSlop(slop);
      PhraseQuery pq = (PhraseQuery) query;
      org.apache.lucene.index.Term[] terms = pq.getTerms();
      int[] positions = pq.getPositions();
      for (int i = 0; i < terms.length; ++i) {
        builder.add(terms[i], positions[i]);
      }
      query = builder.build();
    }
    if (query instanceof MultiPhraseQuery) {
      ((MultiPhraseQuery) query).setSlop(slop);
    }

    return query;
  }

  protected Query getRangeQuery(String field,
                                String part1,
                                String part2,
                                boolean startInclusive,
                                boolean endInclusive) throws ParseException
  {
    if (lowercaseExpandedTerms) {
      part1 = part1==null ? null : part1.toLowerCase(locale);
      part2 = part2==null ? null : part2.toLowerCase(locale);
    }


    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT, locale);
    df.setLenient(true);
    DateTools.Resolution resolution = getDateResolution(field);
    
    try {
      part1 = DateTools.dateToString(df.parse(part1), resolution);
    } catch (Exception e) { }

    try {
      Date d2 = df.parse(part2);
      if (endInclusive) {
        // The user can only specify the date, not the time, so make sure
        // the time is set to the latest possible time of that date to really
        // include all documents:
        Calendar cal = Calendar.getInstance(timeZone, locale);
        cal.setTime(d2);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);
        d2 = cal.getTime();
      }
      part2 = DateTools.dateToString(d2, resolution);
    } catch (Exception e) { }

    return newRangeQuery(field, part1, part2, startInclusive, endInclusive);
  }

 /**
  * Builds a new BooleanClause instance
  * @param q sub query
  * @param occur how this clause should occur when matching documents
  * @return new BooleanClause instance
  */
  protected BooleanClause newBooleanClause(Query q, BooleanClause.Occur occur) {
    return new BooleanClause(q, occur);
  }

  /**
   * Builds a new PrefixQuery instance
   * @param prefix Prefix term
   * @return new PrefixQuery instance
   */
  protected Query newPrefixQuery(Term prefix){
    PrefixQuery query = new PrefixQuery(prefix);
    query.setRewriteMethod(multiTermRewriteMethod);
    return query;
  }

  /**
   * Builds a new RegexpQuery instance
   * @param regexp Regexp term
   * @return new RegexpQuery instance
   */
  protected Query newRegexpQuery(Term regexp) {
    RegexpQuery query = new RegexpQuery(regexp, RegExp.ALL,
      maxDeterminizedStates);
    query.setRewriteMethod(multiTermRewriteMethod);
    return query;
  }

  /**
   * Builds a new FuzzyQuery instance
   * @param term Term
   * @param minimumSimilarity minimum similarity
   * @param prefixLength prefix length
   * @return new FuzzyQuery Instance
   */
  protected Query newFuzzyQuery(Term term, float minimumSimilarity, int prefixLength) {
    // FuzzyQuery doesn't yet allow constant score rewrite
    String text = term.text();
    int numEdits = FuzzyQuery.floatToEdits(minimumSimilarity, 
        text.codePointCount(0, text.length()));
    return new FuzzyQuery(term,numEdits,prefixLength);
  }

  // TODO: Should this be protected instead?
  private BytesRef analyzeMultitermTerm(String field, String part) {
    return analyzeMultitermTerm(field, part, getAnalyzer());
  }

  protected BytesRef analyzeMultitermTerm(String field, String part, Analyzer analyzerIn) {
    if (analyzerIn == null) analyzerIn = getAnalyzer();

    try (TokenStream source = analyzerIn.tokenStream(field, part)) {
      source.reset();
      
      TermToBytesRefAttribute termAtt = source.getAttribute(TermToBytesRefAttribute.class);

      if (!source.incrementToken())
        throw new IllegalArgumentException("analyzer returned no terms for multiTerm term: " + part);
      BytesRef bytes = BytesRef.deepCopyOf(termAtt.getBytesRef());
      if (source.incrementToken())
        throw new IllegalArgumentException("analyzer returned too many terms for multiTerm term: " + part);
      source.end();
      return bytes;
    } catch (IOException e) {
      throw new RuntimeException("Error analyzing multiTerm term: " + part, e);
    }
  }

  /**
   * Builds a new {@link TermRangeQuery} instance
   * @param field Field
   * @param part1 min
   * @param part2 max
   * @param startInclusive true if the start of the range is inclusive
   * @param endInclusive true if the end of the range is inclusive
   * @return new {@link TermRangeQuery} instance
   */
  protected Query newRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) {
    final BytesRef start;
    final BytesRef end;
     
    if (part1 == null) {
      start = null;
    } else {
      start = analyzeRangeTerms ? analyzeMultitermTerm(field, part1) : new BytesRef(part1);
    }
     
    if (part2 == null) {
      end = null;
    } else {
      end = analyzeRangeTerms ? analyzeMultitermTerm(field, part2) : new BytesRef(part2);
    }
      
    final TermRangeQuery query = new TermRangeQuery(field, start, end, startInclusive, endInclusive);

    query.setRewriteMethod(multiTermRewriteMethod);
    return query;
  }

  /**
   * Builds a new MatchAllDocsQuery instance
   * @return new MatchAllDocsQuery instance
   */
  protected Query newMatchAllDocsQuery() {
    return new MatchAllDocsQuery();
  }

  /**
   * Builds a new WildcardQuery instance
   * @param t wildcard term
   * @return new WildcardQuery instance
   */
  protected Query newWildcardQuery(Term t) {
    WildcardQuery query = new WildcardQuery(t, maxDeterminizedStates);
    query.setRewriteMethod(multiTermRewriteMethod);
    return query;
  }

  /**
   * Factory method for generating query, given a set of clauses.
   * By default creates a boolean query composed of clauses passed in.
   *
   * Can be overridden by extending classes, to modify query being
   * returned.
   *
   * @param clauses List that contains {@link org.apache.lucene.search.BooleanClause} instances
   *    to join.
   *
   * @return Resulting {@link org.apache.lucene.search.Query} object.
   * @exception org.apache.lucene.queryparser.classic.ParseException throw in overridden method to disallow
   */
  protected Query getBooleanQuery(List<BooleanClause> clauses) throws ParseException {
    return getBooleanQuery(clauses, false);
  }

  /**
   * Factory method for generating query, given a set of clauses.
   * By default creates a boolean query composed of clauses passed in.
   *
   * Can be overridden by extending classes, to modify query being
   * returned.
   *
   * @param clauses List that contains {@link org.apache.lucene.search.BooleanClause} instances
   *    to join.
   * @param disableCoord true if coord scoring should be disabled.
   *
   * @return Resulting {@link org.apache.lucene.search.Query} object.
   * @exception org.apache.lucene.queryparser.classic.ParseException throw in overridden method to disallow
   */
  protected Query getBooleanQuery(List<BooleanClause> clauses, boolean disableCoord)
    throws ParseException
  {
    if (clauses.size()==0) {
      return null; // all clause words were filtered away by the analyzer.
    }
    BooleanQuery.Builder query = newBooleanQuery(disableCoord);
    for(final BooleanClause clause: clauses) {
      query.add(clause);
    }
    return query.build();
  }

  /**
   * Factory method for generating a query. Called when parser
   * parses an input term token that contains one or more wildcard
   * characters (? and *), but is not a prefix term token (one
   * that has just a single * character at the end)
   *<p>
   * Depending on settings, prefix term may be lower-cased
   * automatically. It will not go through the default Analyzer,
   * however, since normal Analyzers are unlikely to work properly
   * with wildcard templates.
   *<p>
   * Can be overridden by extending classes, to provide custom handling for
   * wildcard queries, which may be necessary due to missing analyzer calls.
   *
   * @param field Name of the field query will use.
   * @param termStr Term token that contains one or more wild card
   *   characters (? or *), but is not simple prefix term
   *
   * @return Resulting {@link org.apache.lucene.search.Query} built for the term
   * @exception org.apache.lucene.queryparser.classic.ParseException throw in overridden method to disallow
   */
  protected Query getWildcardQuery(String field, String termStr) throws ParseException
  {
    if ("*".equals(field)) {
      if ("*".equals(termStr)) return newMatchAllDocsQuery();
    }
    if (!allowLeadingWildcard && (termStr.startsWith("*") || termStr.startsWith("?")))
      throw new ParseException("'*' or '?' not allowed as first character in WildcardQuery");
    if (lowercaseExpandedTerms) {
      termStr = termStr.toLowerCase(locale);
    }
    Term t = new Term(field, termStr);
    return newWildcardQuery(t);
  }

  /**
   * Factory method for generating a query. Called when parser
   * parses an input term token that contains a regular expression
   * query.
   *<p>
   * Depending on settings, pattern term may be lower-cased
   * automatically. It will not go through the default Analyzer,
   * however, since normal Analyzers are unlikely to work properly
   * with regular expression templates.
   *<p>
   * Can be overridden by extending classes, to provide custom handling for
   * regular expression queries, which may be necessary due to missing analyzer
   * calls.
   *
   * @param field Name of the field query will use.
   * @param termStr Term token that contains a regular expression
   *
   * @return Resulting {@link org.apache.lucene.search.Query} built for the term
   * @exception org.apache.lucene.queryparser.classic.ParseException throw in overridden method to disallow
   */
  protected Query getRegexpQuery(String field, String termStr) throws ParseException
  {
    if (lowercaseExpandedTerms) {
      termStr = termStr.toLowerCase(locale);
    }
    Term t = new Term(field, termStr);
    return newRegexpQuery(t);
  }

  /**
   * Factory method for generating a query (similar to
   * {@link #getWildcardQuery}). Called when parser parses an input term
   * token that uses prefix notation; that is, contains a single '*' wildcard
   * character as its last character. Since this is a special case
   * of generic wildcard term, and such a query can be optimized easily,
   * this usually results in a different query object.
   *<p>
   * Depending on settings, a prefix term may be lower-cased
   * automatically. It will not go through the default Analyzer,
   * however, since normal Analyzers are unlikely to work properly
   * with wildcard templates.
   *<p>
   * Can be overridden by extending classes, to provide custom handling for
   * wild card queries, which may be necessary due to missing analyzer calls.
   *
   * @param field Name of the field query will use.
   * @param termStr Term token to use for building term for the query
   *    (<b>without</b> trailing '*' character!)
   *
   * @return Resulting {@link org.apache.lucene.search.Query} built for the term
   * @exception org.apache.lucene.queryparser.classic.ParseException throw in overridden method to disallow
   */
  protected Query getPrefixQuery(String field, String termStr) throws ParseException
  {
    if (!allowLeadingWildcard && termStr.startsWith("*"))
      throw new ParseException("'*' not allowed as first character in PrefixQuery");
    if (lowercaseExpandedTerms) {
      termStr = termStr.toLowerCase(locale);
    }
    Term t = new Term(field, termStr);
    return newPrefixQuery(t);
  }

   /**
   * Factory method for generating a query (similar to
   * {@link #getWildcardQuery}). Called when parser parses
   * an input term token that has the fuzzy suffix (~) appended.
   *
   * @param field Name of the field query will use.
   * @param termStr Term token to use for building term for the query
   *
   * @return Resulting {@link org.apache.lucene.search.Query} built for the term
   * @exception org.apache.lucene.queryparser.classic.ParseException throw in overridden method to disallow
   */
  protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws ParseException
  {
    if (lowercaseExpandedTerms) {
      termStr = termStr.toLowerCase(locale);
    }
    Term t = new Term(field, termStr);
    return newFuzzyQuery(t, minSimilarity, fuzzyPrefixLength);
  }


   // extracted from the .jj grammar
  Query handleBareTokenQuery(String qfield, Token term, Token fuzzySlop, boolean prefix, boolean wildcard, boolean fuzzy, boolean regexp) throws ParseException {
    Query q;

    String termImage=discardEscapeChar(term.image);
    if (wildcard) {
      q = getWildcardQuery(qfield, term.image);
    } else if (prefix) {
      q = getPrefixQuery(qfield,
          discardEscapeChar(term.image.substring
              (0, term.image.length()-1)));
    } else if (regexp) {
      q = getRegexpQuery(qfield, term.image.substring(1, term.image.length()-1));
    } else if (fuzzy) {
      q = handleBareFuzzy(qfield, fuzzySlop, termImage);
    } else {
      q = getFieldQuery(qfield, termImage, false);
    }
    return q;
  }

  Query handleBareFuzzy(String qfield, Token fuzzySlop, String termImage)
      throws ParseException {
    Query q;
    float fms = fuzzyMinSim;
    try {
      fms = Float.valueOf(fuzzySlop.image.substring(1)).floatValue();
    } catch (Exception ignored) { }
    if(fms < 0.0f){
      throw new ParseException("Minimum similarity for a FuzzyQuery has to be between 0.0f and 1.0f !");
    } else if (fms >= 1.0f && fms != (int) fms) {
      throw new ParseException("Fractional edit distances are not allowed!");
    }
    q = getFuzzyQuery(qfield, termImage, fms);
    return q;
  }

  // extracted from the .jj grammar
  Query handleQuotedTerm(String qfield, Token term, Token fuzzySlop) throws ParseException {
    int s = phraseSlop;  // default
    if (fuzzySlop != null) {
      try {
        s = Float.valueOf(fuzzySlop.image.substring(1)).intValue();
      }
      catch (Exception ignored) { }
    }
    return getFieldQuery(qfield, discardEscapeChar(term.image.substring(1, term.image.length()-1)), s);
  }

  // extracted from the .jj grammar
  Query handleBoost(Query q, Token boost) {
    if (boost != null) {
      float f = (float) 1.0;
      try {
        f = Float.valueOf(boost.image).floatValue();
      }
      catch (Exception ignored) {
    /* Should this be handled somehow? (defaults to "no boost", if
     * boost number is invalid)
     */
      }

      // avoid boosting null queries, such as those caused by stop words
      if (q != null) {
        q = new BoostQuery(q, f);
      }
    }
    return q;
  }



  /**
   * Returns a String where the escape char has been
   * removed, or kept only once if there was a double escape.
   *
   * Supports escaped unicode characters, e. g. translates
   * <code>\\u0041</code> to <code>A</code>.
   *
   */
  String discardEscapeChar(String input) throws ParseException {
    // Create char array to hold unescaped char sequence
    char[] output = new char[input.length()];

    // The length of the output can be less than the input
    // due to discarded escape chars. This variable holds
    // the actual length of the output
    int length = 0;

    // We remember whether the last processed character was
    // an escape character
    boolean lastCharWasEscapeChar = false;

    // The multiplier the current unicode digit must be multiplied with.
    // E. g. the first digit must be multiplied with 16^3, the second with 16^2...
    int codePointMultiplier = 0;

    // Used to calculate the codepoint of the escaped unicode character
    int codePoint = 0;

    for (int i = 0; i < input.length(); i++) {
      char curChar = input.charAt(i);
      if (codePointMultiplier > 0) {
        codePoint += hexToInt(curChar) * codePointMultiplier;
        codePointMultiplier >>>= 4;
        if (codePointMultiplier == 0) {
          output[length++] = (char)codePoint;
          codePoint = 0;
        }
      } else if (lastCharWasEscapeChar) {
        if (curChar == 'u') {
          // found an escaped unicode character
          codePointMultiplier = 16 * 16 * 16;
        } else {
          // this character was escaped
          output[length] = curChar;
          length++;
        }
        lastCharWasEscapeChar = false;
      } else {
        if (curChar == '\\') {
          lastCharWasEscapeChar = true;
        } else {
          output[length] = curChar;
          length++;
        }
      }
    }

    if (codePointMultiplier > 0) {
      throw new ParseException("Truncated unicode escape sequence.");
    }

    if (lastCharWasEscapeChar) {
      throw new ParseException("Term can not end with escape character.");
    }

    return new String(output, 0, length);
  }

  /** Returns the numeric value of the hexadecimal character */
  static final int hexToInt(char c) throws ParseException {
    if ('0' <= c && c <= '9') {
      return c - '0';
    } else if ('a' <= c && c <= 'f'){
      return c - 'a' + 10;
    } else if ('A' <= c && c <= 'F') {
      return c - 'A' + 10;
    } else {
      throw new ParseException("Non-hex character in Unicode escape sequence: " + c);
    }
  }

  /**
   * Returns a String where those characters that QueryParser
   * expects to be escaped are escaped by a preceding <code>\</code>.
   */
  public static String escape(String s) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      // These characters are part of the query syntax and must be escaped
      if (c == '\\' || c == '+' || c == '-' || c == '!' || c == '(' || c == ')' || c == ':'
        || c == '^' || c == '[' || c == ']' || c == '\"' || c == '{' || c == '}' || c == '~'
        || c == '*' || c == '?' || c == '|' || c == '&' || c == '/') {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }

}
