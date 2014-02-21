package org.apache.lucene.queryparser.spans;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.sandbox.queries.SlowFuzzyQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery.RewriteMethod;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;

/**
 * This class overrides some important functionality within QueryParserBase, esp.
 * for generating terminal spanquery nodes: term, range, regex, fuzzy, etc.
 * <p>
 * When SpanQueries are eventually nuked, there should be an easyish 
 * refactoring of classes that extend this class to extend QueryParserBase.
 * <p>
 * This should also allow for an easy transfer to javacc or similar.
 * 
 */
abstract class SpanQueryParserBase extends AnalyzingQueryParserBase {

  //better to make these public in QueryParserBase

  //they are needed in addClause
  public static final int CONJ_NONE   = 0;
  public static final int CONJ_AND    = 1;
  public static final int CONJ_OR     = 2;

  public static final int MOD_NONE    = 0;
  public static final int MOD_NOT     = 10;
  public static final int MOD_REQ     = 11;

  public static final float UNSPECIFIED_BOOST = -1.0f;
  public static final int UNSPECIFIED_SLOP = -1;
  public static final Boolean UNSPECIFIED_IN_ORDER = null;
  public static final float DEFAULT_BOOST = 1.0f;

  public static final boolean DEFAULT_IN_ORDER = true;

  private static final Pattern FUZZY_PATTERN = Pattern
      .compile("~(>)?(?:(\\d+)?(?:\\.(\\d+))?)?(?:,(\\d+))?$");
  private final Pattern WILDCARD_PATTERN = Pattern.compile("([?*]+)");
  
  protected static final int PREFIX = 0;
  protected static final int WILDCARD = 1;
  protected static final int NEITHER_PREFIX_NOR_WILDCARD = 2;

  private int spanNearMaxDistance = 100;
  private int spanNotNearMaxDistance = 50;
  //if a full term is analyzed and the analyzer returns nothing, 
  //should a ParseException be thrown or should I just ignore the full token.
  private boolean throwExceptionForEmptyTerm = false;
  private boolean lowercaseRegex = false;

  ////////
  //Unsupported operations
  ///////

  /**
   * There is no direct parallel in the SpanQuery 
   * world for position increments.  This always throws an UnsupportedOperationException.
   */
  @Override
  public void setEnablePositionIncrements(boolean enable) {
    throw new UnsupportedOperationException("Sorry, position increments are not supported by SpanQueries");
  }

  /**
   * @see #setEnablePositionIncrements(boolean)
   * @return nothing, ever
   */
  @Override
  public boolean getEnablePositionIncrements() {
    throw new UnsupportedOperationException("Sorry, position increments are not supported by SpanQueries");
  }

  /**
   * Unsupported. Try newNearQuery. Always throws UnsupportedOperationException.
   * @return nothing, ever
   */
  @Override
  protected PhraseQuery newPhraseQuery() {
    throw new UnsupportedOperationException("newPhraseQuery not supported.  Try newNearQuery.");
  }

  /**
   * Unsupported. Try newNearQuery. Always throws UnsupportedOperationException.
   * @return nothing, ever
   */
  @Override
  protected MultiPhraseQuery newMultiPhraseQuery() {
    throw new UnsupportedOperationException("newMultiPhraseQuery not supported.  Try newNearQuery.");
  }

  /**
   * Returns new SpanNearQuery.  This is added as parallelism to newPhraseQuery.
   * Not sure it is of any use.
   */
  protected SpanNearQuery newNearQuery(SpanQuery[] queries, int slop, boolean inOrder, boolean collectPayloads) {
    return new SpanNearQuery(queries, slop, inOrder, collectPayloads);
  }

  ///////
  // Override getXQueries to return span queries
  // Lots of boilerplate.  Sorry.
  //////

  @Override 
  protected Query newRegexpQuery(Term t) {
    RegexpQuery query = new RegexpQuery(t);
    query.setRewriteMethod(getMultiTermRewriteMethod(t.field()));
    return new SpanMultiTermQueryWrapper<RegexpQuery>(query);
  }

  /**
   * Currently returns multiTermRewriteMethod no matter the field.
   * This allows for hooks for overriding to handle
   * field-specific MultiTermRewriteMethod handling
   * @return RewriteMethod for a given field
   */
  public RewriteMethod getMultiTermRewriteMethod(String field) {
    return getMultiTermRewriteMethod();
  }

  @Override
  protected Query getRegexpQuery(String field, String termStr) throws ParseException {
    if (getLowercaseRegex()) {
      termStr = termStr.toLowerCase(getLocale());
    }
    Term t = new Term(field, termStr);
    return newRegexpQuery(t);
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
  protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws ParseException {
    return getFuzzyQuery(field, termStr, minSimilarity, getFuzzyPrefixLength(), FuzzyQuery.defaultTranspositions);
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
  protected Query getFuzzyQuery(String field, String termStr, 
      float minSimilarity, int prefixLength) throws ParseException {
    return getFuzzyQuery(field, termStr, minSimilarity, prefixLength, FuzzyQuery.defaultTranspositions);
  }

  /**
   * nocommit
   * @return query
   * @throws ParseException, RuntimeException if there was an IOException from the analysis process
   */
  protected Query getFuzzyQuery(String field, String termStr,
      float minSimilarity, int prefixLength, boolean transpositions) throws ParseException {
    if (getNormMultiTerms() == NORM_MULTI_TERMS.ANALYZE) {
      termStr = analyzeMultitermTermParseEx(field, termStr).utf8ToString();
    } else if (getNormMultiTerms() == NORM_MULTI_TERMS.LOWERCASE) {
      termStr = termStr.toLowerCase(getLocale());
    }
    Term t = new Term(field, unescape(termStr));
    return newFuzzyQuery(t, minSimilarity, prefixLength, transpositions);
  }

  /**
   * Creates a new fuzzy term. 
   * If minimumSimilarity is >= 1.0f, this rounds to avoid
   * exception for numEdits != whole number.
   * 
   * @return fuzzy query
   */
  protected Query newFuzzyQuery(Term t, float minimumSimilarity, int prefixLength,
      boolean transpositions) {

    if (minimumSimilarity <=0.0f) {
      return newTermQuery(t);
    }
    String text = t.text();
    int numEdits = 0;
    int len = text.codePointCount(0, text.length());
    if (getFuzzyMinSim() < 1.0f) {
      //if both are < 1.0 then make sure that parameter that was passed in
      //is >= than fuzzyminsim
      if (minimumSimilarity < 1.0f) {
        minimumSimilarity = (minimumSimilarity < getFuzzyMinSim())? getFuzzyMinSim() : minimumSimilarity;
        numEdits = unboundedFloatToEdits(minimumSimilarity, len);
      } else {
        //if fuzzyMinSim < 1.0 and the parameter that was passed in
        //is >= 1, convert that to a %, test against fuzzyminsim and then
        //recalculate numEdits
        float tmpSim = (len-minimumSimilarity)/(float)len;
        tmpSim = (tmpSim < getFuzzyMinSim())? getFuzzyMinSim() : tmpSim;
        numEdits = unboundedFloatToEdits(tmpSim, len);
      }
    } else {
      //if fuzzyMinSim >= 1.0f

      if (minimumSimilarity < 1.0f) {
        int tmpNumEdits = unboundedFloatToEdits(minimumSimilarity, len);
        numEdits = (tmpNumEdits >= (int)getFuzzyMinSim())?(int)getFuzzyMinSim() : tmpNumEdits;
      } else {
        numEdits = (minimumSimilarity >= getFuzzyMinSim())? (int) getFuzzyMinSim() : (int)minimumSimilarity;
      }
    }
    /*
     * This currently picks between FQ and SlowFQ based on numEdits.
     * This is only because SFQ doesn't allow transpositions yet.
     * Once SFQ does allow transpositions, this can be changed to
     * run SFQ only...because SFQ does the right thing and returns
     * an Automaton for numEdits <= 2.
     */
    if (numEdits <= FuzzyQuery.defaultMaxEdits) {
      FuzzyQuery fq =new FuzzyQuery(t, numEdits, prefixLength, FuzzyQuery.defaultMaxExpansions,
          transpositions);
      fq.setRewriteMethod(getMultiTermRewriteMethod(t.field()));
      return new SpanMultiTermQueryWrapper<FuzzyQuery>(fq);
    } else {
      SlowFuzzyQuery sfq = new SlowFuzzyQuery(t,
          numEdits, prefixLength);
      sfq.setRewriteMethod(getMultiTermRewriteMethod(t.field()));
      return new SpanMultiTermQueryWrapper<SlowFuzzyQuery>(sfq);
    }
  }

  @Override 
  protected Query newPrefixQuery(Term t) {
    PrefixQuery q = new PrefixQuery(t);
    q.setRewriteMethod(getMultiTermRewriteMethod(t.field()));
    return new SpanMultiTermQueryWrapper<PrefixQuery>(q);
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
  protected Query getPrefixQuery(String field, String termStr) throws ParseException {
    if (!getAllowLeadingWildcard() && termStr.startsWith("*"))
      throw new ParseException("'*' not allowed as first character in PrefixQuery");

    if (getNormMultiTerms() == NORM_MULTI_TERMS.ANALYZE) {
      termStr = analyzeMultitermTermParseEx(field, termStr).utf8ToString();
    } else if (getNormMultiTerms() == NORM_MULTI_TERMS.LOWERCASE) {
      termStr = termStr.toLowerCase(getLocale());
    }
    Term t = new Term(field, unescape(termStr));
    return newPrefixQuery(t);
  }

  @Override
  protected Query newWildcardQuery(Term t) {
    WildcardQuery q = new WildcardQuery(t);
    q.setRewriteMethod(getMultiTermRewriteMethod(t.field()));
    return new SpanMultiTermQueryWrapper<WildcardQuery>(q);
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
  @Override
  protected Query getWildcardQuery(String field, String termStr) throws ParseException {
    if ("*".equals(field)) {
      if ("*".equals(termStr)) return newMatchAllDocsQuery();
    }
    if (!getAllowLeadingWildcard() && (termStr.startsWith("*") || termStr.startsWith("?")))
      throw new ParseException("'*' or '?' not allowed as first character in WildcardQuery");

    if (getNormMultiTerms() == NORM_MULTI_TERMS.ANALYZE) {

      termStr = analyzeWildcard(field, termStr);
    } else if (getNormMultiTerms() == NORM_MULTI_TERMS.LOWERCASE) {
      termStr = termStr.toLowerCase(getLocale());
    }

    Term t = new Term(field, termStr);
    return newWildcardQuery(t);
  }

  /**
   * Builds a new {@link TermRangeQuery} instance.
   * Will convert to lowercase if {@link #getLowercaseExpandedTerms()} == true.
   * Will analyze terms if {@link #getAnalyzeRangeTerms()} == true.
   * 
   * 
   * @param field Field
   * @param part1 min
   * @param part2 max
   * @param startInclusive true if the start of the range is inclusive
   * @param endInclusive true if the end of the range is inclusive
   * @return new {@link TermRangeQuery} instance
   */
  @Override
  protected Query newRangeQuery(String field, String part1, String part2, 
      boolean startInclusive, boolean endInclusive) {
    //TODO: modify newRangeQuery in QueryParserBase to throw ParseException for failure of analysis
    //need to copy and paste this until we can change analyzeMultiterm(String field, String part) to protected
    //if we just returned a spanmultitermwrapper around super.newRangeQuery(), analyzeMultiterm would use 
    //the analyzer, but not the multitermAnalyzer
    String start = null;
    String end = null;

    if (part1 == null) {
      start = null;
    } else {
      if (getAnalyzeRangeTerms()) {
        try {
          start = analyzeMultitermTermParseEx(field, part1).utf8ToString();
        } catch (ParseException e) {
          //swallow
        }
      }
      //if there was an exception during analysis, swallow it and
      //try for lowercase
      if ((start == null && getAnalyzeRangeTerms()) ||
          getNormMultiTerms() == NORM_MULTI_TERMS.LOWERCASE) {
        start = part1.toLowerCase(getLocale());
      } else {
        start = part1;
      }
    }

    if (part2 == null) {
      end = null;
    } else {
      if (getAnalyzeRangeTerms()) {
        try { 
          end = analyzeMultitermTermParseEx(field, part1).utf8ToString();
        } catch (ParseException e) {
          //swallow..doh!
        }
      }
      if ((end == null && getAnalyzeRangeTerms()) ||
          getNormMultiTerms() == NORM_MULTI_TERMS.LOWERCASE) {
        end = part2.toLowerCase(getLocale());
      } else {
        end = part2;
      }
    }

    start = (start == null) ? null : unescape(start);
    end = (end == null) ? null : unescape(end);
    final TermRangeQuery query =
        TermRangeQuery.newStringRange(field, start, end, startInclusive, endInclusive);

    query.setRewriteMethod(getMultiTermRewriteMethod(field));
    return new SpanMultiTermQueryWrapper<TermRangeQuery>(query);
  }

  /**
   * This identifies and then builds the various single term and/or multiterm
   * queries, including MatchAllDocsQuery. This does not identify a regex or range term query!
   * 
   * <p>
   * For {@link org.apache.lucene.search.FuzzyQuery}, this defaults to 
   * {@link org.apache.lucene.search.FuzzyQuery#defaultMaxEdits}
   * if no value is specified after the ~.
   * @return SpanQuery, MatchAllDocsQuery or null if termText is a stop word
   */
  public Query buildAnySingleTermQuery(String field, String termText, boolean quoted) throws ParseException {

    if (quoted == true) {
      return getFieldQuery(field, termText, quoted);
    }

    if (quoted == false && field.equals("*") && termText.equals("*")) {
      return new MatchAllDocsQuery();
    }

    Query q = null;
    // is this a fuzzy term?
    Matcher m = FUZZY_PATTERN.matcher(termText);

    if (m.find() && ! isCharEscaped(termText, m.start())) {
      String term = termText.substring(0, m.start());
      String transposString = m.group(1);
      String minSimilarityString = m.group(2);
      String decimalComponent = m.group(3);
      String prefixLenString = m.group(4);
      float minSimilarity = (float) FuzzyQuery.defaultMaxEdits;
      if (minSimilarityString != null && minSimilarityString.length() > 0) {
        if (decimalComponent == null || decimalComponent.length() == 0) {
          decimalComponent = "0";
        }
        try {
          minSimilarity = Float.parseFloat(minSimilarityString + "." + decimalComponent);
        } catch (NumberFormatException e) {
          // shouldn't ever happen. If it does, fall back to original value of
          // slop
          // swallow
        }
      }

      // if the user enters 2.4 for example, round it so that there won't be
      // an
      // illegalparameter exception
      if (minSimilarity >= 1.0f) {
        minSimilarity = (float) Math.round(minSimilarity);
      }

      int prefixLen = getFuzzyPrefixLength();
      if (prefixLenString != null) {
        try {
          prefixLen = Integer.parseInt(prefixLenString);
        } catch (NumberFormatException e) {
          //swallow
        }
      }
      boolean transpositions = (transposString != null) ? false : true;

      q = getFuzzyQuery(field, term, minSimilarity, prefixLen, transpositions);
      return q;
    }

    int wildCardOrPrefix = testWildCardOrPrefix(termText);
    if (wildCardOrPrefix == PREFIX) {
      q = getPrefixQuery(field,
          termText.substring(0, termText.length() - 1));
    } else if (wildCardOrPrefix == WILDCARD) {
      q = getWildcardQuery(field, termText);
    }
    // if you've found anything, return it
    if (q != null) {
      return q;
    }
    // treat as basic single term query    
    return getFieldQuery(field, termText, quoted);
  }

  protected int testWildCardOrPrefix(String termText) {
    if (termText == null || termText.length() == 0) {
      return NEITHER_PREFIX_NOR_WILDCARD;
    }

    Matcher m = WILDCARD_PATTERN.matcher(termText);
    Set<Integer> ws = new HashSet<Integer>();
    while (m.find()) {
      if (! isCharEscaped(termText, m.start())) {
        ws.add(m.start());
      }
    }
    if (ws.size() > 0) {

      if (ws.size() == 1 // there's only one wildcard character
          && ws.contains(termText.length() - 1) // it isn't escaped
          && termText.indexOf("*") == termText.length() - 1 // it is * not ?
          && termText.length() > 1) { //it isn't just * by itself
        return PREFIX;
      } else {
        return WILDCARD;
      }
    }
    return NEITHER_PREFIX_NOR_WILDCARD;
  }

  @Override
  protected Query newTermQuery(Term t) {
    t = unescape(t);
    return new SpanTermQuery(t);
  }

  @Override
  protected Query getFieldQuery(String field, String termText, boolean quoted)
      throws ParseException {
    return newFieldQuery(getAnalyzer(field), field, termText, quoted);
  }

  @Override
  protected Query getFieldQuery(String field, String queryText, int slop)
      throws ParseException {
    Query query = getFieldQuery(field, queryText, true);

    if (query instanceof SpanNearQuery) {
      if (((SpanNearQuery)query).getSlop() != slop) {
        slop = (spanNearMaxDistance > -1 && slop > spanNearMaxDistance) ? spanNearMaxDistance : slop;
        SpanQuery[] clauses = ((SpanNearQuery) query).getClauses();
        query = new SpanNearQuery(clauses, slop, true);
      }
    }

    return query;
  }
  
  /**
   * Build what appears to be a simple single term query. If the analyzer breaks
   * it into multiple terms, treat that as a "phrase" or as an "or" depending on
   * the value of {@link #autoGeneratePhraseQueries}.
   * 
   * If the analyzer is null, this returns {@link #handleNullAnalyzer(String, String)}
   * 
   * Can return null!
   * @param quoted -- is the term quoted
   * @return query
   */
  @Override 
  protected Query newFieldQuery(Analyzer analyzer, String fieldName, String termText, boolean quoted)
      throws ParseException {
    if (analyzer == null) {
      return handleNullAnalyzer(fieldName, termText);
    }
    //largely plagiarized from QueryParserBase
    TokenStream source;
    try {
      source = analyzer.tokenStream(fieldName, termText);
      source.reset();
    } catch (IOException e) {
      ParseException p = new ParseException("Unable to initialize TokenStream to analyze query text");
      p.initCause(e);
      throw p;
    }
    CachingTokenFilter buffer = new CachingTokenFilter(source);
    TermToBytesRefAttribute termAtt = null;
    PositionIncrementAttribute posIncrAtt = null;
    OffsetAttribute offsetAtt = null;
    int numTokens = 0;

    buffer.reset();

    if (buffer.hasAttribute(TermToBytesRefAttribute.class)) {
      termAtt = buffer.getAttribute(TermToBytesRefAttribute.class);
    }
    if (buffer.hasAttribute(PositionIncrementAttribute.class)) {
      posIncrAtt = buffer.getAttribute(PositionIncrementAttribute.class);
    }
    if (buffer.hasAttribute(OffsetAttribute.class)) {
      offsetAtt = buffer.getAttribute(OffsetAttribute.class);
    }

    boolean hasMoreTokens = false;
    if (termAtt != null) {
      try {
        hasMoreTokens = buffer.incrementToken();
        while (hasMoreTokens) {
          numTokens++;
          hasMoreTokens = buffer.incrementToken();
        }
      } catch (IOException e) {
        // ignore
      }
    }
    try {
      // rewind the buffer stream
      buffer.reset();
      //source.end();
      // close original stream - all tokens buffered
      source.close();
    }
    catch (IOException e) {
      ParseException p = new ParseException("Cannot close TokenStream analyzing query text");
      p.initCause(e);
      throw p;
    }

    BytesRef bytes = termAtt == null ? null : termAtt.getBytesRef();

    if (numTokens == 0) {
      if (throwExceptionForEmptyTerm) {
        throw new ParseException("Couldn't find any content term in: "+ termText);
      }
      return null;
    } else if (numTokens == 1) {
      try {
        boolean hasNext = buffer.incrementToken();
        assert hasNext == true;
        termAtt.fillBytesRef();
      } catch (IOException e) {
        // safe to ignore, because we know the number of tokens
      }
      return newTermQuery(new Term(fieldName, BytesRef.deepCopyOf(bytes)));
    } else {

      List<SpanQuery> queries = new ArrayList<SpanQuery>();
      try {
        if (posIncrAtt != null) {
          analyzeComplexSingleTerm(fieldName, buffer, termAtt, bytes, posIncrAtt, queries);
        } else if (offsetAtt != null) {
          analyzeComplexSingleTerm(fieldName, buffer, termAtt, bytes, offsetAtt, queries);
        } else {
          while (buffer.incrementToken()) {
            termAtt.fillBytesRef();
            queries.add((SpanTermQuery)newTermQuery(new Term(fieldName, BytesRef.deepCopyOf(bytes))));
          }
        }
      } catch (IOException e) {
        //ignore
      }
      List<SpanQuery> nonEmpties = new LinkedList<SpanQuery>();
      for (SpanQuery piece : queries) {
        if (piece != null) {
          nonEmpties.add(piece);
        } else if (piece == null && throwExceptionForEmptyTerm) {
          throw new ParseException("Stop word found in " + termText);
        }
      }

      if (nonEmpties.size() == 0) {
        return getEmptySpanQuery();
      }
      if (nonEmpties.size() == 1) {
        return nonEmpties.get(0);
      }
      SpanQuery[] ret = nonEmpties
          .toArray(new SpanQuery[nonEmpties.size()]);
      if (quoted || getAutoGeneratePhraseQueries() == true) {
        return new SpanNearQuery(ret, 0, true);
      } else {
        return new SpanOrQuery(ret);
      }
    }
  }

  /**
   * Built to be overridden.  In SpanQueryParserBase, this returns SpanTermQuery
   * with no modifications to termText
   * @return query
   */
  public Query handleNullAnalyzer(String fieldName, String termText) {
    int prefixOrWildcard = testWildCardOrPrefix(termText);
    if (prefixOrWildcard == PREFIX) {
      return handleNullAnalyzerPrefix(fieldName, termText);
    }
    return new SpanTermQuery(new Term(fieldName, termText));
  }

  /**
   * Built to be overridden.  In SpanQueryParserBase, this returns SpanTermQuery
   * or prefix with no modifications to termText.
   * @return query
   */
  public Query handleNullAnalyzerPrefix(String fieldName, String prefix) {
    return new SpanTermQuery(new Term(fieldName, prefix));
  }
  
  /**
   * Built to be overridden.  In SpanQueryParserBase, this returns SpanTermQuery
   * with no modifications to termText
   * 
   * @return query
   */
  public Query handleNullAnalyzerRange(String fieldName, String start,
      String end, boolean startInclusive, boolean endInclusive) {
    final TermRangeQuery query =
        TermRangeQuery.newStringRange(fieldName, start, end, startInclusive, endInclusive);

    query.setRewriteMethod(getMultiTermRewriteMethod(fieldName));
    return new SpanMultiTermQueryWrapper<TermRangeQuery>(query);
  }

  private void analyzeComplexSingleTerm(String fieldName,
      CachingTokenFilter ts, TermToBytesRefAttribute termAtt, BytesRef bytes,
      OffsetAttribute offAtt,
      List<SpanQuery> queries) throws IOException {
    int lastStart = -1;
    while (ts.incrementToken()) {
      termAtt.fillBytesRef();
      //if start is the same, treat it as a synonym...ignore end because
      //of potential for shingles
      if (lastStart > -1 && offAtt.startOffset() == lastStart)
        //&& offAttr.endOffset() == lastEnd)
      {

        handleSyn(queries, (SpanTermQuery)newTermQuery(new Term(fieldName, BytesRef.deepCopyOf(bytes))));
      } else {
        queries.add((SpanTermQuery)newTermQuery(new Term(fieldName, BytesRef.deepCopyOf(bytes))));
      }
      lastStart = offAtt.startOffset();
    }
  }

  private void analyzeComplexSingleTerm(String fieldName,
      CachingTokenFilter ts, TermToBytesRefAttribute termAtt, BytesRef bytes, 
      PositionIncrementAttribute posAtt, 
      List<SpanQuery> queries) throws IOException {
    while (ts.incrementToken()) {
      termAtt.fillBytesRef();
      if (posAtt.getPositionIncrement() == 0) {
        handleSyn(queries, (SpanTermQuery)newTermQuery(new Term(fieldName, BytesRef.deepCopyOf(bytes))));
      } else {
        //add null for stop words
        for (int i = 1; i < posAtt.getPositionIncrement(); i++) {
          queries.add(null);
        }
        queries.add((SpanTermQuery)newTermQuery(new Term(fieldName, BytesRef.deepCopyOf(bytes))));
      }
    }
  }

  private void handleSyn(List<SpanQuery> queries, SpanQuery currQuery) {
    assert(queries != null);
    //grab the last query added to queries
    SpanQuery last = null;
    boolean removed = false;
    if (queries.size() > 0) {
      last = queries.remove(queries.size()-1);
      removed = true;
    }
    //if it exists and does not equal null
    if (last != null) {
      if (last instanceof SpanOrQuery) {
        ((SpanOrQuery)last).addClause(currQuery);
      } else {
        SpanQuery tmp = last;
        last = new SpanOrQuery();
        ((SpanOrQuery)last).addClause(tmp);
        ((SpanOrQuery)last).addClause(currQuery);
      }
      queries.add(last);
    } else {
      //if you actually removed a null, put it back on
      if (removed) {
        queries.add(null);
      }
      //then add the new term
      queries.add(currQuery);
    }
  }

  /**
   * 
   * @return {@link org.apache.lucene.search.spans.SpanOrQuery} might be empty if clauses is null or contains
   *         only empty queries
   */   
  protected SpanQuery buildSpanOrQuery(List<SpanQuery> clauses) 
      throws ParseException {
    if (clauses == null || clauses.size() == 0)
      return getEmptySpanQuery();

    List<SpanQuery> nonEmpties = removeEmpties(clauses);
    if (nonEmpties.size() == 0) {
      return getEmptySpanQuery();
    }
    if (nonEmpties.size() == 1)
      return nonEmpties.get(0);

    SpanQuery[] arr = nonEmpties.toArray(new SpanQuery[nonEmpties.size()]);
    return new SpanOrQuery(arr);
  }


  protected SpanQuery buildSpanNearQuery(List<SpanQuery> clauses, int slop,
      Boolean inOrder) throws ParseException {
    if (clauses == null || clauses.size() == 0)
      return getEmptySpanQuery();

    List<SpanQuery> nonEmpties = removeEmpties(clauses);

    if (nonEmpties.size() == 0) {
      return getEmptySpanQuery();
    }
    if (nonEmpties.size() == 1) {
      SpanQuery child = nonEmpties.get(0);
      //if single child is itself a SpanNearQuery, inherit slop and inorder
      if (child instanceof SpanNearQuery) {
        SpanQuery[] childsClauses = ((SpanNearQuery)child).getClauses();
        child = new SpanNearQuery(childsClauses, slop, inOrder);
      }
    }

    if (slop == UNSPECIFIED_SLOP) {
      slop = getPhraseSlop();
    } else if  (spanNearMaxDistance > -1 && slop > spanNearMaxDistance) {
      slop = spanNearMaxDistance;
    }

    boolean localInOrder = DEFAULT_IN_ORDER;
    if (inOrder != UNSPECIFIED_IN_ORDER) {
      localInOrder = inOrder.booleanValue();
    }

    SpanQuery[] arr = nonEmpties.toArray(new SpanQuery[nonEmpties.size()]);
    return new SpanNearQuery(arr, slop, localInOrder);
  }

  /**
   * This is meant to "fix" two cases that might be surprising to a
   * non-whitespace language speaker. If a user entered, e.g. "\u5927\u5B66"~3,
   * and {@link #autoGeneratePhraseQueries} is set to true, then the parser
   * would treat this recursively and yield [[\u5927\u5B66]]~3 by default. The user
   * probably meant: find those two characters within three words of each other,
   * not find those right next to each other and that hit has to be within three
   * words of nothing.
   * 
   * If a user entered the same thing and {@link #autoGeneratePhraseQueries} is
   * set to false, then the parser would treat this as [(\u5927\u5B66)]~3: find
   * one character or the other and then that hit has to be within three words
   * of nothing...not the desired outcome  * @param field
   * 
   * 
   * @param termText this is the sole child of a SpanNearQuery as identified by a whitespace-based tokenizer
   * @return query
   */
  protected Query specialHandlingForSpanNearWithOneComponent(String field,
      String termText, 
      int ancestralSlop, Boolean ancestralInOrder) throws ParseException {
    Query q = newFieldQuery(getAnalyzer(field), field, termText, true);
    if (q instanceof SpanNearQuery) {
      SpanQuery[] childsClauses = ((SpanNearQuery)q).getClauses();
      return buildSpanNearQuery(Arrays.asList(childsClauses), ancestralSlop, ancestralInOrder);
    }
    return q;
  }

  protected SpanQuery buildSpanNotNearQuery(List<SpanQuery> clauses, int pre,
      int post) throws ParseException {
    if (clauses.size() != 2) {
      throw new ParseException(
          String.format("SpanNotNear query must have two clauses. I count %d",
              clauses.size()));
    }
    // if include is an empty query, treat this as just an empty query
    if (isEmptyQuery(clauses.get(0))) {
      return clauses.get(0);
    }
    // if exclude is an empty query, return include alone
    if (isEmptyQuery(clauses.get(1))) {
      return clauses.get(0);
    }

    if (spanNotNearMaxDistance > -1 && pre > spanNotNearMaxDistance) {
      pre = spanNotNearMaxDistance;
    }
    if (spanNotNearMaxDistance > -1 && post > spanNotNearMaxDistance) {
      post = spanNotNearMaxDistance;
    }
    return new SpanNotQuery(clauses.get(0), clauses.get(1), pre, post);
  }


  private List<SpanQuery> removeEmpties(List<SpanQuery> queries) 
      throws ParseException {

    List<SpanQuery> nonEmpties = new ArrayList<SpanQuery>();
    for (SpanQuery q : queries) {
      if (!isEmptyQuery(q)) {
        nonEmpties.add(q);
      } else if (throwExceptionForEmptyTerm) {
        throw new ParseException("Stop word or unparseable term found");
      }
    }
    return nonEmpties;
  }

  public SpanQuery getEmptySpanQuery() {
    SpanQuery q = new SpanOrQuery(new SpanTermQuery[0]);
    return q;
  }

  public boolean isEmptyQuery(Query q) {
    if (q == null ||
        q instanceof SpanOrQuery && ((SpanOrQuery) q).getClauses().length == 0) {
      return true;
    }
    return false;
  }

  public static Term unescape(Term t) {

    String txt = t.text();
    try {
      UnescapedCharSequence un = EscapeQuerySyntaxImpl.discardEscapeChar(txt);

      if (! un.toString().equals(txt)) {
        t = new Term(t.field(),un.toString());
      }
    } catch (org.apache.lucene.queryparser.flexible.standard.parser.ParseException e) {
      //swallow;
    }

    return t;
  }

  public static String unescape(String s) {
    try {
      UnescapedCharSequence un = EscapeQuerySyntaxImpl.discardEscapeChar(s);
      return un.toString();
    } catch (org.apache.lucene.queryparser.flexible.standard.parser.ParseException e) {
      //swallow;
    }

    return s;
  }

  /**
   * 
   * @return maximum distance allowed for a SpanNear query.  Can return negative values.
   */
  public int getSpanNearMaxDistance() {
    return spanNearMaxDistance;
  }

  /**
   * 
   * @param spanNearMaxDistance maximum distance for a SpanNear (phrase) query. If < 0, 
   * there is no limitation on distances in SpanNear queries.
   */
  public void setSpanNearMaxDistance(int spanNearMaxDistance) {
    this.spanNearMaxDistance = spanNearMaxDistance;
  }

  /**
   * 
   * @return maximum distance allowed for a SpanNotNear query.  
   * Can return negative values.
   */
  public int getSpanNotNearMaxDistance() {
    return spanNotNearMaxDistance;
  }

  /**
   * 
   * @param spanNotNearMaxDistance maximum distance for the previous and post distance for a SpanNotNear query. If < 0, 
   * there is no limitation on distances in SpanNotNear queries.
   */
  public void setSpanNotNearMaxDistance(int spanNotNearMaxDistance) {
    this.spanNotNearMaxDistance = spanNotNearMaxDistance;
  }

  /**
   * If the a term passes through the analyzer and nothing comes out,
   * throw an exception or silently ignore the missing term.  This can
   * happen with stop words or with other strings that the analyzer
   * ignores.
   * 
   * <p>
   * This is applied only at the full term level.
   * <p>
   * Currently, a parseException is thrown no matter the setting on this
   * whenever an analyzer can't return a value for a multiterm query.
   *  
   * @return throw exception if analyzer yields empty term
   */
  public boolean getThrowExceptionForEmptyTerm() {
    return throwExceptionForEmptyTerm;
  }

  /**
   * @see #getThrowExceptionForEmptyTerm()
   */
  public void setThrowExceptionForEmptyTerm(boolean throwExceptionForEmptyTerm) {
    this.throwExceptionForEmptyTerm = throwExceptionForEmptyTerm;
  }

  protected static boolean isCharEscaped(String s, int i) {
    int j = i;
    int esc = 0;
    while (--j >=0 && s.charAt(j) == '\\') {
      esc++;
    }
    if (esc % 2 == 0) {
      return false;
    }
    return true;
  }
  
  /**
   * Copied nearly exactly from FuzzyQuery's floatToEdits.
   * <p>
   * There are two differences:
   * <p>
   * <ol>
   * <li>FuzzyQuery's floatToEdits requires that the return value 
   * be <= LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE<li>
   * <li>This adds a small amount so that nearly exact 
   * hits don't get floored: 0.80 for termLen 5 should = 1</li>
   * <ol>
   * @return edits
   */
  public static int unboundedFloatToEdits(float minimumSimilarity, int termLen) {
    if (minimumSimilarity >= 1f) {
      return (int)minimumSimilarity;
    } else if (minimumSimilarity == 0.0f) {
      return 0; // 0 means exact, not infinite # of edits!
    } else {
      return (int)(0.00001f+(1f-minimumSimilarity) * termLen);
    }
  }
  
  public boolean getLowercaseRegex() {
    return lowercaseRegex;
  }
  
  public void setLowercaseRegex(boolean lowercaseRegex) {
    this.lowercaseRegex = lowercaseRegex;
  }
}
