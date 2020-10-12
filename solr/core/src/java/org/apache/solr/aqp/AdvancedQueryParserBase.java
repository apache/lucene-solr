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
package org.apache.solr.aqp;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.queryparser.charstream.CharStream;
import org.apache.lucene.queryparser.charstream.FastCharStream;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.solr.analysis.ReversedWildcardFilterFactory;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.SolrException;
import org.apache.solr.parser.Operator;
import org.apache.solr.parser.SynonymQueryStyle;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.SyntaxError;

import static org.apache.solr.parser.SynonymQueryStyle.AS_SAME_TERM;

/**
 * This class is overridden by QueryParser in QueryParser.jj
 * and acts to separate the majority of the Java code from the .jj grammar file.
 */
@SuppressWarnings("Duplicates")
public abstract class AdvancedQueryParserBase extends QueryBuilder implements QueryParserConstants {

  private static final String REVERSE_WILDCARD_LOWER_BOUND = new String(new char[]{ReverseStringFilter.START_OF_HEADING_MARKER + 1});

  private static final int TERMS_QUERY_THRESHOLD = 16;   // @lucene.internal Set to a low value temporarily for better test coverage
  private static final int MIN_PREFIX_TERM_LENGTH = 3;

  static final int MOD_NONE = 0;
  static final int MOD_NOT = 10;
  static final int MOD_MUST = 11;
  static final int MOD_SHOULD = 12;

  private SynonymQueryStyle synonymQueryStyle = AS_SAME_TERM;

  // make it possible to call setDefaultOperator() without accessing
  // the nested class:
  /**
   * Alternative form of QueryParser.Operator.AND
   */
  static final Operator AND_OPERATOR = Operator.AND;
  /**
   * Alternative form of QueryParser.Operator.OR
   */
  private static final Operator OR_OPERATOR = Operator.OR;

  /**
   * The default operator that parser uses to combine query terms
   */
  Operator operator = OR_OPERATOR;

  private String defaultField;

  private boolean autoGeneratePhraseQueries = false;
  private int flags;

  private IndexSchema schema;
  private QParser parser;

  // implementation detail - caching ReversedWildcardFilterFactory based on type
  private Map<FieldType, ReversedWildcardFilterFactory> leadingWildcards;

  // internal: A simple raw fielded query
  static class RawQuery extends Query {
    final SpanContext spanContext;
    final SchemaField sfield;
    private final List<String> externalVals;

    RawQuery(SchemaField sfield, String externalVal, SpanContext spanContext) {
      this(sfield, Collections.singletonList(externalVal), spanContext);
    }

    RawQuery(SchemaField sfield, List<String> externalVals, SpanContext spanContext) {
      this.sfield = sfield;
      this.externalVals = externalVals;
      this.spanContext = spanContext;
    }

    int getTermCount() {
      return externalVals.size();
    }

    List<String> getExternalVals() {
      return externalVals;
    }

    String getJoinedExternalVal() {
      return externalVals.size() == 1 ? externalVals.get(0) : String.join(" ", externalVals);
    }

    @Override
    public String toString(String field) {
      return "RAW(" + field + "," + getJoinedExternalVal() + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
      return false;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  // So the generated QueryParser(CharStream) won't error out
  AdvancedQueryParserBase() {
    super(null);
  }

  // the generated parser will create these in QueryParser
  abstract void ReInit(CharStream stream);

  @SuppressWarnings("SameParameterValue")
  abstract Query TopLevelQuery(String field) throws ParseException, SyntaxError;

  public void init(String defaultField, QParser parser) {
    this.schema = parser.getReq().getSchema();
    this.parser = parser;
    this.flags = parser.getFlags();
    this.defaultField = defaultField;
    setAnalyzer(schema.getQueryAnalyzer());
  }

  /**
   * Parses a query string, returning a {@link Query}.
   *
   * @param query the query string to be parsed.
   */
  public Query parse(String query) throws SyntaxError {
    ReInit(new FastCharStream(new StringReader(query)));
    try {
      // TopLevelQuery is a Query followed by the end-of-input (EOF)
      Query res = TopLevelQuery(null);  // pass null so we can tell later if an explicit field was provided or not
      return res != null ? res : newBooleanQuery().build();
    } catch (ParseException | TokenMgrError tme) {
      throw new SyntaxError("Cannot parse '" + query + "': " + tme.getMessage(), tme);
    } catch (BooleanQuery.TooManyClauses tmc) {
      throw new SyntaxError("Cannot parse '" + query + "': too many boolean clauses", tmc);
    }
  }


  /**
   * @return Returns the default field.
   */
  public String getDefaultField() {
    return this.defaultField;
  }

  private String explicitField;

  /**
   * Handles the default field if null is passed
   */
  String getField(String fieldName) {
    explicitField = fieldName;
    return fieldName != null ? fieldName : this.defaultField;
  }

  /**
   * For a fielded query, returns the actual field specified (i.e. null if default is being used)
   * myfield:A or myfield:(A B C) will both return "myfield"
   */
  private String getExplicitField() {
    return explicitField;
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
   * Set how overlapping query terms (ie synonyms) should be scored, as if they're the same term,
   * picking highest scoring term, or OR'ing them together
   *
   * @param synonymQueryStyle how to score terms that overlap see {{@link SynonymQueryStyle}}
   */
  private void setSynonymQueryStyle(SynonymQueryStyle synonymQueryStyle) {
    this.synonymQueryStyle = synonymQueryStyle;
  }

  /**
   * Gets how overlapping query terms should be scored
   */
  public SynonymQueryStyle getSynonymQueryStyle() {
    return this.synonymQueryStyle;
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

  void addClause(List<BooleanClause> clauses, int mods, Query q) {
    // We might have been passed a null query; the term might have been
    // filtered away by the analyzer.
    if (q == null)
      return;

    switch (mods) {
      case MOD_NOT:
        clauses.add(newBooleanClause(q, BooleanClause.Occur.MUST_NOT));
        break;
      case MOD_MUST:
        clauses.add(newBooleanClause(q, BooleanClause.Occur.MUST));
        break;
      case MOD_SHOULD:
        clauses.add(newBooleanClause(q, BooleanClause.Occur.SHOULD));
        break;
      default:
        if (operator == OR_OPERATOR) {
          clauses.add(newBooleanClause(q, BooleanClause.Occur.SHOULD));
        } else {
          clauses.add(newBooleanClause(q, BooleanClause.Occur.MUST));
        }
    }
  }

  /**
   * Called from QueryParser's MultiTerm rule.
   * Assumption: no conjunction or modifiers (conj == CONJ_NONE and mods == MOD_NONE)
   */
  void addMultiTermClause(List<BooleanClause> clauses, Query q) {
    // We might have been passed a null query; the term might have been
    // filtered away by the analyzer.
    if (q == null) {
      return;
    }
    clauses.add(newBooleanClause(q, operator == AND_OPERATOR ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD));
  }

  protected Query newFieldQuery(Analyzer analyzer, String field, String queryText,
                                boolean quoted, boolean fieldAutoGenPhraseQueries, boolean fieldEnableGraphQueries,
                                SynonymQueryStyle synonymQueryStyle)
      throws SyntaxError {
//    System.out.println(queryText);
    BooleanClause.Occur occur = operator == Operator.AND ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD;
    setEnableGraphQueries(fieldEnableGraphQueries);
    setSynonymQueryStyle(synonymQueryStyle);
    Query query = createFieldQuery(analyzer, occur, field, queryText,
        quoted || fieldAutoGenPhraseQueries || autoGeneratePhraseQueries, 0);
    setEnableGraphQueries(true); // reset back to default
    setSynonymQueryStyle(AS_SAME_TERM);
    return query;
  }

  /**
   * Builds a new BooleanClause instance
   *
   * @param q     sub query
   * @param occur how this clause should occur when matching documents
   * @return new BooleanClause instance
   */
  private BooleanClause newBooleanClause(Query q, BooleanClause.Occur occur) {
    return new BooleanClause(q, occur);
  }

  /**
   * Builds a new PrefixQuery instance
   *
   * @param prefix Prefix term
   * @return new PrefixQuery instance
   */
  private Query newPrefixQuery(Term prefix) {
    SchemaField sf = schema.getField(prefix.field());
    return sf.getType().getPrefixQuery(parser, sf, prefix.text());
  }

  /**
   * Builds a new RegexpQuery instance
   *
   * @param regexp Regexp term
   * @return new RegexpQuery instance
   */
  private Query newRegexpQuery(Term regexp) {
    RegexpQuery query = new RegexpQuery(regexp);
    SchemaField sf = schema.getField(regexp.field());
    query.setRewriteMethod(sf.getType().getRewriteMethod(parser, sf));
    return query;
  }

  @Override
  protected Query newSynonymQuery(TermAndBoost[] terms) {
    switch (synonymQueryStyle) {
      case PICK_BEST:
        List<Query> currPosnClauses = new ArrayList<>(terms.length);
        for (TermAndBoost term : terms) {
          currPosnClauses.add(newTermQuery(term.term, term.boost));
        }
        return new DisjunctionMaxQuery(currPosnClauses, 0.0f);
      case AS_DISTINCT_TERMS:
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (TermAndBoost term : terms) {
          builder.add(newTermQuery(term.term, term.boost), BooleanClause.Occur.SHOULD);
        }
        return builder.build();
      case AS_SAME_TERM:
        return super.newSynonymQuery(terms);
      default:
        throw new AssertionError("unrecognized synonymQueryStyle passed when creating newSynonymQuery");
    }
  }


  /**
   * Builds a new MatchAllDocsQuery instance
   *
   * @return new MatchAllDocsQuery instance
   */
  private Query newMatchAllDocsQuery() {
    return new MatchAllDocsQuery();
  }

  /**
   * Builds a new WildcardQuery instance
   *
   * @param t wildcard term
   * @return new WildcardQuery instance
   */
  private WildcardQuery newWildcardQuery(Term t) {
    WildcardQuery query = new WildcardQuery(t);
    SchemaField sf = schema.getField(t.field());
    query.setRewriteMethod(sf.getType().getRewriteMethod(parser, sf));
    return query;
  }

  /**
   * Factory method for generating query, given a set of clauses.
   * By default creates a boolean query composed of clauses passed in.
   * <p>
   * Can be overridden by extending classes, to modify query being
   * returned.
   *
   * @param clauses List that contains {@link BooleanClause} instances
   *                to join.
   * @return Resulting {@link Query} object.
   */
  Query getBooleanQuery(List<BooleanClause> clauses) throws SyntaxError {
    if (clauses.size() == 0) {
      return null; // all clause words were filtered away by the analyzer.
    }

    SchemaField sfield = null;
    List<RawQuery> fieldValues = null;

    boolean onlyRawQueries = true;
    int allRawQueriesTermCount = 0;
    for (BooleanClause clause : clauses) {
      if (clause.getQuery() instanceof RawQuery) {
        allRawQueriesTermCount += ((RawQuery) clause.getQuery()).getTermCount();
      } else {
        onlyRawQueries = false;
      }
    }
    boolean useTermsQuery = (flags & QParser.FLAG_FILTER) != 0 && allRawQueriesTermCount > TERMS_QUERY_THRESHOLD;

    BooleanQuery.Builder booleanBuilder = newBooleanQuery();
    Map<SchemaField, List<RawQuery>> fmap = new HashMap<>();

    for (BooleanClause clause : clauses) {
      Query subq = clause.getQuery();
      if (subq instanceof RawQuery) {
        if (clause.getOccur() != BooleanClause.Occur.SHOULD) {
          // We only collect optional terms for set queries.  Since this isn't optional,
          // convert the raw query to a normal query and handle as usual.
          clause = new BooleanClause(rawToNormal(subq), clause.getOccur());
        } else {
          // Optional raw query.
          RawQuery rawq = (RawQuery) subq;

          // only look up fmap and type info on a field change
          if (sfield != rawq.sfield) {
            sfield = rawq.sfield;
            fieldValues = fmap.get(sfield);
            // If this field isn't indexed, or if it is indexed and we want to use TermsQuery, then collect this value.
            // We are currently relying on things like PointField not being marked as indexed in order to bypass
            // the "useTermQuery" check.
            if ((fieldValues == null && useTermsQuery) || !sfield.indexed()) {
              fieldValues = new ArrayList<>(2);
              fmap.put(sfield, fieldValues);
            }
          }

          if (fieldValues != null) {
            fieldValues.add(rawq);
            continue;
          }

          clause = new BooleanClause(rawToNormal(subq), clause.getOccur());
        }
      }

      booleanBuilder.add(clause);
    }


    for (Map.Entry<SchemaField, List<RawQuery>> entry : fmap.entrySet()) {
      sfield = entry.getKey();
      fieldValues = entry.getValue();
      FieldType ft = sfield.getType();

      int termCount = fieldValues.stream().mapToInt(RawQuery::getTermCount).sum();
      if ((sfield.indexed() && termCount < TERMS_QUERY_THRESHOLD) || termCount == 1) {
        // use boolean query instead
        for (RawQuery rawq : fieldValues) {
          Query subq;
          if (ft.isTokenized() && sfield.indexed()) {
            boolean fieldAutoGenPhraseQueries = ft instanceof TextField && ((TextField) ft).getAutoGeneratePhraseQueries();
            boolean fieldEnableGraphQueries = ft instanceof TextField && ((TextField) ft).getEnableGraphQueries();

            SynonymQueryStyle synonymQueryStyle = AS_SAME_TERM;
            if (ft instanceof TextField) {
              synonymQueryStyle = ((TextField) (ft)).getSynonymQueryStyle();
            }

            subq = newFieldQuery(getAnalyzer(), sfield.getName(), rawq.getJoinedExternalVal(),
                false, fieldAutoGenPhraseQueries, fieldEnableGraphQueries, synonymQueryStyle);
            booleanBuilder.add(subq, BooleanClause.Occur.SHOULD);
          } else {
            for (String externalVal : rawq.getExternalVals()) {
              subq = ft.getFieldQuery(this.parser, sfield, externalVal);
              booleanBuilder.add(subq, BooleanClause.Occur.SHOULD);
            }
          }
        }
      } else {
        List<String> externalVals
            = fieldValues.stream().flatMap(rawq -> rawq.getExternalVals().stream()).collect(Collectors.toList());
        Query subq = ft.getSetQuery(this.parser, sfield, externalVals);
        if (onlyRawQueries && termCount == allRawQueriesTermCount)
          return subq; // if this is everything, don't wrap in a boolean query
        booleanBuilder.add(subq, BooleanClause.Occur.SHOULD);
      }
    }

    BooleanQuery bq = QueryUtils.build(booleanBuilder, parser);
    if (bq.clauses().size() == 1) { // Unwrap single SHOULD query
      BooleanClause clause = bq.clauses().iterator().next();
      if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
        return clause.getQuery();
      }
    }
    return bq;
  }


  // called from parser
  Query handleBareTokenQuery(String qfield, Token term, boolean prefix, boolean wildcard, boolean regexp, QueryParser qp, int pdepth) throws SyntaxError {
    Query q;
    SpanContext spanContext = (SpanContext) term.getValue();
    String image = term.image;
    image = parenMagic(qp, pdepth, image);
    if (wildcard) {
      q = getWildcardQuery(qfield, image);
    } else if (prefix) {
      q = getPrefixQuery(qfield,
          discardEscapeChar(image.substring
              (0, image.length() - 1)), spanContext);
    } else if (regexp) {
      q = getRegexpQuery(qfield, image.substring(1, image.length() - 1));
    } else {
      String termImage = discardEscapeChar(image);
      q = getFieldQuery(qfield, termImage, false, true, spanContext);
    }
    return mtSpanContext(spanContext, q);
  }

  // called from parser

  Query handleQuotedTerm(String qfield, Token term, String suffix) throws SyntaxError {
    String raw = discardEscapeChar(term.image.substring(1, term.image.length()-1));
    SpanContext spanContext = (SpanContext) term.getValue();
    return getFieldQuery(qfield + suffix, raw, true, spanContext);
  }

  abstract void eatRp() throws ParseException, SyntaxError;

  /**
   * Returns a String where the escape char has been
   * removed, or kept only once if there was a double escape.
   * <p>
   * Supports escaped unicode characters, e. g. translates
   * <code>\\u0041</code> to <code>A</code>.
   */
  String discardEscapeChar(String input) throws SyntaxError {
    int start = input.indexOf('\\');
    if (start < 0) return input;

    // Create char array to hold unescaped char sequence
    char[] output = new char[input.length()];
    input.getChars(0, start, output, 0);

    // The length of the output can be less than the input
    // due to discarded escape chars. This variable holds
    // the actual length of the output
    int length = start;

    // We remember whether the last processed character was
    // an escape character
    boolean lastCharWasEscapeChar = true;

    // The multiplier the current unicode digit must be multiplied with.
    // E. g. the first digit must be multiplied with 16^3, the second with 16^2...
    int codePointMultiplier = 0;

    // Used to calculate the codepoint of the escaped unicode character
    int codePoint = 0;

    // start after the first escape char
    for (int i = start + 1; i < input.length(); i++) {
      char curChar = input.charAt(i);
      if (codePointMultiplier > 0) {
        codePoint += hexToInt(curChar) * codePointMultiplier;
        codePointMultiplier >>>= 4;
        if (codePointMultiplier == 0) {
          output[length++] = (char) codePoint;
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
      throw new SyntaxError("Truncated unicode escape sequence.");
    }

    if (lastCharWasEscapeChar) {
      throw new SyntaxError("Term can not end with escape character.");
    }

    return new String(output, 0, length);
  }

  /**
   * Returns the numeric value of the hexadecimal character
   */
  private static int hexToInt(char c) throws SyntaxError {
    if ('0' <= c && c <= '9') {
      return c - '0';
    } else if ('a' <= c && c <= 'f') {
      return c - 'a' + 10;
    } else if ('A' <= c && c <= 'F') {
      return c - 'A' + 10;
    } else {
      throw new SyntaxError("Non-hex character in Unicode escape sequence: " + c);
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


  private ReversedWildcardFilterFactory getReversedWildcardFilterFactory(FieldType fieldType) {
    if (leadingWildcards == null) leadingWildcards = new HashMap<>();
    ReversedWildcardFilterFactory fac = leadingWildcards.get(fieldType);
    if (fac != null || leadingWildcards.containsKey(fieldType)) {
      return fac;
    }

    Analyzer a = fieldType.getIndexAnalyzer();
    if (a instanceof TokenizerChain) {
      // examine the indexing analysis chain if it supports leading wildcards
      TokenizerChain tc = (TokenizerChain) a;
      TokenFilterFactory[] factories = tc.getTokenFilterFactories();
      for (TokenFilterFactory factory : factories) {
        if (factory instanceof ReversedWildcardFilterFactory) {
          fac = (ReversedWildcardFilterFactory) factory;
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
              "no field name specified in query and no default specified via 'df' param");
    }
  }

  private String analyzeIfMultitermTermText(String field, String part, FieldType fieldType) {
    if (part == null || !(fieldType instanceof TextField) || ((TextField) fieldType).getMultiTermAnalyzer() == null)
      return part;
    SchemaField sf = schema.getFieldOrNull((field));
    if (sf == null) return part;
    return TextField.analyzeMultiTerm(field, part, ((TextField) fieldType).getMultiTermAnalyzer()).utf8ToString();
  }

  // Create a "normal" query from a RawQuery (or just return the current query if it's not raw)
  Query rawToNormal(Query q) {
    Query normal = q;
    if (q instanceof RawQuery) {
      RawQuery rawq = (RawQuery) q;
      if (rawq.sfield.getType().isTokenized()) {
        normal = rawq.sfield.getType().getFieldQuery(parser, rawq.sfield, rawq.getJoinedExternalVal());
      } else {
        FieldType ft = rawq.sfield.getType();
        if (rawq.getTermCount() == 1) {
          normal = ft.getFieldQuery(this.parser, rawq.sfield, rawq.getExternalVals().get(0));
        } else {
          BooleanQuery.Builder booleanBuilder = newBooleanQuery();
          for (String externalVal : rawq.getExternalVals()) {
            Query subq = ft.getFieldQuery(this.parser, rawq.sfield, externalVal);
            booleanBuilder.add(subq, BooleanClause.Occur.SHOULD);
          }
          normal = QueryUtils.build(booleanBuilder, parser);
        }
      }
    }
    return normal;
  }

  protected Query getFieldQuery(String field, String queryText, boolean quoted, SpanContext sc) throws SyntaxError {
    return getFieldQuery(field, queryText, quoted, false, sc);
  }

  // private use for getFieldQuery
  private String lastFieldName;
  private SchemaField lastField;

  // if raw==true, then it's possible for this method to return a RawQuery that will need to be transformed
  // further before using.
  Query getFieldQuery(String field, String queryText, boolean quoted, boolean raw, SpanContext sc) throws SyntaxError {
    checkNullField(field);

    SchemaField sf;
    if (field.equals(lastFieldName)) {
      // only look up the SchemaField on a field change... this helps with memory allocation of dynamic fields
      // and large queries like foo_i:(1 2 3 4 5 6 7 8 9 10) when we are passed "foo_i" each time.
      sf = lastField;
    } else {
      lastFieldName = field;
      sf = lastField = schema.getFieldOrNull(field);
    }

    if (sf != null) {
      FieldType ft = sf.getType();
      // delegate to type for everything except tokenized fields
      if (ft.isTokenized() && sf.indexed()) {
        Query q = fieldQFromTermText(field, ft, queryText, quoted);
        if (sc != null) {
          return convertToSpans(sc, q, operator);
        }
        return q;
      } else {
        if (raw) {
          return new RawQuery(sf, queryText, sc);
        } else {
          if (sc == null) {
            return ft.getFieldQuery(parser, sf, queryText);
          } else {
            BytesRefBuilder br = new BytesRefBuilder();
            ft.readableToIndexed(queryText, br);
            return new SpanTermQuery(new Term(sf.getName(), br));
          }
        }
      }
    }

    // default to a normal field query
    return newFieldQuery(getAnalyzer(), field, queryText, quoted, false, true, AS_SAME_TERM);
  }

  SpanQuery convertToSpans(SpanContext spanContext, Query q, Operator localOp) {
    boolean inOrder = spanContext.type == W;
    if (q == null) {
      return null;
    }
    if (q instanceof SpanQuery) {
      return (SpanQuery) q;
    } else if (q instanceof TermQuery) {
      return convertTermToSpanTerm((TermQuery) q);
    } else if (q instanceof BooleanQuery) {
      BooleanQuery bq = (BooleanQuery) q;
      List<SpanQuery> sqs = new ArrayList<>();
      for (BooleanClause booleanClause : bq) {
        Operator subOp = booleanClause.getOccur() == BooleanClause.Occur.SHOULD ? Operator.OR : Operator.AND;
        sqs.add(convertToSpans(spanContext, booleanClause.getQuery(), subOp));
      }
      if (localOp == Operator.OR) {
        return new SpanOrQuery(sqs.toArray(new SpanQuery[]{}));
      }
      return new SpanNearQuery(sqs.toArray(new SpanQuery[]{}), spanContext.distOpNum, inOrder);
    } else if (q instanceof PhraseQuery) {
      PhraseQuery pq = (PhraseQuery) q;
      return new SpanNearQuery(Arrays.stream(pq.getTerms())
          .map(SpanTermQuery::new)
          .collect(Collectors.toList())
          .toArray(new SpanQuery[]{}), 0, true);
    } else if (q instanceof SynonymQuery) {
      SynonymQuery sq = (SynonymQuery) q;
      List<Term> terms = sq.getTerms();
      List<SpanQuery> sqs = new ArrayList<>();
      for (Term term : terms) {
        sqs.add(new SpanTermQuery(term));
      }
      return new SpanOrQuery(sqs.toArray(new SpanQuery[]{}));
    } else if (q instanceof MultiTermQuery) {
      MultiTermQuery mtq = (MultiTermQuery) q;
      return new SpanMultiTermQueryWrapper<>(mtq);
    }
    throw new IllegalStateException("Only expect to convert term, boolean and phrase queries to span queries, got a " + q.getClass() + " query!");
  }

  private SpanTermQuery convertTermToSpanTerm(TermQuery q) {
    Term t = q.getTerm();
    TermStates ts = q.getTermStates();
    return new SpanTermQuery(t, ts);
  }

  // Assumption: quoted is always false
  Query getFieldQuery(String field, List<String> queryTerms, SpanContext sc, QueryParser qp, int pdepth) throws SyntaxError {
    checkNullField(field);

    String lastTerm = queryTerms.get(queryTerms.size() - 1);
    String fixup = parenMagic(qp, pdepth, lastTerm);
    queryTerms.set(queryTerms.size() - 1, fixup);

    SchemaField sf;
    if (field.equals(lastFieldName)) {
      // only look up the SchemaField on a field change... this helps with memory allocation of dynamic fields
      // and large queries like foo_i:(1 2 3 4 5 6 7 8 9 10) when we are passed "foo_i" each time.
      sf = lastField;
    } else {
      lastFieldName = field;
      sf = lastField = schema.getFieldOrNull(field);
    }

    if (sf != null) {
      FieldType ft = sf.getType();
      // delegate to type for everything except tokenized fields
      if (ft.isTokenized() && sf.indexed()) {
        BooleanQuery.Builder booleanBuilder = newBooleanQuery();
        final BooleanClause.Occur occur
            = operator == AND_OPERATOR ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD;
        boolean atLeastOneClause = false;
        for (String subq : queryTerms) {
          Query query = fieldQFromTermText(field, ft, subq, false);
          if (query != null) {
            atLeastOneClause = true;
            booleanBuilder.add(query, occur);
          }
        }
        if (atLeastOneClause) {
          return booleanBuilder.build();
        } else {
          // all terms were stop words or otherwise eliminated.
          return null;
        }
      } else {
        return new RawQuery(sf, queryTerms, sc);
      }
    }

    // default to a normal field query
    String queryText = queryTerms.size() == 1 ? queryTerms.get(0) : String.join(" ", queryTerms);
    return newFieldQuery(getAnalyzer(), field, queryText, false, false, true, AS_SAME_TERM);
  }

  private Query fieldQFromTermText(String field, FieldType ft, String queryText, boolean quoted) throws SyntaxError {
    boolean fieldAutoGenPhraseQueries = ft instanceof TextField && ((TextField) ft).getAutoGeneratePhraseQueries();
    boolean fieldEnableGraphQueries = ft instanceof TextField && ((TextField) ft).getEnableGraphQueries();
    SynonymQueryStyle synonymQueryStyle = AS_SAME_TERM;
    if (ft instanceof TextField) {
      synonymQueryStyle = ((TextField) (ft)).getSynonymQueryStyle();
    }
    return newFieldQuery
        (getAnalyzer(), field, queryText, quoted, fieldAutoGenPhraseQueries, fieldEnableGraphQueries, synonymQueryStyle);
  }

  private String parenMagic(QueryParser qp, int pDepth, String lastTerm) throws SyntaxError {
    StringBuilder builder = new StringBuilder(lastTerm.length() + 5);
    builder.append(lastTerm);

    // magically consume extra ) that should be part of the last term if it follows

    int parenCount = countFollowingParens(qp);

    // consume parens up to the point where we need them to complete the open syntactical parenthesis.
    // we want the lesser of the unclosed parens in the token, or the excess parens after the token
    int parensToConsume = parenCount - pDepth;

    try {
      while (parensToConsume > 0 && qp.getToken(1).kind == RPAREN) {
        eatRp();
        builder.append(")");
        parensToConsume--;
      }
    } catch (ParseException e) {
      // docs mention ParseError, but such a class doesn't seem to exist...
      // https://www.cs.purdue.edu/homes/hosking/javacc/doc/apiroutines.html
    }

    return builder.toString();
  }

  private int countFollowingParens(QueryParser parser) {
    int parenCount = 0;
    while (parser.getToken(1 + parenCount).kind == RPAREN) {
      parenCount++;
    }
    return parenCount;
  }

  private boolean isRangeShouldBeProtectedFromReverse(String field, String part1) {
    checkNullField(field);
    SchemaField sf = schema.getField(field);

    return part1 == null && getReversedWildcardFilterFactory(sf.getType()) != null;
  }

  // called from parser
  Query getRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) {
    boolean reverse = isRangeShouldBeProtectedFromReverse(field, part1);
    return getRangeQueryImpl(field, reverse ? REVERSE_WILDCARD_LOWER_BOUND : part1, part2, startInclusive || reverse, endInclusive);
  }

  private Query getRangeQueryImpl(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) {
    checkNullField(field);
    SchemaField sf = schema.getField(field);
    return sf.getType().getRangeQuery(parser, sf, part1, part2, startInclusive, endInclusive);
  }

  // called from parser
  private Query getPrefixQuery(String field, String termStr, SpanContext spanContext) throws SyntaxError {
    checkNullField(field);
    if (termStr.length() < MIN_PREFIX_TERM_LENGTH) {
      // disallow prefix queries having insufficient prefix chars
      throw new SyntaxError("Prefix query term must at be least " + MIN_PREFIX_TERM_LENGTH + " characters.");
    }
    termStr = analyzeIfMultitermTermText(field, termStr, schema.getFieldType(field));

    // Solr has always used constant scoring for prefix queries.  This should return constant scoring by default.
    return mtSpanContext(spanContext, newPrefixQuery(new Term(field, termStr)));
  }

  // called from parser
  private Query getWildcardQuery(String field, String termStr) throws SyntaxError {
    checkNullField(field);
    // *:* -> MatchAllDocsQuery
    if ("*".equals(termStr)) {
      if ("*".equals(field) || getExplicitField() == null) {
        return newMatchAllDocsQuery();
      } else {
        // Solr has always used constant scoring for wildcard queries.  This should return constant scoring by default.
        return newWildcardQuery(new Term(field, termStr));
      }
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
        automaton = Operations.concatenate(automaton, Automata.makeChar(factory.getMarkerChar()));
        automaton = Operations.reverse(automaton);
      } else {
        // reverse wildcardfilter is active: remove false positives
        // fsa representing false positives (markerChar*)
        Automaton falsePositives = Operations.concatenate(
            Automata.makeChar(factory.getMarkerChar()),
            Automata.makeAnyString());
        // subtract these away
        automaton = Operations.minus(automaton, falsePositives, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
      }
      return new AutomatonQuery(term, automaton) {
        // override toString so it's completely transparent
        @Override
        public String toString(String field) {
          StringBuilder buffer = new StringBuilder();
          if (!getField().equals(field)) {
            buffer.append(getField());
            buffer.append(":");
          }
          buffer.append(term.text());
          return buffer.toString();
        }
      };
    } else {

      throw new SyntaxError("Wildcard queries are not allowed for field \"" + field + "\"");
    }

  }

  private Query mtSpanContext(SpanContext spanContext, Query result) {
    if (result instanceof MultiTermQuery) {
      return spanContext != null ? new SpanMultiTermQueryWrapper<>((MultiTermQuery) result) : result;
    } else {
      return result;
    }
  }

  // called from parser
  private Query getRegexpQuery(String field, String termStr) {
    termStr = analyzeIfMultitermTermText(field, termStr, schema.getFieldType(field));
    return newRegexpQuery(new Term(field, termStr));
  }


  void assertNotInsideSpan(Token tk) throws ParseException {
    if (tk != null) {
      SpanContext value = (SpanContext) tk.getValue();
      if (value != null) {
        int type = value.type;
        if (type != DEFAULT) {
          throw new ParseException("W/ and N/ operators may not contain more than one field");
        }
      }
    }
  }

}
