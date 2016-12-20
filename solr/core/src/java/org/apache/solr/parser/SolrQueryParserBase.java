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
package org.apache.solr.parser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.solr.analysis.ReversedWildcardFilterFactory;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.SolrException;
import org.apache.solr.parser.QueryParser.Operator;
import org.apache.solr.query.FilterQuery;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.search.SyntaxError;

/** This class is overridden by QueryParser in QueryParser.jj
 * and acts to separate the majority of the Java code from the .jj grammar file. 
 */
public abstract class SolrQueryParserBase extends QueryBuilder {

  public static final int TERMS_QUERY_THRESHOLD = 16;   // @lucene.internal Set to a low value temporarily for better test coverage

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

  /** The default operator that parser uses to combine query terms */
  Operator operator = OR_OPERATOR;

  MultiTermQuery.RewriteMethod multiTermRewriteMethod = MultiTermQuery.CONSTANT_SCORE_REWRITE;
  boolean allowLeadingWildcard = true;

  String defaultField;
  int phraseSlop = 0;     // default slop for phrase queries
  float fuzzyMinSim = FuzzyQuery.defaultMinSimilarity;
  int fuzzyPrefixLength = FuzzyQuery.defaultPrefixLength;

  boolean autoGeneratePhraseQueries = false;
  int flags;

  protected IndexSchema schema;
  protected QParser parser;

  // implementation detail - caching ReversedWildcardFilterFactory based on type
  private Map<FieldType, ReversedWildcardFilterFactory> leadingWildcards;

  /**
   * Identifies the list of all known "magic fields" that trigger
   * special parsing behavior
   */
  public static enum MagicFieldName {
    VAL("_val_", "func"), QUERY("_query_", null);

    public final String field;
    public final String subParser;
    MagicFieldName(final String field, final String subParser) {
      this.field = field;
      this.subParser = subParser;
    }
    @Override
    public String toString() {
      return field;
    }
    private final static Map<String,MagicFieldName> lookup
        = new HashMap<>();
    static {
      for(MagicFieldName s : EnumSet.allOf(MagicFieldName.class))
        lookup.put(s.toString(), s);
    }
    public static MagicFieldName get(final String field) {
      return lookup.get(field);
    }
  }

  // internal: A simple raw fielded query
  public static class RawQuery extends Query {
    final SchemaField sfield;
    final String externalVal;

    public RawQuery(SchemaField sfield, String externalVal) {
      this.sfield = sfield;
      this.externalVal = externalVal;
    }

    @Override
    public String toString(String field) {
      return "RAW(" + field + "," + externalVal + ")";
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
  protected SolrQueryParserBase() {
    super(null);
  }
  // the generated parser will create these in QueryParser
  public abstract void ReInit(CharStream stream);
  public abstract Query TopLevelQuery(String field) throws ParseException, SyntaxError;


  public void init(Version matchVersion, String defaultField, QParser parser) {
    this.schema = parser.getReq().getSchema();
    this.parser = parser;
    this.flags = parser.getFlags();
    this.defaultField = defaultField;
    setAnalyzer(schema.getQueryAnalyzer());
  }

  // Turn on the "filter" bit and return the previous flags for the caller to save
  int startFilter() {
    int oldFlags = flags;
    flags |= QParser.FLAG_FILTER;
    return oldFlags;
  }

  void restoreFlags(int flagsToRestore) {
    flags = flagsToRestore;
  }

    /** Parses a query string, returning a {@link org.apache.lucene.search.Query}.
    *  @param query  the query string to be parsed.
    */
  public Query parse(String query) throws SyntaxError {
    ReInit(new FastCharStream(new StringReader(query)));
    try {
      // TopLevelQuery is a Query followed by the end-of-input (EOF)
      Query res = TopLevelQuery(null);  // pass null so we can tell later if an explicit field was provided or not
      return res!=null ? res : newBooleanQuery().build();
    }
    catch (ParseException | TokenMgrError tme) {
      throw new SyntaxError("Cannot parse '" +query+ "': " + tme.getMessage(), tme);
    } catch (BooleanQuery.TooManyClauses tmc) {
      throw new SyntaxError("Cannot parse '" +query+ "': too many boolean clauses", tmc);
    }
  }


  /**
   * @return Returns the default field.
   */
  public String getDefaultField() {
    return this.defaultField;
  }

  protected String explicitField;
  /** Handles the default field if null is passed */
  public String getField(String fieldName) {
    explicitField = fieldName;
    return fieldName != null ? fieldName : this.defaultField;
  }

  /** For a fielded query, returns the actual field specified (i.e. null if default is being used)
   * myfield:A or myfield:(A B C) will both return "myfield"
   */
  public String getExplicitField() {
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
   * Get the minimal similarity for fuzzy queries.
   */
  public float getFuzzyMinSim() {
      return fuzzyMinSim;
  }

  /**
   * Set the minimum similarity for fuzzy queries.
   * Default is 2f.
   */
  public void setFuzzyMinSim(float fuzzyMinSim) {
      this.fuzzyMinSim = fuzzyMinSim;
  }

   /**
   * Get the prefix length for fuzzy queries.
   * @return Returns the fuzzyPrefixLength.
   */
  public int getFuzzyPrefixLength() {
    return fuzzyPrefixLength;
  }

  /**
   * Set the prefix length for fuzzy queries. Default is 0.
   * @param fuzzyPrefixLength The fuzzyPrefixLength to set.
   */
  public void setFuzzyPrefixLength(int fuzzyPrefixLength) {
    this.fuzzyPrefixLength = fuzzyPrefixLength;
  }

  /**
   * Sets the default slop for phrases.  If zero, then exact phrase matches
   * are required.  Default value is zero.
   */
  public void setPhraseSlop(int phraseSlop) {
    this.phraseSlop = phraseSlop;
  }

  /**
   * Gets the default slop for phrases.
   */
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
  public void setAllowLeadingWildcard(boolean allowLeadingWildcard) {
    this.allowLeadingWildcard = allowLeadingWildcard;
  }

  /**
   * @see #setAllowLeadingWildcard(boolean)
   */
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
   * By default QueryParser uses {@link org.apache.lucene.search.MultiTermQuery#CONSTANT_SCORE_REWRITE}
   * when creating a PrefixQuery, WildcardQuery or RangeQuery. This implementation is generally preferable because it
   * a) Runs faster b) Does not have the scarcity of terms unduly influence score
   * c) avoids any "TooManyBooleanClauses" exception.
   * However, if your application really needs to use the
   * old-fashioned BooleanQuery expansion rewriting and the above
   * points are not relevant then use this to change
   * the rewrite method.
   */
  public void setMultiTermRewriteMethod(MultiTermQuery.RewriteMethod method) {
    multiTermRewriteMethod = method;
  }


  /**
   * @see #setMultiTermRewriteMethod
   */
  public MultiTermQuery.RewriteMethod getMultiTermRewriteMethod() {
    return multiTermRewriteMethod;
  }


  protected void addClause(List<BooleanClause> clauses, int conj, int mods, Query q) {
    boolean required, prohibited;

    // If this term is introduced by AND, make the preceding term required,
    // unless it's already prohibited
    if (clauses.size() > 0 && conj == CONJ_AND) {
      BooleanClause c = clauses.get(clauses.size()-1);
      if (!c.isProhibited())
        clauses.set(clauses.size() - 1, new BooleanClause(c.getQuery(), BooleanClause.Occur.MUST));
    }

    if (clauses.size() > 0 && operator == AND_OPERATOR && conj == CONJ_OR) {
      // If this term is introduced by OR, make the preceding term optional,
      // unless it's prohibited (that means we leave -a OR b but +a OR b-->a OR b)
      // notice if the input is a OR b, first term is parsed as required; without
      // this modification a OR b would parsed as +a OR b
      BooleanClause c = clauses.get(clauses.size()-1);
      if (!c.isProhibited())
        clauses.set(clauses.size() - 1, new BooleanClause(c.getQuery(), BooleanClause.Occur.SHOULD));
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



  protected Query newFieldQuery(Analyzer analyzer, String field, String queryText, boolean quoted)  throws SyntaxError {
    BooleanClause.Occur occur = operator == Operator.AND ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD;
    return createFieldQuery(analyzer, occur, field, queryText, quoted || autoGeneratePhraseQueries, phraseSlop);
  }



  /**
   * Base implementation delegates to {@link #getFieldQuery(String,String,boolean)}.
   * This method may be overridden, for example, to return
   * a SpanNearQuery instead of a PhraseQuery.
   *
   */
  protected Query getFieldQuery(String field, String queryText, int slop)
        throws SyntaxError {
    Query query = getFieldQuery(field, queryText, true, false);

    // only set slop of the phrase query was a result of this parser
    // and not a sub-parser.
    if (subQParser == null) {
      if (query instanceof PhraseQuery) {
        PhraseQuery pq = (PhraseQuery) query;
        Term[] terms = pq.getTerms();
        int[] positions = pq.getPositions();
        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        for (int i = 0; i < terms.length; ++i) {
          builder.add(terms[i], positions[i]);
        }
        builder.setSlop(slop);
        query = builder.build();
      } else if (query instanceof MultiPhraseQuery) {
        MultiPhraseQuery mpq = (MultiPhraseQuery)query;
      
        if (slop != mpq.getSlop()) {
          query = new MultiPhraseQuery.Builder(mpq).setSlop(slop).build();
        }
      }
    }

    return query;
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
    SchemaField sf = schema.getField(prefix.field());
    return sf.getType().getPrefixQuery(parser, sf, prefix.text());
  }

  /**
   * Builds a new RegexpQuery instance
   * @param regexp Regexp term
   * @return new RegexpQuery instance
   */
  protected Query newRegexpQuery(Term regexp) {
    RegexpQuery query = new RegexpQuery(regexp);
    SchemaField sf = schema.getField(regexp.field());
    query.setRewriteMethod(sf.getType().getRewriteMethod(parser, sf));
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
    WildcardQuery query = new WildcardQuery(t);
    SchemaField sf = schema.getField(t.field());
    query.setRewriteMethod(sf.getType().getRewriteMethod(parser, sf));
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
   */
  protected Query getBooleanQuery(List<BooleanClause> clauses) throws SyntaxError
  {
    if (clauses.size()==0) {
      return null; // all clause words were filtered away by the analyzer.
    }

    SchemaField sfield = null;
    List<String> fieldValues = null;


    boolean useTermsQuery = (flags & QParser.FLAG_FILTER)!=0 && clauses.size() > TERMS_QUERY_THRESHOLD;
    int clausesAdded = 0;

    BooleanQuery.Builder booleanBuilder = newBooleanQuery();
    Map<SchemaField, List<String>> fmap = new HashMap<>();

    for (BooleanClause clause : clauses) {
      Query subq = clause.getQuery();
      if (subq instanceof RawQuery) {
        if (clause.getOccur() != BooleanClause.Occur.SHOULD) {
          // We only collect optional terms for set queries.  Since this isn't optional,
          // convert the raw query to a normal query and handle as usual.
          clause = new BooleanClause( rawToNormal(subq), clause.getOccur() );
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
            if (fieldValues == null && useTermsQuery || !sfield.indexed()) {
              fieldValues = new ArrayList<>(2);
              fmap.put(sfield, fieldValues);
            }
          }

          if (fieldValues != null) {
            fieldValues.add(rawq.externalVal);
            continue;
          }

          clause = new BooleanClause( rawToNormal(subq), clause.getOccur() );
        }
      }

      clausesAdded++;
      booleanBuilder.add(clause);
    }


    for (Map.Entry<SchemaField,List<String>> entry : fmap.entrySet()) {
      sfield = entry.getKey();
      fieldValues = entry.getValue();
      FieldType ft = sfield.getType();

      // TODO: pull more of this logic out to FieldType?  We would need to be able to add clauses to our existing booleanBuilder.
      if (sfield.indexed() && fieldValues.size() < TERMS_QUERY_THRESHOLD || fieldValues.size() == 1) {
        // use boolean query instead
        for (String externalVal : fieldValues) {
          Query subq = ft.getFieldQuery(this.parser, sfield, externalVal);
          clausesAdded++;
          booleanBuilder.add(subq, BooleanClause.Occur.SHOULD);
        }
      } else {
        Query subq = ft.getSetQuery(this.parser, sfield, fieldValues);
        if (fieldValues.size() == clauses.size()) return subq; // if this is everything, don't wrap in a boolean query
        clausesAdded++;
        booleanBuilder.add(subq, BooleanClause.Occur.SHOULD);
      }
    }

    return booleanBuilder.build();
  }


   // called from parser
  Query handleBareTokenQuery(String qfield, Token term, Token fuzzySlop, boolean prefix, boolean wildcard, boolean fuzzy, boolean regexp) throws SyntaxError {
    Query q;

    if (wildcard) {
      q = getWildcardQuery(qfield, term.image);
    } else if (prefix) {
      q = getPrefixQuery(qfield,
          discardEscapeChar(term.image.substring
              (0, term.image.length()-1)));
    } else if (regexp) {
      q = getRegexpQuery(qfield, term.image.substring(1, term.image.length()-1));
    } else if (fuzzy) {
      float fms = fuzzyMinSim;
      try {
        fms = Float.valueOf(fuzzySlop.image.substring(1)).floatValue();
      } catch (Exception ignored) { }
      if(fms < 0.0f){
        throw new SyntaxError("Minimum similarity for a FuzzyQuery has to be between 0.0f and 1.0f !");
      } else if (fms >= 1.0f && fms != (int) fms) {
        throw new SyntaxError("Fractional edit distances are not allowed!");
      }
      String termImage=discardEscapeChar(term.image);
      q = getFuzzyQuery(qfield, termImage, fms);
    } else {
      String termImage=discardEscapeChar(term.image);
      q = getFieldQuery(qfield, termImage, false, true);
    }
    return q;
  }

  // called from parser
  Query handleQuotedTerm(String qfield, Token term, Token fuzzySlop) throws SyntaxError {
    int s = phraseSlop;  // default
    if (fuzzySlop != null) {
      try {
        s = Float.valueOf(fuzzySlop.image.substring(1)).intValue();
      }
      catch (Exception ignored) { }
    }

    String raw = discardEscapeChar(term.image.substring(1, term.image.length()-1));
    return getFieldQuery(qfield, raw, s);
  }



  // Called from parser
  // Raw queries are transformed to normal queries before wrapping in a BoostQuery
  Query handleBoost(Query q, Token boost) {
    // q==null check is to avoid boosting null queries, such as those caused by stop words
    if (boost == null || boost.image.length()==0 || q == null) {
      return q;
    }
    if (boost.image.charAt(0) == '=') {
      // syntax looks like foo:x^=3
      float val = Float.parseFloat(boost.image.substring(1));
      Query newQ = q;
      if (q instanceof ConstantScoreQuery || q instanceof SolrConstantScoreQuery) {
        // skip
      } else {
        newQ = new ConstantScoreQuery( rawToNormal(q) );
      }
      return new BoostQuery(newQ, val);
    }

    float boostVal = Float.parseFloat(boost.image);

    return new BoostQuery( rawToNormal(q), boostVal);
  }



  /**
   * Returns a String where the escape char has been
   * removed, or kept only once if there was a double escape.
   *
   * Supports escaped unicode characters, e. g. translates
   * <code>\\u0041</code> to <code>A</code>.
   *
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
    for (int i = start+1; i < input.length(); i++) {
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
      throw new SyntaxError("Truncated unicode escape sequence.");
    }

    if (lastCharWasEscapeChar) {
      throw new SyntaxError("Term can not end with escape character.");
    }

    return new String(output, 0, length);
  }

  /** Returns the numeric value of the hexadecimal character */
  static final int hexToInt(char c) throws SyntaxError {
    if ('0' <= c && c <= '9') {
      return c - '0';
    } else if ('a' <= c && c <= 'f'){
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


  protected ReversedWildcardFilterFactory getReversedWildcardFilterFactory(FieldType fieldType) {
    if (leadingWildcards == null) leadingWildcards = new HashMap<>();
    ReversedWildcardFilterFactory fac = leadingWildcards.get(fieldType);
    if (fac != null || leadingWildcards.containsKey(fieldType)) {
      return fac;
    }

    Analyzer a = fieldType.getIndexAnalyzer();
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
              "no field name specified in query and no default specified via 'df' param");
    }
  }

  protected String analyzeIfMultitermTermText(String field, String part, FieldType fieldType) {

    if (part == null || ! (fieldType instanceof TextField) || ((TextField)fieldType).getMultiTermAnalyzer() == null) return part;

    SchemaField sf = schema.getFieldOrNull((field));
    if (sf == null || ! (fieldType instanceof TextField)) return part;
    String out = TextField.analyzeMultiTerm(field, part, ((TextField)fieldType).getMultiTermAnalyzer()).utf8ToString();
    return out;
  }


  private QParser subQParser = null;

  // Create a "normal" query from a RawQuery (or just return the current query if it's not raw)
  Query rawToNormal(Query q) {
    if (!(q instanceof RawQuery)) return q;
    RawQuery rq = (RawQuery)q;
    return rq.sfield.getType().getFieldQuery(parser, rq.sfield, rq.externalVal);
  }

  protected Query getFieldQuery(String field, String queryText, boolean quoted) throws SyntaxError {
    return getFieldQuery(field, queryText, quoted, false);
  }

  // private use for getFieldQuery
  private String lastFieldName;
  private SchemaField lastField;

  // if raw==true, then it's possible for this method to return a RawQuery that will need to be transformed
  // further before using.
  protected Query getFieldQuery(String field, String queryText, boolean quoted, boolean raw) throws SyntaxError {
    checkNullField(field);

    SchemaField sf;
    if (field.equals(lastFieldName)) {
      // only look up the SchemaField on a field change... this helps with memory allocation of dynamic fields
      // and large queries like foo_i:(1 2 3 4 5 6 7 8 9 10) when we are passed "foo_i" each time.
      sf = lastField;
    } else {
      // intercept magic field name of "_" to use as a hook for our
      // own functions.
      if (field.charAt(0) == '_' && parser != null) {
        MagicFieldName magic = MagicFieldName.get(field);
        if (null != magic) {
          subQParser = parser.subQuery(queryText, magic.subParser);
          return subQParser.getQuery();
        }
      }

      lastFieldName = field;
      sf = lastField = schema.getFieldOrNull(field);
    }

    if (sf != null) {
      FieldType ft = sf.getType();
      // delegate to type for everything except tokenized fields
      if (ft.isTokenized() && sf.indexed()) {
        return newFieldQuery(getAnalyzer(), field, queryText, quoted || (ft instanceof TextField && ((TextField)ft).getAutoGeneratePhraseQueries()));
      } else {
        if (raw) {
          return new RawQuery(sf, queryText);
        } else {
          return sf.getType().getFieldQuery(parser, sf, queryText);
        }
      }
    }

    // default to a normal field query
    return newFieldQuery(getAnalyzer(), field, queryText, quoted);
  }



  // called from parser
  protected Query getRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) throws SyntaxError {
    checkNullField(field);
    SchemaField sf = schema.getField(field);
    return sf.getType().getRangeQuery(parser, sf, part1, part2, startInclusive, endInclusive);
  }

  // called from parser
  protected Query getPrefixQuery(String field, String termStr) throws SyntaxError {
    checkNullField(field);

    termStr = analyzeIfMultitermTermText(field, termStr, schema.getFieldType(field));

    // Solr has always used constant scoring for prefix queries.  This should return constant scoring by default.
    return newPrefixQuery(new Term(field, termStr));
  }

  // called from parser
  protected Query getWildcardQuery(String field, String termStr) throws SyntaxError {
    checkNullField(field);
    // *:* -> MatchAllDocsQuery
    if ("*".equals(termStr)) {
      if ("*".equals(field) || getExplicitField() == null) {
        return newMatchAllDocsQuery();
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
    }

    // Solr has always used constant scoring for wildcard queries.  This should return constant scoring by default.
    return newWildcardQuery(new Term(field, termStr));
  }

  // called from parser
  protected Query getRegexpQuery(String field, String termStr) throws SyntaxError
  {
    termStr = analyzeIfMultitermTermText(field, termStr, schema.getFieldType(field));
    return newRegexpQuery(new Term(field, termStr));
  }

  // called from parser
  protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws SyntaxError {
    termStr = analyzeIfMultitermTermText(field, termStr, schema.getFieldType(field));
    Term t = new Term(field, termStr);
    return newFuzzyQuery(t, minSimilarity, getFuzzyPrefixLength());
  }

  // called from parser
  protected Query getLocalParams(String qfield, String lparams) throws SyntaxError {
    QParser nested = parser.subQuery(lparams, null);
    return nested.getQuery();
  }

  // called from parser for filter(query)
  Query getFilter(Query q) {
    return new FilterQuery(q);
  }

}
