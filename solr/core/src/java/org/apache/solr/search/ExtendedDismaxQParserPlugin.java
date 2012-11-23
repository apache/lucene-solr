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

package org.apache.solr.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.queries.function.BoostedQuery;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ProductFloatFunction;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.*;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.parser.ParseException;
import org.apache.solr.parser.QueryParser;
import org.apache.solr.parser.SolrQueryParserBase.MagicFieldName;
import org.apache.solr.common.params.DisMaxParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.util.SolrPluginUtils;

/**
 * An advanced multi-field query parser based on the DisMax parser.
 * See Wiki page http://wiki.apache.org/solr/ExtendedDisMax
 * @lucene.experimental
 */
public class ExtendedDismaxQParserPlugin extends QParserPlugin {
  public static final String NAME = "edismax";

  public void init(NamedList args) {
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new ExtendedDismaxQParser(qstr, localParams, params, req);
  }
}


class ExtendedDismaxQParser extends QParser {

  /**
   * A field we can't ever find in any schema, so we can safely tell
   * DisjunctionMaxQueryParser to use it as our defaultField, and
   * map aliases from it to any field in our schema.
   */
  private static String IMPOSSIBLE_FIELD_NAME = "\uFFFC\uFFFC\uFFFC";

  /** shorten the class references for utilities */
  private static class U extends SolrPluginUtils {
    /* :NOOP */
  }

  /** shorten the class references for utilities */
  private static interface DMP extends DisMaxParams {
    /**
     * User fields. The fields that can be used by the end user to create field-specific queries.
     */
    public static String UF = "uf";
  }


  public ExtendedDismaxQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  /** 
   * The field names specified by 'qf' that (most) clauses will 
   * be queried against 
   */
  private Map<String,Float> queryFields;
   
  /** 
   * The field names specified by 'uf' that users are 
   * allowed to include literally in their query string.  The Float
   * boost values will be applied automaticly to any clause using that 
   * field name. '*' will be treated as an alias for any 
   * field that exists in the schema. Wildcards are allowed to
   * express dynamicFields.
   */
  private UserFields userFields;

  private Query parsedUserQuery;

  private String[] boostParams;
  private String[] multBoosts;
  private List<Query> boostQueries;
  private Query altUserQuery;
  private QParser altQParser;
  private SolrParams solrParams;


  @Override
  public Query parse() throws SyntaxError {
    SolrParams localParams = getLocalParams();
    SolrParams params = getParams();
    
    solrParams = SolrParams.wrapDefaults(localParams, params);

    final String minShouldMatch = 
      DisMaxQParser.parseMinShouldMatch(req.getSchema(), solrParams);

    userFields = new UserFields(U.parseFieldBoosts(solrParams.getParams(DMP.UF)));
    
    queryFields = DisMaxQParser.parseQueryFields(req.getSchema(), solrParams);

    // Phrase slop array
    int pslop[] = new int[4];
    pslop[0] = solrParams.getInt(DisMaxParams.PS, 0);
    pslop[2] = solrParams.getInt(DisMaxParams.PS2, pslop[0]);
    pslop[3] = solrParams.getInt(DisMaxParams.PS3, pslop[0]);

    
    // Boosted phrase of the full query string
    List<FieldParams> phraseFields = 
      U.parseFieldBoostsAndSlop(solrParams.getParams(DMP.PF),0,pslop[0]);
    // Boosted Bi-Term Shingles from the query string
    List<FieldParams> phraseFields2 = 
      U.parseFieldBoostsAndSlop(solrParams.getParams(DMP.PF2),2,pslop[2]);
    // Boosted Tri-Term Shingles from the query string
    List<FieldParams> phraseFields3 = 
      U.parseFieldBoostsAndSlop(solrParams.getParams(DMP.PF3),3,pslop[3]);

    float tiebreaker = solrParams.getFloat(DisMaxParams.TIE, 0.0f);

    int qslop = solrParams.getInt(DisMaxParams.QS, 0);

    // remove stopwords from mandatory "matching" component?
    boolean stopwords = solrParams.getBool("stopwords", true);

    /* the main query we will execute.  we disable the coord because
     * this query is an artificial construct
     */
    BooleanQuery query = new BooleanQuery(true);

    /* * * Main User Query * * */
    parsedUserQuery = null;
    String userQuery = getString();
    altUserQuery = null;
    if( userQuery == null || userQuery.trim().length() == 0 ) {
      // If no query is specified, we may have an alternate
      String altQ = solrParams.get( DisMaxParams.ALTQ );
      if (altQ != null) {
        altQParser = subQuery(altQ, null);
        altUserQuery = altQParser.getQuery();
        query.add( altUserQuery , BooleanClause.Occur.MUST );
      } else {
        return null;
        // throw new SyntaxError("missing query string" );
      }
    }
    else {     
      // There is a valid query string
      // userQuery = partialEscape(U.stripUnbalancedQuotes(userQuery)).toString();

      boolean lowercaseOperators = solrParams.getBool("lowercaseOperators", true);
      String mainUserQuery = userQuery;

      ExtendedSolrQueryParser up =
        new ExtendedSolrQueryParser(this, IMPOSSIBLE_FIELD_NAME);
      up.addAlias(IMPOSSIBLE_FIELD_NAME,
                tiebreaker, queryFields);
      addAliasesFromRequest(up, tiebreaker);
      up.setPhraseSlop(qslop);     // slop for explicit user phrase queries
      up.setAllowLeadingWildcard(true);

      // defer escaping and only do if lucene parsing fails, or we need phrases
      // parsing fails.  Need to sloppy phrase queries anyway though.
      List<Clause> clauses = null;
      int numPluses = 0;
      int numMinuses = 0;
      int numOR = 0;
      int numNOT = 0;

      clauses = splitIntoClauses(userQuery, false);
      for (Clause clause : clauses) {
        if (clause.must == '+') numPluses++;
        if (clause.must == '-') numMinuses++;
        if (clause.isBareWord()) {
          String s = clause.val;
          if ("OR".equals(s)) {
            numOR++;
          } else if ("NOT".equals(s)) {
            numNOT++;
          } else if (lowercaseOperators && "or".equals(s)) {
            numOR++;
          }
        }
      }

      // Always rebuild mainUserQuery from clauses to catch modifications from splitIntoClauses
      // This was necessary for userFields modifications to get propagated into the query.
      // Convert lower or mixed case operators to uppercase if we saw them.
      // only do this for the lucene query part and not for phrase query boosting
      // since some fields might not be case insensitive.
      // We don't use a regex for this because it might change and AND or OR in
      // a phrase query in a case sensitive field.
      StringBuilder sb = new StringBuilder();
      for (int i=0; i<clauses.size(); i++) {
        Clause clause = clauses.get(i);
        String s = clause.raw;
        // and and or won't be operators at the start or end
        if (lowercaseOperators && i>0 && i+1<clauses.size()) {
          if ("AND".equalsIgnoreCase(s)) {
            s="AND";
          } else if ("OR".equalsIgnoreCase(s)) {
            s="OR";
          }
        }
        sb.append(s);
        sb.append(' ');
      }
      
      mainUserQuery = sb.toString();
      
      // For correct lucene queries, turn off mm processing if there
      // were explicit operators (except for AND).
      boolean doMinMatched = (numOR + numNOT + numPluses + numMinuses) == 0;
      // but always for unstructured implicit bqs created by getFieldQuery
      up.minShouldMatch = minShouldMatch;

      try {
        up.setRemoveStopFilter(!stopwords);
        up.exceptions = true;
        parsedUserQuery = up.parse(mainUserQuery);

        if (stopwords && isEmpty(parsedUserQuery)) {
         // if the query was all stop words, remove none of them
          up.setRemoveStopFilter(true);
          parsedUserQuery = up.parse(mainUserQuery);          
        }
      } catch (Exception e) {
        // ignore failure and reparse later after escaping reserved chars
        up.exceptions = false;
      }

      if (parsedUserQuery != null && doMinMatched) {
        if (parsedUserQuery instanceof BooleanQuery) {
          SolrPluginUtils.setMinShouldMatch((BooleanQuery)parsedUserQuery, minShouldMatch);
        }
      }

      if (parsedUserQuery == null) {
        sb = new StringBuilder();
        for (Clause clause : clauses) {

          boolean doQuote = clause.isPhrase;

          String s=clause.val;
          if (!clause.isPhrase && ("OR".equals(s) || "AND".equals(s) || "NOT".equals(s))) {
            doQuote=true;
          }

          if (clause.must != 0) {
            sb.append(clause.must);
          }
          if (clause.field != null) {
            sb.append(clause.field);
            sb.append(':');
          }
          if (doQuote) {
            sb.append('"');
          }
          sb.append(clause.val);
          if (doQuote) {
            sb.append('"');
          }
          if (clause.field != null) {
            // Add the default user field boost, if any
            Float boost = userFields.getBoost(clause.field);
            if(boost != null)
              sb.append("^").append(boost);
          }
          sb.append(' ');
        }
        String escapedUserQuery = sb.toString();
        parsedUserQuery = up.parse(escapedUserQuery);

        if (parsedUserQuery instanceof BooleanQuery) {
          BooleanQuery t = new BooleanQuery();
          SolrPluginUtils.flattenBooleanQuery(t, (BooleanQuery)parsedUserQuery);
          SolrPluginUtils.setMinShouldMatch(t, minShouldMatch);
          parsedUserQuery = t;
        }
      }

      query.add(parsedUserQuery, BooleanClause.Occur.MUST);

      // sloppy phrase queries for proximity
      List<FieldParams> allPhraseFields = new ArrayList<FieldParams>();
      allPhraseFields.addAll(phraseFields);
      allPhraseFields.addAll(phraseFields2);
      allPhraseFields.addAll(phraseFields3);

      if (allPhraseFields.size() > 0) {
        // find non-field clauses
        List<Clause> normalClauses = new ArrayList<Clause>(clauses.size());
        for (Clause clause : clauses) {
          if (clause.field != null || clause.isPhrase) continue;
          // check for keywords "AND,OR,TO"
          if (clause.isBareWord()) {
            String s = clause.val.toString();
            // avoid putting explict operators in the phrase query
            if ("OR".equals(s) || "AND".equals(s) || "NOT".equals(s) || "TO".equals(s)) continue;
          }
          normalClauses.add(clause);
        }

        // full phrase and shingles
        for (FieldParams phraseField: allPhraseFields) {
          Map<String,Float> pf = new HashMap<String,Float>(1);
          pf.put(phraseField.getField(),phraseField.getBoost());
          addShingledPhraseQueries(query, normalClauses, pf,   
          phraseField.getWordGrams(),tiebreaker, phraseField.getSlop());
        }
        
      }
    }



    /* * * Boosting Query * * */
    boostParams = solrParams.getParams(DisMaxParams.BQ);
    //List<Query> boostQueries = U.parseQueryStrings(req, boostParams);
    boostQueries=null;
    if (boostParams!=null && boostParams.length>0) {
      boostQueries = new ArrayList<Query>();
      for (String qs : boostParams) {
        if (qs.trim().length()==0) continue;
        Query q = subQuery(qs, null).getQuery();
        boostQueries.add(q);
      }
    }
    if (null != boostQueries) {
      for(Query f : boostQueries) {
        query.add(f, BooleanClause.Occur.SHOULD);
      }
    }

    /* * * Boosting Functions * * */

    String[] boostFuncs = solrParams.getParams(DisMaxParams.BF);
    if (null != boostFuncs && 0 != boostFuncs.length) {
      for (String boostFunc : boostFuncs) {
        if(null == boostFunc || "".equals(boostFunc)) continue;
        Map<String,Float> ff = SolrPluginUtils.parseFieldBoosts(boostFunc);
        for (String f : ff.keySet()) {
          Query fq = subQuery(f, FunctionQParserPlugin.NAME).getQuery();
          Float b = ff.get(f);
          if (null != b) {
            fq.setBoost(b);
          }
          query.add(fq, BooleanClause.Occur.SHOULD);
        }
      }
    }


    //
    // create a boosted query (scores multiplied by boosts)
    //
    Query topQuery = query;
    multBoosts = solrParams.getParams("boost");
    if (multBoosts!=null && multBoosts.length>0) {

      List<ValueSource> boosts = new ArrayList<ValueSource>();
      for (String boostStr : multBoosts) {
        if (boostStr==null || boostStr.length()==0) continue;
        Query boost = subQuery(boostStr, FunctionQParserPlugin.NAME).getQuery();
        ValueSource vs;
        if (boost instanceof FunctionQuery) {
          vs = ((FunctionQuery)boost).getValueSource();
        } else {
          vs = new QueryValueSource(boost, 1.0f);
        }
        boosts.add(vs);
      }

      if (boosts.size()>1) {
        ValueSource prod = new ProductFloatFunction(boosts.toArray(new ValueSource[boosts.size()]));
        topQuery = new BoostedQuery(query, prod);
      } else if (boosts.size() == 1) {
        topQuery = new BoostedQuery(query, boosts.get(0));
      }
    }

    return topQuery;
  }

  /**
   * Extracts all the aliased fields from the requests and adds them to up
   */
  private void addAliasesFromRequest(ExtendedSolrQueryParser up, float tiebreaker) {
    Iterator<String> it = solrParams.getParameterNamesIterator();
    while(it.hasNext()) {
      String param = it.next();
      if(param.startsWith("f.") && param.endsWith(".qf")) {
        // Add the alias
        String fname = param.substring(2,param.length()-3);
        String qfReplacement = solrParams.get(param);
        Map<String,Float> parsedQf = SolrPluginUtils.parseFieldBoosts(qfReplacement);
        if(parsedQf.size() == 0)
          return;
        up.addAlias(fname, tiebreaker, parsedQf);
      }
    }
  }

  /**
   * Modifies the main query by adding a new optional Query consisting
   * of shingled phrase queries across the specified clauses using the 
   * specified field =&gt; boost mappings.
   *
   * @param mainQuery Where the phrase boosting queries will be added
   * @param clauses Clauses that will be used to construct the phrases
   * @param fields Field =&gt; boost mappings for the phrase queries
   * @param shingleSize how big the phrases should be, 0 means a single phrase
   * @param tiebreaker tie breker value for the DisjunctionMaxQueries
   * @param slop slop value for the constructed phrases
   */
  private void addShingledPhraseQueries(final BooleanQuery mainQuery, 
                                        final List<Clause> clauses,
                                        final Map<String,Float> fields,
                                        int shingleSize,
                                        final float tiebreaker,
                                        final int slop) 
    throws SyntaxError {
    
    if (null == fields || fields.isEmpty() || 
        null == clauses || clauses.size() < shingleSize ) 
      return;
    
    if (0 == shingleSize) shingleSize = clauses.size();

    final int goat = shingleSize-1; // :TODO: better name for var?

    StringBuilder userPhraseQuery = new StringBuilder();
      for (int i=0; i < clauses.size() - goat; i++) {
        userPhraseQuery.append('"');
        for (int j=0; j <= goat; j++) {
          userPhraseQuery.append(clauses.get(i + j).val);
          userPhraseQuery.append(' ');
        }
        userPhraseQuery.append('"');
        userPhraseQuery.append(' ');
      }

      /* for parsing sloppy phrases using DisjunctionMaxQueries */
      ExtendedSolrQueryParser pp =
        new ExtendedSolrQueryParser(this, IMPOSSIBLE_FIELD_NAME);

      pp.addAlias(IMPOSSIBLE_FIELD_NAME, tiebreaker, fields);
      pp.setPhraseSlop(slop);
      pp.setRemoveStopFilter(true);  // remove stop filter and keep stopwords

      /* :TODO: reevaluate using makeDismax=true vs false...
       * 
       * The DismaxQueryParser always used DisjunctionMaxQueries for the 
       * pf boost, for the same reasons it used them for the qf fields.
       * When Yonik first wrote the ExtendedDismaxQParserPlugin, he added
       * the "makeDismax=false" property to use BooleanQueries instead, but 
       * when asked why his response was "I honestly don't recall" ...
       *
       * https://issues.apache.org/jira/browse/SOLR-1553?focusedCommentId=12793813#action_12793813
       *
       * so for now, we continue to use dismax style queries becuse it 
       * seems the most logical and is back compatible, but we should 
       * try to figure out what Yonik was thinking at the time (because he 
       * rarely does things for no reason)
       */
      pp.makeDismax = true; 


      // minClauseSize is independent of the shingleSize because of stop words
      // (if they are removed from the middle, so be it, but we need at least 
      // two or there shouldn't be a boost)
      pp.minClauseSize = 2;  
      
      // TODO: perhaps we shouldn't use synonyms either...

      Query phrase = pp.parse(userPhraseQuery.toString());
      if (phrase != null) {
        mainQuery.add(phrase, BooleanClause.Occur.SHOULD);
      }
  }


  @Override
  public String[] getDefaultHighlightFields() {
    String[] highFields = queryFields.keySet().toArray(new String[0]);
    return highFields;
  }

  @Override
  public Query getHighlightQuery() throws SyntaxError {
    return parsedUserQuery == null ? altUserQuery : parsedUserQuery;
  }

  @Override
  public void addDebugInfo(NamedList<Object> debugInfo) {
    super.addDebugInfo(debugInfo);
    debugInfo.add("altquerystring", altUserQuery);
    if (null != boostQueries) {
      debugInfo.add("boost_queries", boostParams);
      debugInfo.add("parsed_boost_queries",
                QueryParsing.toString(boostQueries, getReq().getSchema()));
    }
    debugInfo.add("boostfuncs", getReq().getParams().getParams(DisMaxParams.BF));
  }


// FIXME: Not in use
//  public static CharSequence partialEscape(CharSequence s) {
//    StringBuilder sb = new StringBuilder();
//
//    int len = s.length();
//    for (int i = 0; i < len; i++) {
//      char c = s.charAt(i);
//      if (c == ':') {
//        // look forward to make sure it's something that won't
//        // cause a parse exception (something that won't be escaped... like
//        // +,-,:, whitespace
//        if (i+1<len && i>0) {
//          char ch = s.charAt(i+1);
//          if (!(Character.isWhitespace(ch) || ch=='+' || ch=='-' || ch==':')) {
//            // OK, at this point the chars after the ':' will be fine.
//            // now look back and try to determine if this is a fieldname
//            // [+,-]? [letter,_] [letter digit,_,-,.]*
//            // This won't cover *all* possible lucene fieldnames, but we should
//            // only pick nice names to begin with
//            int start, pos;
//            for (start=i-1; start>=0; start--) {
//              ch = s.charAt(start);
//              if (Character.isWhitespace(ch)) break;
//            }
//
//            // skip whitespace
//            pos = start+1;
//
//            // skip leading + or -
//            ch = s.charAt(pos);
//            if (ch=='+' || ch=='-') {
//              pos++;
//            }
//
//            // we don't need to explicitly check for end of string
//            // since ':' will act as our sentinal
//
//              // first char can't be '-' or '.'
//              ch = s.charAt(pos++);
//              if (Character.isJavaIdentifierPart(ch)) {
//
//                for(;;) {
//                  ch = s.charAt(pos++);
//                  if (!(Character.isJavaIdentifierPart(ch) || ch=='-' || ch=='.')) {
//                    break;
//                  }
//                }
//
//                if (pos<=i) {
//                  // OK, we got to the ':' and everything looked like a valid fieldname, so
//                  // don't escape the ':'
//                  sb.append(':');
//                  continue;  // jump back to start of outer-most loop
//                }
//
//              }
//
//
//          }
//        }
//
//        // we fell through to here, so we should escape this like other reserved chars.
//        sb.append('\\');
//      }
//      else if (c == '\\' || c == '!' || c == '(' || c == ')' ||
//          c == '^' || c == '[' || c == ']' ||
//          c == '{'  || c == '}' || c == '~' || c == '*' || c == '?'
//          )
//      {
//        sb.append('\\');
//      }
//      sb.append(c);
//    }
//    return sb;
//  }


  static class Clause {

    boolean isBareWord() {
      return must==0 && !isPhrase;
    }

    String field;
    String rawField;  // if the clause is +(foo:bar) then rawField=(foo
    boolean isPhrase;
    boolean hasWhitespace;
    boolean hasSpecialSyntax;
    boolean syntaxError;
    char must;   // + or -
    String val;  // the field value (minus the field name, +/-, quotes)
    String raw;  // the raw clause w/o leading/trailing whitespace
  }

  
  public List<Clause> splitIntoClauses(String s, boolean ignoreQuote) {
    ArrayList<Clause> lst = new ArrayList<Clause>(4);
    Clause clause;

    int pos=0;
    int end=s.length();
    char ch=0;
    int start;
    boolean disallowUserField;
    outer: while (pos < end) {
      clause = new Clause();
      disallowUserField = true;
      
      ch = s.charAt(pos);

      while (Character.isWhitespace(ch)) {
        if (++pos >= end) break;
        ch = s.charAt(pos);
      }

      start = pos;      

      if (ch=='+' || ch=='-') {
        clause.must = ch;
        pos++;
      }

      clause.field = getFieldName(s, pos, end);
      if(clause.field != null && !userFields.isAllowed(clause.field)) {
        clause.field = null;
      }
      if (clause.field != null) {
        disallowUserField = false;
        int colon = s.indexOf(':',pos);
        clause.rawField = s.substring(pos, colon);
        pos += colon - pos; // skip the field name
        pos++;  // skip the ':'
      }

      if (pos>=end) break;


      char inString=0;

      ch = s.charAt(pos);
      if (!ignoreQuote && ch=='"') {
        clause.isPhrase = true;
        inString = '"';
        pos++;
      }

      StringBuilder sb = new StringBuilder();
      while (pos < end) {
        ch = s.charAt(pos++);
        if (ch=='\\') {    // skip escaped chars, but leave escaped
          sb.append(ch);
          if (pos >= end) {
            sb.append(ch); // double backslash if we are at the end of the string
            break;
          }
          ch = s.charAt(pos++);
          sb.append(ch);
          continue;
        } else if (inString != 0 && ch == inString) {
          inString=0;
          break;
        } else if (Character.isWhitespace(ch)) {
          clause.hasWhitespace=true;
          if (inString == 0) {
            // end of the token if we aren't in a string, backing
            // up the position.
            pos--;
            break;
          }
        }

        if (inString == 0) {
          switch (ch) {
            case '!':
            case '(':
            case ')':
            case ':':
            case '^':
            case '[':
            case ']':
            case '{':
            case '}':
            case '~':
            case '*':
            case '?':
            case '"':
            case '+':
            case '-':
            case '\\':
            case '|':
            case '&':
            case '/':
              clause.hasSpecialSyntax = true;
              sb.append('\\');
          }
        } else if (ch=='"') {
          // only char we need to escape in a string is double quote
          sb.append('\\');
        }
        sb.append(ch);
      }
      clause.val = sb.toString();
 
      if (clause.isPhrase) {
        if (inString != 0) {
          // detected bad quote balancing... retry
          // parsing with quotes like any other char
          return splitIntoClauses(s, true);
        }

        // special syntax in a string isn't special
        clause.hasSpecialSyntax = false;        
      } else {
        // an empty clause... must be just a + or - on it's own
        if (clause.val.length() == 0) {
          clause.syntaxError = true;
          if (clause.must != 0) {
            clause.val="\\"+clause.must;
            clause.must = 0;
            clause.hasSpecialSyntax = true;
          } else {
            // uh.. this shouldn't happen.
            clause=null;
          }
        }
      }

      if (clause != null) {
        if(disallowUserField) {
          clause.raw = s.substring(start, pos);
          // escape colons, except for "match all" query
          if(!"*:*".equals(clause.raw)) {
            clause.raw = clause.raw.replaceAll(":", "\\\\:");
          }
        } else {
          clause.raw = s.substring(start, pos);
          // Add default userField boost if no explicit boost exists
          if(userFields.isAllowed(clause.field) && !clause.raw.contains("^")) {
            Float boost = userFields.getBoost(clause.field);
            if(boost != null)
              clause.raw += "^" + boost;
          }
        }
        lst.add(clause);
      }
    }

    return lst;
  }

  /** 
   * returns a field name or legal field alias from the current 
   * position of the string 
   */
  public String getFieldName(String s, int pos, int end) {
    if (pos >= end) return null;
    int p=pos;
    int colon = s.indexOf(':',pos);
    // make sure there is space after the colon, but not whitespace
    if (colon<=pos || colon+1>=end || Character.isWhitespace(s.charAt(colon+1))) return null;
    char ch = s.charAt(p++);
    while ((ch=='(' || ch=='+' || ch=='-') && (pos<end)) {
      ch = s.charAt(p++);
      pos++;
    }
    if (!Character.isJavaIdentifierPart(ch)) return null;
    while (p<colon) {
      ch = s.charAt(p++);
      if (!(Character.isJavaIdentifierPart(ch) || ch=='-' || ch=='.')) return null;
    }
    String fname = s.substring(pos, p);
    boolean isInSchema = getReq().getSchema().getFieldTypeNoEx(fname) != null;
    boolean isAlias = solrParams.get("f."+fname+".qf") != null;
    boolean isMagic = (null != MagicFieldName.get(fname));
    
    return (isInSchema || isAlias || isMagic) ? fname : null;
  }

  public static List<String> split(String s, boolean ignoreQuote) {
    ArrayList<String> lst = new ArrayList<String>(4);
    int pos=0, start=0, end=s.length();
    char inString=0;
    char ch=0;
    while (pos < end) {
      char prevChar=ch;
      ch = s.charAt(pos++);
      if (ch=='\\') {    // skip escaped chars
        pos++;
      } else if (inString != 0 && ch==inString) {
        inString=0;
      } else if (!ignoreQuote && ch=='"') {
        // If char is directly preceeded by a number or letter
        // then don't treat it as the start of a string.
        if (!Character.isLetterOrDigit(prevChar)) {
          inString=ch;
        }
      } else if (Character.isWhitespace(ch) && inString==0) {
        lst.add(s.substring(start,pos-1));
        start=pos;
      }
    }
    if (start < end) {
      lst.add(s.substring(start,end));
    }

    if (inString != 0) {
      // unbalanced quote... ignore them
      return split(s, true);
    }

    return lst;
  }




    enum QType {
      FIELD,
      PHRASE,
      PREFIX,
      WILDCARD,
      FUZZY,
      RANGE
    }


  static final RuntimeException unknownField = new RuntimeException("UnknownField");
  static {
    unknownField.fillInStackTrace();
  }

  /**
   * A subclass of SolrQueryParser that supports aliasing fields for
   * constructing DisjunctionMaxQueries.
   */
  static class ExtendedSolrQueryParser extends SolrQueryParser {


    /** A simple container for storing alias info
     */
    protected class Alias {
      public float tie;
      public Map<String,Float> fields;
    }

    boolean makeDismax=true;
    boolean disableCoord=true;
    boolean allowWildcard=true;
    int minClauseSize = 0;    // minimum number of clauses per phrase query...
                              // used when constructing boosting part of query via sloppy phrases
    boolean exceptions;  //  allow exceptions to be thrown (for example on a missing field)

    private Map<String, Analyzer> nonStopFilterAnalyzerPerField;
    private boolean removeStopFilter;
    String minShouldMatch; // for inner boolean queries produced from a single fieldQuery

    /**
     * Where we store a map from field name we expect to see in our query
     * string, to Alias object containing the fields to use in our
     * DisjunctionMaxQuery and the tiebreaker to use.
     */
    protected Map<String,Alias> aliases = new HashMap<String,Alias>(3);

    public ExtendedSolrQueryParser(QParser parser, String defaultField) {
      super(parser, defaultField);
      // don't trust that our parent class won't ever change it's default
      setDefaultOperator(QueryParser.Operator.OR);
    }

    public void setRemoveStopFilter(boolean remove) {
//      analyzer.removeStopFilter = remove;
      removeStopFilter = remove;
    }

    @Override
    protected Query getBooleanQuery(List clauses, boolean disableCoord) throws SyntaxError {
      Query q = super.getBooleanQuery(clauses, disableCoord);
      if (q != null) {
        q = QueryUtils.makeQueryable(q);
      }
      return q;
    }


    ////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////

    @Override
    protected void addClause(List clauses, int conj, int mods, Query q) {
//System.out.println("addClause:clauses="+clauses+" conj="+conj+" mods="+mods+" q="+q);
      super.addClause(clauses, conj, mods, q);
    }

    /**
     * Add an alias to this query parser.
     *
     * @param field the field name that should trigger alias mapping
     * @param fieldBoosts the mapping from fieldname to boost value that
     *                    should be used to build up the clauses of the
     *                    DisjunctionMaxQuery.
     * @param tiebreaker to the tiebreaker to be used in the
     *                   DisjunctionMaxQuery
     * @see SolrPluginUtils#parseFieldBoosts
     */
    public void addAlias(String field, float tiebreaker,
                         Map<String,Float> fieldBoosts) {

      Alias a = new Alias();
      a.tie = tiebreaker;
      a.fields = fieldBoosts;
      aliases.put(field, a);
    }
    
    /**
     * Returns the aliases found for a field.
     * Returns null if there are no aliases for the field
     * @return Alias
     */
    public Alias getAlias(String field) {
      return aliases.get(field);
    }


    QType type;
    String field;
    String val;
    String val2;
    boolean bool;
    boolean bool2;
    float flt;
    int slop;

    @Override
    protected Query getFieldQuery(String field, String val, boolean quoted) throws SyntaxError {
//System.out.println("getFieldQuery: val="+val);

      this.type = QType.FIELD;
      this.field = field;
      this.val = val;
      this.slop = getPhraseSlop(); // unspecified
      return getAliasedQuery();
    }

    @Override
    protected Query getFieldQuery(String field, String val, int slop) throws SyntaxError {
//System.out.println("getFieldQuery: val="+val+" slop="+slop);

      this.type = QType.PHRASE;
      this.field = field;
      this.val = val;
      this.slop = slop;
      return getAliasedQuery();
    }

    @Override
    protected Query getPrefixQuery(String field, String val) throws SyntaxError {
//System.out.println("getPrefixQuery: val="+val);
      if (val.equals("") && field.equals("*")) {
        return new MatchAllDocsQuery();
      }
      this.type = QType.PREFIX;
      this.field = field;
      this.val = val;
      return getAliasedQuery();
    }

    @Override
    protected Query newFieldQuery(Analyzer analyzer, String field, String queryText, boolean quoted) throws SyntaxError {
      Analyzer actualAnalyzer;
      if (removeStopFilter) {
        if (nonStopFilterAnalyzerPerField == null) {
          nonStopFilterAnalyzerPerField = new HashMap<String, Analyzer>();
        }
        actualAnalyzer = nonStopFilterAnalyzerPerField.get(field);
        if (actualAnalyzer == null) {
          actualAnalyzer = noStopwordFilterAnalyzer(field);
        }
      } else {
        actualAnalyzer = parser.getReq().getSchema().getFieldType(field).getQueryAnalyzer();
      }
      return super.newFieldQuery(actualAnalyzer, field, queryText, quoted);
    }

    @Override
     protected Query getRangeQuery(String field, String a, String b, boolean startInclusive, boolean endInclusive) throws SyntaxError {
//System.out.println("getRangeQuery:");

      this.type = QType.RANGE;
      this.field = field;
      this.val = a;
      this.val2 = b;
      this.bool = startInclusive;
      this.bool2 = endInclusive;
      return getAliasedQuery();
    }

    @Override
    protected Query getWildcardQuery(String field, String val) throws SyntaxError {
//System.out.println("getWildcardQuery: val="+val);

      if (val.equals("*")) {
        if (field.equals("*")) {
          return new MatchAllDocsQuery();
        } else{
          return getPrefixQuery(field,"");
        }
      }
      this.type = QType.WILDCARD;
      this.field = field;
      this.val = val;
      return getAliasedQuery();
    }

    @Override
    protected Query getFuzzyQuery(String field, String val, float minSimilarity) throws SyntaxError {
//System.out.println("getFuzzyQuery: val="+val);

      this.type = QType.FUZZY;
      this.field = field;
      this.val = val;
      this.flt = minSimilarity;
      return getAliasedQuery();
    }

    /**
     * Delegates to the super class unless the field has been specified
     * as an alias -- in which case we recurse on each of
     * the aliased fields, and the results are composed into a
     * DisjunctionMaxQuery.  (so yes: aliases which point at other
     * aliases should work)
     */
    protected Query getAliasedQuery() throws SyntaxError {
      Alias a = aliases.get(field);
      this.validateCyclicAliasing(field);
      if (a != null) {
        List<Query> lst = getQueries(a);
        if (lst == null || lst.size()==0)
            return getQuery();
        // make a DisjunctionMaxQuery in this case too... it will stop
        // the "mm" processing from making everything required in the case
        // that the query expanded to multiple clauses.
        // DisMaxQuery.rewrite() removes itself if there is just a single clause anyway.
        // if (lst.size()==1) return lst.get(0);

        if (makeDismax) {
          DisjunctionMaxQuery q = new DisjunctionMaxQuery(lst, a.tie);
          return q;
        } else {
          // should we disable coord?
          BooleanQuery q = new BooleanQuery(disableCoord);
          for (Query sub : lst) {
            q.add(sub, BooleanClause.Occur.SHOULD);
          }
          return q;
        }
      } else {

        // verify that a fielded query is actually on a field that exists... if not,
        // then throw an exception to get us out of here, and we'll treat it like a
        // literal when we try the escape+re-parse.
        if (exceptions) {
          FieldType ft = schema.getFieldTypeNoEx(field);
          if (ft == null && null == MagicFieldName.get(field)) {
            throw unknownField;
          }
        }

        return getQuery();
      }
    }

    /**
     * Validate there is no cyclic referencing in the aliasing
     */
    private void validateCyclicAliasing(String field) throws SyntaxError {
       Set<String> set = new HashSet<String>();
       set.add(field);
       if(validateField(field, set)) {
         throw new SyntaxError("Field aliases lead to a cycle");
       }
    }
    
    private boolean validateField(String field, Set<String> set) {
      if(this.getAlias(field) == null) {
        return false;
      }
      boolean hascycle = false;
      for(String referencedField:this.getAlias(field).fields.keySet()) {
        if(!set.add(referencedField)) {
          hascycle = true;
        } else {
          if(validateField(referencedField, set)) {
            hascycle = true;
          }
          set.remove(referencedField);
        }
      }
      return hascycle;
    }

    protected List<Query> getQueries(Alias a) throws SyntaxError {
       if (a == null) return null;
       if (a.fields.size()==0) return null;
       List<Query> lst= new ArrayList<Query>(4);

       for (String f : a.fields.keySet()) {
         this.field = f;
         Query sub = getAliasedQuery();
         if (sub != null) {
           Float boost = a.fields.get(f);
           if (boost != null) {
              sub.setBoost(boost);
           }
           lst.add(sub);
         }
       }
       return lst;
     }

    private Query getQuery() {
      try {

        switch (type) {
          case FIELD:  // fallthrough
          case PHRASE:
            Query query = super.getFieldQuery(field, val, type == QType.PHRASE);
            // A BooleanQuery is only possible from getFieldQuery if it came from
            // a single whitespace separated term. In this case, check the coordination
            // factor on the query: if its enabled, that means we aren't a set of synonyms
            // but instead multiple terms from one whitespace-separated term, we must
            // apply minShouldMatch here so that it works correctly with other things
            // like aliasing.
            if (query instanceof BooleanQuery) {
              BooleanQuery bq = (BooleanQuery) query;
              if (!bq.isCoordDisabled()) {
                SolrPluginUtils.setMinShouldMatch(bq, minShouldMatch);
              }
            }
            if (query instanceof PhraseQuery) {
              PhraseQuery pq = (PhraseQuery)query;
              if (minClauseSize > 1 && pq.getTerms().length < minClauseSize) return null;
              ((PhraseQuery)query).setSlop(slop);
            } else if (query instanceof MultiPhraseQuery) {
              MultiPhraseQuery pq = (MultiPhraseQuery)query;
              if (minClauseSize > 1 && pq.getTermArrays().size() < minClauseSize) return null;
              ((MultiPhraseQuery)query).setSlop(slop);
            } else if (minClauseSize > 1) {
              // if it's not a type of phrase query, it doesn't meet the minClauseSize requirements
              return null;
            }
            return query;
          case PREFIX: return super.getPrefixQuery(field, val);
          case WILDCARD: return super.getWildcardQuery(field, val);
          case FUZZY: return super.getFuzzyQuery(field, val, flt);
          case RANGE: return super.getRangeQuery(field, val, val2, bool, bool2);
        }
        return null;

      } catch (Exception e) {
        // an exception here is due to the field query not being compatible with the input text
        // for example, passing a string to a numeric field.
        return null;
      }
    }

    private Analyzer noStopwordFilterAnalyzer(String fieldName) {
      FieldType ft = parser.getReq().getSchema().getFieldType(fieldName);
      Analyzer qa = ft.getQueryAnalyzer();
      if (!(qa instanceof TokenizerChain)) {
        return qa;
      }

      TokenizerChain tcq = (TokenizerChain) qa;
      Analyzer ia = ft.getAnalyzer();
      if (ia == qa || !(ia instanceof TokenizerChain)) {
        return qa;
      }
      TokenizerChain tci = (TokenizerChain) ia;

      // make sure that there isn't a stop filter in the indexer
      for (TokenFilterFactory tf : tci.getTokenFilterFactories()) {
        if (tf instanceof StopFilterFactory) {
          return qa;
        }
      }

      // now if there is a stop filter in the query analyzer, remove it
      int stopIdx = -1;
      TokenFilterFactory[] facs = tcq.getTokenFilterFactories();

      for (int i = 0; i < facs.length; i++) {
        TokenFilterFactory tf = facs[i];
        if (tf instanceof StopFilterFactory) {
          stopIdx = i;
          break;
        }
      }

      if (stopIdx == -1) {
        // no stop filter exists
        return qa;
      }

      TokenFilterFactory[] newtf = new TokenFilterFactory[facs.length - 1];
      for (int i = 0, j = 0; i < facs.length; i++) {
        if (i == stopIdx) continue;
        newtf[j++] = facs[i];
      }

      TokenizerChain newa = new TokenizerChain(tcq.getTokenizerFactory(), newtf);
      newa.setPositionIncrementGap(tcq.getPositionIncrementGap(fieldName));
      return newa;
    }
  }

  static boolean isEmpty(Query q) {
    if (q==null) return true;
    if (q instanceof BooleanQuery && ((BooleanQuery)q).clauses().size()==0) return true;
    return false;
  }
  
  /**
   * Class that encapsulates the input from userFields parameter and can answer whether
   * a field allowed or disallowed as fielded query in the query string
   */
  static class UserFields {
    private Map<String,Float> userFieldsMap;
    private DynamicField[] dynamicUserFields;
    private DynamicField[] negativeDynamicUserFields;

    UserFields(Map<String,Float> ufm) {
      userFieldsMap = ufm;
      if (0 == userFieldsMap.size()) {
        userFieldsMap.put("*", null);
      }
      
      // Process dynamic patterns in userFields
      ArrayList<DynamicField> dynUserFields = new ArrayList<DynamicField>();
      ArrayList<DynamicField> negDynUserFields = new ArrayList<DynamicField>();
      for(String f : userFieldsMap.keySet()) {
        if(f.contains("*")) {
          if(f.startsWith("-"))
            negDynUserFields.add(new DynamicField(f.substring(1)));
          else
            dynUserFields.add(new DynamicField(f));
        }
      }
      Collections.sort(dynUserFields);
      dynamicUserFields = dynUserFields.toArray(new DynamicField[dynUserFields.size()]);
      Collections.sort(negDynUserFields);
      negativeDynamicUserFields = negDynUserFields.toArray(new DynamicField[negDynUserFields.size()]);
//      System.out.println("** userF="+userFieldsMap+", dynUF="+Arrays.toString(dynamicUserFields)+", negDynUF="+Arrays.toString(negativeDynamicUserFields));
    }
    
    /**
     * Is the given field name allowed according to UserFields spec given in the uf parameter?
     * @param fname the field name to examine
     * @return true if the fielded queries are allowed on this field
     */
    public boolean isAllowed(String fname) {
      boolean res = ((userFieldsMap.containsKey(fname) || isDynField(fname, false)) && 
          !userFieldsMap.containsKey("-"+fname) &&
          !isDynField(fname, true));
      return res;
    }
    
    private boolean isDynField(String field, boolean neg) {
      return getDynFieldForName(field, neg) == null ? false : true;
    }
    
    private String getDynFieldForName(String f, boolean neg) {
      for( DynamicField df : neg?negativeDynamicUserFields:dynamicUserFields ) {
        if( df.matches( f ) ) return df.wildcard;
      }
      return null;
    }
    
    /**
     * Finds the default user field boost associated with the given field.
     * This is parsed from the uf parameter, and may be specified as wildcards, e.g. *name^2.0 or *^3.0
     * @param field the field to find boost for
     * @return the float boost value associated with the given field or a wildcard matching the field
     */
    public Float getBoost(String field) {
      return (userFieldsMap.containsKey(field)) ?
          userFieldsMap.get(field) : // Exact field
          userFieldsMap.get(getDynFieldForName(field, false)); // Dynamic field
    }
  }
  
  /* Represents a dynamic field, for easier matching, inspired by same class in IndexSchema */
  static class DynamicField implements Comparable<DynamicField> {
    final static int STARTS_WITH=1;
    final static int ENDS_WITH=2;
    final static int CATCHALL=3;

    final String wildcard;
    final int type;

    final String str;

    protected DynamicField(String wildcard) {
      this.wildcard = wildcard;
      if (wildcard.equals("*")) {
        type=CATCHALL;
        str=null;
      }
      else if (wildcard.startsWith("*")) {
        type=ENDS_WITH;
        str=wildcard.substring(1);
      }
      else if (wildcard.endsWith("*")) {
        type=STARTS_WITH;
        str=wildcard.substring(0,wildcard.length()-1);
      }
      else {
        throw new RuntimeException("dynamic field name must start or end with *");
      }
    }

    /*
     * Returns true if the regex wildcard for this DynamicField would match the input field name
     */
    public boolean matches(String name) {
      if (type==CATCHALL) return true;
      else if (type==STARTS_WITH && name.startsWith(str)) return true;
      else if (type==ENDS_WITH && name.endsWith(str)) return true;
      else return false;
    }

    /**
     * Sort order is based on length of regex.  Longest comes first.
     * @param other The object to compare to.
     * @return a negative integer, zero, or a positive integer
     * as this object is less than, equal to, or greater than
     * the specified object.
     */
    public int compareTo(DynamicField other) {
      return other.wildcard.length() - wildcard.length();
    }
    
    public String toString() {
      return this.wildcard;
    }
  }
}
