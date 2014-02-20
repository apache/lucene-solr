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
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.CharStream;
import org.apache.lucene.queryparser.classic.ParseException;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.Version;

/**
 * <p>This parser leverages the power of SpanQuery and can combine them with
 * traditional boolean logic and multiple field information.
 * This parser includes functionality from:
 * <ul>
 * <li> {@link org.apache.lucene.queryparser.classic.QueryParser classic QueryParser}: most of its syntax</li>
 * <li> {@link org.apache.lucene.queryparser.surround.parser.QueryParser SurroundQueryParser}: recursive parsing for "near" and "not" clauses.</li>
 * <li> {@link org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser}: 
 * can handle "near" queries that include multiterms ({@link org.apache.lucene.search.WildcardQuery},
 * {@link org.apache.lucene.search.FuzzyQuery}, {@link org.apache.lucene.search.RegexpQuery}).</li>
 * <li> {@link org.apache.lucene.queryparser.analyzing.AnalyzingQueryParser}: has an option to analyze multiterms.</li>
 * </ul>
 * 
 * </p>
 * 
 * <p>
 * <b>Background</b>
 * This parser is designed to expose as much of the sophistication as is available within the Query/SpanQuery components.
 * The basic approach of this parser is to build BooleanQueries comprised of SpanQueries.  The parser recursively works 
 * through boolean/fielded chunks and then recursively works through SpanQueries.
 * </p>
 * 
 * <p>
 * Goals for this parser:
 * <ul>
 * <li>Expose as much of the underlying capabilities as possible.</li>
 * <li>Keep the syntax as close to Lucene's classic 
 * {@link org.apache.lucene.queryparser.classic.QueryParser} as possible.</li>
 * <li>Make analysis of multiterms a fundamental part of the parser 
 * {@link AnalyzingQueryParserBase}.</li>
 * </ul>
 * </p>
 * <p><b>Similarities and Differences</b></p>
 * 
 * <p> Same as classic syntax:
 * <ul>
 * <li> term: test </li>
 * <li> fuzzy: roam~0.8, roam~2</li>
 * <li> wildcard: te?t, test*, t*st</li>
 * <li> regex: <code>/[mb]oat/</code></li>
 * <li> phrase: &quot;jakarta apache&quot;</li>
 * <li> phrase with slop: &quot;jakarta apache&quot;~3</li>
 * <li> &quot;or&quot; clauses: jakarta apache</li>
 * <li> grouping clauses: (jakarta apache)</li>
 * <li> field: author:hatcher title:lucene</li>
 * <li> boolean operators: (lucene AND apache) NOT jakarta
 * <li> required/not required operators: +lucene +apache -jakarta</li>
 * <li> boolean with field:(author:hatcher AND author:gospodnetic) AND title:lucene</li>
 * </ul>
 * </p>
 * <p> Main additions in SpanQueryParser syntax vs. classic:
 * <ul>
 * <li> Can require "in order" for phrases with slop with the ~> operator: &quot;jakarta apache&quot;~>3</li>
 * <li> Can specify "not near" &quot;bieber fever&quot;!~3,10 ::
 * find &quot;bieber&quot; but not if &quot;fever&quot; appears within 3 words before or
 * 10 words after it.</li>
 * <li> Fully recursive phrasal queries with [ and ]; as in: [[jakarta apache]~3 lucene]~>4 :: 
 * find &quot;jakarta&quot; within 3 words of &quot;apache&quot;, and that hit has to be within four
 * words before &quot;lucene&quot;.</li>
 * <li> Can also use [] for single level phrasal queries instead of &quot;&quot; as in: [jakarta apache]</li>
 * <li> Can use &quot;or&quot; clauses in phrasal queries: &quot;apache (lucene solr)&quot;~3 :: 
 * find &quot;apache&quot; and then either &quot;lucene&quot; or &quot;solr&quot; within three words.
 * </li>
 * <li> Can use multiterms in phrasal queries: "jakarta~1 ap*che"~2</li>
 * <li> Did I mention recursion: [[jakarta~1 ap*che]~2 (solr~ /l[ou]+[cs][en]+/)]~10 ::
 * Find something like &quot;jakarta&quot; within two words of &quot;ap*che&quot; and that hit
 * has to be within ten words of something like &quot;solr&quot; or that lucene regex.</li>
 * <li> How about: &quot;fever (travlota~2 disco "saturday night" beeber~1)&quot;!~3,10 :: find fever but not if something like
 * travlota or disco or "saturday night" or something like beeber appears within 3 words before or 10 words after.</li>
 * <li> Can require at least x number of hits at boolean level: "apache AND (lucene solr tika)~2</li>
 * <li> Can have a negative query: -jakarta will return all documents that do not contain jakarta</li>
 * </ul>
 * </p>
 * <p>
 * Trivial additions:
 * <ul>
 * <li> Can specify prefix length in fuzzy queries: jakarta~1,2 (edit distance=1, prefix=2)</li>
 * <li> Can specify prefix Optimal String Alignment (OSA) vs Levenshtein 
 * in fuzzy queries: jakarta~1 (OSA) vs jakarta~>1 (Levenshtein)</li>
 * </ul>
 * 
 * <p> <b>Analysis</b>
 * You can specify different analyzers
 * to handle whole term versus multiterm components.
 * </p>
 * 
 * <p>
 * <b>Using quotes for a single term</b>
 * The default with SpanQueryParser is to use single quotes (Classic QueryParser uses double quotes):
 * 'abc~2' will be treated as a single term 'abc~2' not as a fuzzy term.
 * Remember to use quotes or use escapes for anything with backslashes or hyphens:
 * 12/02/04 (is broken into a term "12", a regex "/02/" and a term "04")
 * '12/02/04' is treated a a single token.
 * </p>
 * <p> <b>Stop word handling</b>
 * </p>
 * <p>The user can choose to throw a {@link org.apache.lucene.queryparser.classic.ParseException} if a stop word is encountered.
 * If SpanQueryParserBase.throwExceptionForEmptyTerm is set to false (default), the following should happen.
 * </p>
 * <p>
 * <ul>
 * <li>Term: "the" will return an empty SpanQuery (similar to classic queryparser)</li>
 * <li>BooleanOr: (the apache jakarta) will drop the stop word and return a 
 * {@link org.apache.lucene.search.spans.SpanOrQuery} for &quot;apache&quot; 
 * or &quot;jakarta&quot;
 * <li>SpanNear: "apache and jakarta" will drop the "and" and match on only "apache jakarta"<li>
 * </ul></p>
 * <p>A parse exception is currently always thrown if the parser analyzes a multiterm, and a subcomponent of the
 * multiterm has a stopword: the*tre
 * </p>
 * <p> Expert: Other subtle differences between SpanQueryParser and classic QueryParser.
 * <ul>
 * <li>Fuzzy queries with slop > 2 are handled by SlowFuzzyQuery.  The developer can set the minFuzzySim to limit
 * the maximum edit distance (i.e. turn off SlowFuzzyQuery by setting fuzzyMinSim = 2.0f.</li>
 * <li>Fuzzy queries with edit distance >=1 are rounded so that an exception is not thrown.</li>
 * </ul>
 * </p>
 * <p> Truly Expert: there are a few other very subtle differences that are documented in comments
 * in the sourcecode in the header of SpanQueryParser.
 * </p>
 * <p>
 * <b>NOTE</b> You must add the sandbox jar to your class path to include 
 * the currently deprecated {@link org.apache.lucene.sandbox.queries.SlowFuzzyQuery}.
 * </p>
 * <p> Limitations of SpanQueryParser compared with classic QueryParser:
 * <ol>
 * <li> There is some learning curve to figure out the subtle differences in syntax between
 * when one is within a phrase and when not. Including:
 * <ol>
 * <li>Boolean operators are not allowed within phrases: &quot;solr (apache AND lucene)&quot;.  
 *      Consider rewriting:[solr [apache lucene]]</li>
 * <li>Field information is not allowed within phrases.</li>
 * <li>Minimum hit counts for boolean "or" queries are not allowed within phrases: [apache (lucene solr tika)~2]</li>
 * </ol>
 * <li> This parser is not built with .jj or the antlr parser framework.  
 * Regrettably, because it is generating a {@link org.apache.lucene.search.spans.SpanQuery},
 * it can't use all of the generalizable queryparser infrastructure that was added with Lucene 4.+.</li>
 * </ol>
 * </p>
 */
public class SpanQueryParser extends AbstractSpanQueryParser {

  /*
   *  Some subtle differences between classic QueryParser and SpanQueryParser
   * 
   *  1) In a range query, this parser is not escaping terms.  So [12-02-03 TO 12-04-03] and
   *  [12/02/03 TO 12/04/03] need to be single-quoted: ['12-02-03' TO '12-04-03'].
   *  
   *  2) The SpanQueryParser does not recognize quotes as a way to escape non-regexes.
   *  In classic syntax a path string of "/abc/def/ghi" is denoted by the double quotes; in
   *  SpanQueryParser, the user has to escape the / as in \/abc\/def\/ghi or use single quotes:
   *  '/abc/def/ghi'
   *  
   *  3) "term^3~" is not handled.  Boosts must currently come after fuzzy mods in SpanQueryParser.
   *
   *  4) SpanQueryParser rounds fuzzy sims that are > 1.0.  This test fails: assertParseException("term~1.1")
   *  
   *  5) SpanQueryParser adds a small amount to its own floatToEdits calculation
   *     so that near exact percentages (e.g. 80% of a 5 char word should yield 1) 
   *     aren't floored and therefore miss.
   *     
   *     For SpanQueryParser, brwon~0.80 hits on "brown". 
   *     
   *  6) By using single-quote escaping, SpanQueryParser will pass issue raised
   *  by LUCENE-1189, which is a token with an odd number of \ ending in a phrasal boundary.
   *  
   *  The test case that was to prove a fix for LUCENE-1189 is slightly different than the original
   *  issue: \"(name:[///mike\\\\\\\") or (name:\"alphonse\")";
   *  
   *  8) SpanQueryParser does not convert regexes to lowercase as a default.  There is a
   *  separate parameter for whether or not to do this.  
   */  

  private static final int MAX_QUERY_LENGTH_CHARS = 30000;

  private String topLevelQueryString;

  public SpanQueryParser(Version matchVersion, String f, Analyzer a) {
    init(matchVersion, f, a);
  }

  public SpanQueryParser(Version matchVersion, String f, Analyzer a, Analyzer multitermAnalyzer) {
    init(matchVersion, f, a, multitermAnalyzer);
  }

  @Override
  public void ReInit(CharStream stream) {
    //this is crazy...convert string to char stream then back to string for processing.
    //The value from extending QueryParserBase was greater than this
    //bit of craziness.  This shouldn't actually ever be called.
    try {
      int i = 0;
      while(i++ <  MAX_QUERY_LENGTH_CHARS) {
        stream.readChar();
      }
    } catch (IOException e) {}
    topLevelQueryString = stream.GetImage();
  }

  @Override
  public Query TopLevelQuery(String field) throws ParseException {
    Query q = _parse(field);
    q = rewriteAllNegative(q);
    return q;
  }

  @Override
  public Query parse(String s) throws ParseException {
    topLevelQueryString = s;
    return TopLevelQuery(getField());
  }

  private Query _parse(String field) throws ParseException {
    if (topLevelQueryString == null || topLevelQueryString.equals("")) {
      return getEmptySpanQuery();
    }
    SpanQueryLexer lexer = new SpanQueryLexer();
    List<SQPToken> tokens = lexer.getTokens(topLevelQueryString);
    SQPClause overallClause = new SQPOrClause(0, tokens.size());
    return parseRecursively(tokens, field, overallClause);
  }

  private Query parseRecursively(final List<SQPToken> tokens, 
      String field, SQPClause clause) 
          throws ParseException {
    int start = clause.getTokenOffsetStart();
    int end = clause.getTokenOffsetEnd();
    testStartEnd(tokens, start, end);
    List<BooleanClause> clauses = new ArrayList<BooleanClause>();
    int conj = CONJ_NONE;
    int mods = MOD_NONE;
    String tmpField = field;
    int i = start;
    while (i < end) {
      Query q = null;
      SQPToken token = tokens.get(i);

      //if boolean operator or field, update local buffers and continue
      if (token instanceof SQPBooleanOpToken) {
        SQPBooleanOpToken t = (SQPBooleanOpToken)token;
        if (t.isConj()) {
          conj = t.getType();
          mods = MOD_NONE;
        } else {
          mods = t.getType();
        }
        i++;
        continue;
      } else if (token instanceof SQPField) {
        tmpField = ((SQPField)token).getField();
        i++;
        continue;
      }
      //if or clause, recur through tokens
      if (token instanceof SQPOrClause) {
        //recur!
        SQPOrClause tmpOr = (SQPOrClause)token;
        q = parseRecursively(tokens, tmpField, tmpOr);

        if (q instanceof BooleanQuery && tmpOr.getMinimumNumberShouldMatch() > 1) {
          ((BooleanQuery)q).setMinimumNumberShouldMatch(tmpOr.getMinimumNumberShouldMatch());
        }
        if (q.getBoost() == 1.0f
            &&  tmpOr.getBoost() != SpanQueryParserBase.UNSPECIFIED_BOOST) {
          q.setBoost(tmpOr.getBoost());
        }
        i = tmpOr.getTokenOffsetEnd();
      } else if (token instanceof SQPNearClause) {
        SQPNearClause tmpNear = (SQPNearClause)token;
        if (getAnalyzer(tmpField) == null) {
          q = parseNullAnalyzer(tmpField, tmpNear);
        } else {
          q = _parsePureSpanClause(tokens, tmpField, tmpNear);
        }
        i = tmpNear.getTokenOffsetEnd();
      } else if (token instanceof SQPNotNearClause) {
        SQPNotNearClause tmpNotNear = (SQPNotNearClause)token;
        q = _parsePureSpanClause(tokens, tmpField, tmpNotNear);
        i = tmpNotNear.getTokenOffsetEnd();
      } else if (token instanceof SQPTerminal) {

        SQPTerminal tmpTerm = (SQPTerminal)token;
        q = testAllDocs(tmpField, tmpTerm);
        if (q == null) {
          if (getAnalyzer(tmpField) == null) {
            if (tmpTerm instanceof SQPRangeTerm) {
              SQPRangeTerm r = (SQPRangeTerm)tmpTerm;
              q = handleNullAnalyzerRange(tmpField, r.getStart(), r.getEnd(), 
                  r.getStartInclusive(), r.getEndInclusive()); 
            } else {
              String t = tmpTerm.getString();
              if (testWildCardOrPrefix(t) == SpanQueryParserBase.PREFIX && t.length() > 0) {
                q = handleNullAnalyzerPrefix(tmpField, t.substring(0, t.length()-1));
              } else {
                q = handleNullAnalyzer(tmpField, tmpTerm.getString());
              }
            }
          } else {
            q = buildSpanTerminal(tmpField, tmpTerm);
          }
        }
        i++;
      } else {
        //throw exception because this could lead to an infinite loop
        //if a new token type is added but not properly accounted for.
        throw new IllegalArgumentException("Don't know how to process token of this type: " + token.getClass());
      }
      if (!isEmptyQuery(q)) {
        addClause(clauses, conj, mods, q);
      }
      //reset mods and conj and field
      mods = MOD_NONE;
      conj = CONJ_NONE;
      tmpField = field;
    }

    if (clauses.size() == 0) {
      return getEmptySpanQuery();
    }
    if (clauses.size() == 1 && 
        clauses.get(0).getOccur() != Occur.MUST_NOT) {
      return clauses.get(0).getQuery();
    }

    BooleanQuery bq = new BooleanQuery();
    try {
      for (BooleanClause bc : clauses) {
        bq.add(bc);
      }
    } catch (BooleanQuery.TooManyClauses e) {
      throw new ParseException(e.getMessage());
    }

    if (clause instanceof SQPOrClause) {
      SQPOrClause tmpClause = (SQPOrClause)clause;
      if (tmpClause.getMinimumNumberShouldMatch() > SQPOrClause.DEFAULT_MINIMUM_NUMBER_SHOULD_MATCH) {
        bq.setMinimumNumberShouldMatch(tmpClause.getMinimumNumberShouldMatch());
      }
    }

    return bq;
  }

  //This is necessary for Solr-ization or anywhere that someone might override getAnalyer(field)
  //and return a null Analyzer.  If the user enters something like: 
  //part_no:"some kind of part"
  // and part_no is a String field, we want to treat "some kind of part" as an unmodified literal string
  private Query parseNullAnalyzer(String field, SQPNearClause tmpNear) {
    String literalSpan = topLevelQueryString.substring(tmpNear.getCharStartOffset(), tmpNear.getCharEndOffset());
    return handleNullAnalyzer(field, literalSpan);
  }

  private Query testAllDocs(String tmpField, SQPTerminal tmpTerm) {
    if (tmpField.equals("*") && 
        tmpTerm instanceof SQPTerm && 
        ((SQPTerm)tmpTerm).getString().equals("*")) {
      return new MatchAllDocsQuery();
    }
    return null;
  }

  private void testStartEnd(List<SQPToken> tokens, int start, int end)
      throws ParseException {

    SQPToken s = tokens.get(start);
    if (s instanceof SQPBooleanOpToken) {
      int type = ((SQPBooleanOpToken)s).getType();
      if ( type == CONJ_AND || type == CONJ_OR) {
        throw new ParseException("Can't start clause with AND or OR");
      }
    }

    SQPToken e = tokens.get(end-1);

    if (e instanceof SQPField) {
      throw new ParseException("Can't end clause with a field token");
    }
    if (e instanceof SQPBooleanOpToken) {
      throw new ParseException("Can't end clause with a boolean operator");
    }
  }


  /**
   * Extracts the spans from the BooleanQueries that are not in Occur.NOT
   * clauses for highlighting. This query should not be used for document retrieval
   * and will likely return different documents than "parse."
   * 
   * @return SpanQuery for highlighting
   */
  public SpanQuery getHighlightQuery(String field, String queryString) throws ParseException {
    Query q = parse(queryString);
    List<SpanQuery> sqs = new ArrayList<SpanQuery>();
    extractSpanQueries(field, q, sqs);
    return buildSpanOrQuery(sqs);
  }

  /**
   * Takes a query generated by this parser and extracts all
   * SpanQueries into sqs that are not in a Boolean.Occur.NOT clause
   * and that match the given field.
   * 
   * The Query must consist of only BooleanQuery and SpanQuery objects!!!
   */
  private void extractSpanQueries(String field, Query query, List<SpanQuery> sqs) {
    if (query == null) {
      return;
    }
    if (query instanceof SpanQuery) {
      SpanQuery sq = (SpanQuery)query;
      if (! isEmptyQuery(sq) && 
          sq.getField().equals(field)) {
        sqs.add((SpanQuery)query);
      }
    } else if (query instanceof BooleanQuery) {
      BooleanQuery bq = (BooleanQuery)query;
      BooleanClause[] clauses = bq.getClauses();
      for (BooleanClause clause : clauses) {
        if (clause.getOccur() != Occur.MUST_NOT) {
          extractSpanQueries(field, clause.getQuery(), sqs);
        }
      }  
    } else {
      //ignore
    }
  }
  
  /**
   * If the query contains only Occur.MUST_NOT clauses,
   * this will add a MatchAllDocsQuery.
   * @return query
   */
  private Query rewriteAllNegative(Query q) {
    
    if (q instanceof BooleanQuery) {
      BooleanQuery bq = (BooleanQuery)q;
      BooleanClause[] clauses = bq.getClauses();
      if (clauses.length == 0) {
        return q;
      }
      for (BooleanClause clause : clauses) {
        if (! clause.getOccur().equals(Occur.MUST_NOT)) {
          //something other than must_not exists, stop here and return q
          return q;
        }
      }
      BooleanQuery ret  = bq.clone();
      ret.add(new MatchAllDocsQuery(), Occur.MUST);
      return ret;
    }
    return q;
  }
}
