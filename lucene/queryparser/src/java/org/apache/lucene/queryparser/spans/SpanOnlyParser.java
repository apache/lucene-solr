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
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.CharStream;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.Version;

/**
 * This is a toy class that enables easy testing of the span only
 * parsing components.  This does not handle boolean operators (AND, NOT, OR, +/-), 
 * and it does not handle multiple fields.  It also doesn't handle MatchAllDocsQueries.
 * <p>
 * However, it does guarantee that a SpanQuery is returned.
 * <p>
 * The functionality of this class was the initial offering in LUCENE-5205.
 * 
 *
 * @see SpanQueryParser
 */
public class SpanOnlyParser extends AbstractSpanQueryParser{


  private static final int MAX_QUERY_LENGTH_CHARS = 30000;


  private String topLevelQueryString = "";

  public SpanOnlyParser(Version matchVersion, String f, Analyzer a) {
    init(matchVersion, f, a);
  }

  public SpanOnlyParser(Version matchVersion, String f, Analyzer a, Analyzer multitermAnalyzer) {
    init(matchVersion, f, a, multitermAnalyzer);
  }

  @Override
  public Query parse(String s) throws ParseException{
    topLevelQueryString = s;
    Query q = TopLevelQuery(getField());
    assert(q == null || q instanceof SpanQuery);
    return q;
  }

  @Override
  public void ReInit(CharStream stream) {
    //this is crazy...convert string to char stream then back to string for processing
    //the value from extending QueryParserBase was greater than this
    //bit of craziness.
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

    return _parsePureSpan(field, topLevelQueryString);
  }


  protected Query _parsePureSpan(String field, String queryString) throws ParseException{
    SpanQueryLexer lexer = new SpanQueryLexer();
    List<SQPToken> tokens = lexer.getTokens(topLevelQueryString);
    SQPClause overallClause = new SQPOrClause(0, tokens.size());
    return _parsePureSpanClause(tokens, field, overallClause);
  }
}
