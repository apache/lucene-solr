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

import java.util.ArrayList;
import java.util.List;


import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;

abstract class AbstractSpanQueryParser extends SpanQueryParserBase {

  @Override
  abstract public Query parse(String s) throws ParseException;

  /**
   * Recursively called to parse a span query
   * <p>
   * This assumes that there are no FIELD tokens and no BOOLEAN operators
   */
  protected SpanQuery _parsePureSpanClause(final List<SQPToken> tokens, 
      String field, SQPClause parentClause) 
          throws ParseException {

    int start = parentClause.getTokenOffsetStart();
    int end = parentClause.getTokenOffsetEnd();

    //test if special handling needed for spannear with one component?
    if (end-start == 1) {

      if (parentClause instanceof SQPNearClause) {
        SQPNearClause nc = (SQPNearClause)parentClause;
        SQPToken t = tokens.get(start);
        if (t instanceof SQPTerm) {

          SpanQuery ret = trySpecialHandlingForSpanNearWithOneComponent(field, (SQPTerm)t, nc);
          if (ret != null) {
            if (parentClause.getBoost() != SpanQueryParserBase.UNSPECIFIED_BOOST) {
              ret.setBoost(parentClause.getBoost());
            }
            return ret;
          }
        }
      }
    }

    List<SpanQuery> queries = new ArrayList<SpanQuery>();
    int i = start;
    while (i < end) {
      SQPToken t = tokens.get(i);
      SpanQuery q = null;
      if (t instanceof SQPClause) {
        SQPClause c = (SQPClause)t;
        q = _parsePureSpanClause(tokens, field, c);
        i = c.getTokenOffsetEnd();
      } else if (t instanceof SQPTerminal) {
        q = buildSpanTerminal(field, (SQPTerminal)t);
        i++;
      } else {
        throw new ParseException("Can't process field, boolean operators or a match all docs query in a pure span.");
      }
      if (q != null) {
        queries.add(q);
      }
    }
    if (queries == null || queries.size() == 0) {
      return getEmptySpanQuery();
    }
    return buildSpanQueryClause(queries, parentClause);
  }   

  private SpanQuery trySpecialHandlingForSpanNearWithOneComponent(String field, 
      SQPTerm token, SQPNearClause clause) 
          throws ParseException {

    int slop = (clause.getSlop() == SpanQueryParserBase.UNSPECIFIED_SLOP) ? getPhraseSlop() : clause.getSlop();
    boolean order = clause.getInOrder() == null ? true : clause.getInOrder().booleanValue();

    SpanQuery ret = (SpanQuery)specialHandlingForSpanNearWithOneComponent(field, 
        token.getString(), slop, order);
    return ret;

  }

  protected SpanQuery buildSpanTerminal(String field, SQPTerminal token) throws ParseException {
    Query q = null;
    if (token instanceof SQPRegexTerm) {
      q = getRegexpQuery(field, ((SQPRegexTerm)token).getString());
    } else if (token instanceof SQPTerm) {
      q = buildAnySingleTermQuery(field, ((SQPTerm)token).getString(), ((SQPTerm)token).isQuoted());
    } else if (token instanceof SQPRangeTerm) {
      SQPRangeTerm rt = (SQPRangeTerm)token;
      q = getRangeQuery(field, rt.getStart(), rt.getEnd(), 
          rt.getStartInclusive(), rt.getEndInclusive());
    }
    if (q != null && token instanceof SQPBoostableToken) {
      float boost = ((SQPBoostableToken)token).getBoost();
      if (boost != SpanQueryParserBase.UNSPECIFIED_BOOST) {
        q.setBoost(boost);
      } 
    }
    if (q != null && q instanceof SpanQuery) {
      return (SpanQuery)q;
    }
    return null;
  }

  private SpanQuery buildSpanQueryClause(List<SpanQuery> queries, SQPClause clause) 
      throws ParseException {
    SpanQuery q = null;
    if (clause instanceof SQPOrClause) {
      q = buildSpanOrQuery(queries);
    } else if (clause instanceof SQPNearClause) {

      int slop = ((SQPNearClause)clause).getSlop();
      if (slop == UNSPECIFIED_SLOP) {
        slop = getPhraseSlop();
      }

      Boolean inOrder = ((SQPNearClause)clause).getInOrder();
      boolean order = false;
      if (inOrder == null) {
        order = slop > 0 ? false : true; 
      } else {
        order = inOrder.booleanValue();
      }
      q = buildSpanNearQuery(queries, 
          slop, order);
    } else if (clause instanceof SQPNotNearClause) {
      q = buildSpanNotNearQuery(queries, 
          ((SQPNotNearClause)clause).getNotPre(), 
          ((SQPNotNearClause)clause).getNotPost());

    } else {
      //throw early and loudly. This should never happen.
      throw new IllegalArgumentException("clause not recognized: "+clause.getClass());
    }

    if (clause.getBoost() != UNSPECIFIED_BOOST) {
      q.setBoost(clause.getBoost());      
    }
    //now update boost if clause only had one child
    if (q.getBoost() == UNSPECIFIED_BOOST && 
        clause.getBoost() != UNSPECIFIED_BOOST && (
            q instanceof SpanTermQuery ||
            q instanceof SpanMultiTermQueryWrapper)) {
      q.setBoost(clause.getBoost());
    }

    return q;
  }
}
