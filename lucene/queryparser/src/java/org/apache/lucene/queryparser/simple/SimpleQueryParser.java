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
package org.apache.lucene.queryparser.simple;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.util.automaton.LevenshteinAutomata;

import java.util.Collections;
import java.util.Map;

/**
 * SimpleQueryParser is used to parse human readable query syntax.
 * <p>
 * The main idea behind this parser is that a person should be able to type
 * whatever they want to represent a query, and this parser will do its best
 * to interpret what to search for no matter how poorly composed the request
 * may be. Tokens are considered to be any of a term, phrase, or subquery for the
 * operations described below.  Whitespace including ' ' '\n' '\r' and '\t'
 * and certain operators may be used to delimit tokens ( ) + | " .
 * <p>
 * Any errors in query syntax will be ignored and the parser will attempt
 * to decipher what it can; however, this may mean odd or unexpected results.
 * <p>
 * <b>Query Operators</b>
 * <ul>
 *  <li>'{@code +}' specifies {@code AND} operation: <code>token1+token2</code>
 *  <li>'{@code |}' specifies {@code OR} operation: <code>token1|token2</code>
 *  <li>'{@code -}' negates a single token: <code>-token0</code>
 *  <li>'{@code "}' creates phrases of terms: <code>"term1 term2 ..."</code>
 *  <li>'{@code *}' at the end of terms specifies prefix query: <code>term*</code>
 *  <li>'{@code ~}N' at the end of terms specifies fuzzy query: <code>term~1</code>
 *  <li>'{@code ~}N' at the end of phrases specifies near query: <code>"term1 term2"~5</code>
 *  <li>'{@code (}' and '{@code )}' specifies precedence: <code>token1 + (token2 | token3)</code>
 * </ul>
 * <p>
 * The {@link #setDefaultOperator default operator} is {@code OR} if no other operator is specified.
 * For example, the following will {@code OR} {@code token1} and {@code token2} together:
 * <code>token1 token2</code>
 * <p>
 * Normal operator precedence will be simple order from right to left.
 * For example, the following will evaluate {@code token1 OR token2} first,
 * then {@code AND} with {@code token3}:
 * <blockquote>token1 | token2 + token3</blockquote>
 * <b>Escaping</b>
 * <p>
 * An individual term may contain any possible character with certain characters
 * requiring escaping using a '{@code \}'.  The following characters will need to be escaped in
 * terms and phrases:
 * {@code + | " ( ) ' \}
 * <p>
 * The '{@code -}' operator is a special case.  On individual terms (not phrases) the first
 * character of a term that is {@code -} must be escaped; however, any '{@code -}' characters
 * beyond the first character do not need to be escaped.
 * For example:
 * <ul>
 *   <li>{@code -term1}   -- Specifies {@code NOT} operation against {@code term1}
 *   <li>{@code \-term1}  -- Searches for the term {@code -term1}.
 *   <li>{@code term-1}   -- Searches for the term {@code term-1}.
 *   <li>{@code term\-1}  -- Searches for the term {@code term-1}.
 * </ul>
 * <p>
 * The '{@code *}' operator is a special case. On individual terms (not phrases) the last
 * character of a term that is '{@code *}' must be escaped; however, any '{@code *}' characters
 * before the last character do not need to be escaped:
 * <ul>
 *   <li>{@code term1*}  --  Searches for the prefix {@code term1}
 *   <li>{@code term1\*} --  Searches for the term {@code term1*}
 *   <li>{@code term*1}  --  Searches for the term {@code term*1}
 *   <li>{@code term\*1} --  Searches for the term {@code term*1}
 * </ul>
 * <p>
 * Note that above examples consider the terms before text processing.
 */
public class SimpleQueryParser extends QueryBuilder {
  /** Map of fields to query against with their weights */
  protected final Map<String,Float> weights;
  /** flags to the parser (to turn features on/off) */
  protected final int flags;

  /** Enables {@code AND} operator (+) */
  public static final int AND_OPERATOR         = 1<<0;
  /** Enables {@code NOT} operator (-) */
  public static final int NOT_OPERATOR         = 1<<1;
  /** Enables {@code OR} operator (|) */
  public static final int OR_OPERATOR          = 1<<2;
  /** Enables {@code PREFIX} operator (*) */
  public static final int PREFIX_OPERATOR      = 1<<3;
  /** Enables {@code PHRASE} operator (") */
  public static final int PHRASE_OPERATOR      = 1<<4;
  /** Enables {@code PRECEDENCE} operators: {@code (} and {@code )} */
  public static final int PRECEDENCE_OPERATORS = 1<<5;
  /** Enables {@code ESCAPE} operator (\) */
  public static final int ESCAPE_OPERATOR      = 1<<6;
  /** Enables {@code WHITESPACE} operators: ' ' '\n' '\r' '\t' */
  public static final int WHITESPACE_OPERATOR  = 1<<7;
  /** Enables {@code FUZZY} operators: (~) on single terms */
  public static final int FUZZY_OPERATOR       = 1<<8;
  /** Enables {@code NEAR} operators: (~) on phrases */
  public static final int NEAR_OPERATOR        = 1<<9;


  private BooleanClause.Occur defaultOperator = BooleanClause.Occur.SHOULD;

  /** Creates a new parser searching over a single field. */
  public SimpleQueryParser(Analyzer analyzer, String field) {
    this(analyzer, Collections.singletonMap(field, 1.0F));
  }

  /** Creates a new parser searching over multiple fields with different weights. */
  public SimpleQueryParser(Analyzer analyzer, Map<String, Float> weights) {
    this(analyzer, weights, -1);
  }

  /** Creates a new parser with custom flags used to enable/disable certain features. */
  public SimpleQueryParser(Analyzer analyzer, Map<String, Float> weights, int flags) {
    super(analyzer);
    this.weights = weights;
    this.flags = flags;
  }

  /** Parses the query text and returns parsed query */
  public Query parse(String queryText) {
    if ("*".equals(queryText.trim())) {
      return new MatchAllDocsQuery();
    }

    char data[] = queryText.toCharArray();
    char buffer[] = new char[data.length];

    State state = new State(data, buffer, 0, data.length);
    parseSubQuery(state);
    if (state.top == null) {
      return new MatchNoDocsQuery("empty string passed to query parser");
    } else {
      return state.top;
    }
  }

  private void parseSubQuery(State state) {
    while (state.index < state.length) {
      if (state.data[state.index] == '(' && (flags & PRECEDENCE_OPERATORS) != 0) {
        // the beginning of a subquery has been found
        consumeSubQuery(state);
      } else if (state.data[state.index] == ')' && (flags & PRECEDENCE_OPERATORS) != 0) {
        // this is an extraneous character so it is ignored
        ++state.index;
      } else if (state.data[state.index] == '"' && (flags & PHRASE_OPERATOR) != 0) {
        // the beginning of a phrase has been found
        consumePhrase(state);
      } else if (state.data[state.index] == '+' && (flags & AND_OPERATOR) != 0) {
        // an and operation has been explicitly set
        // if an operation has already been set this one is ignored
        // if a term (or phrase or subquery) has not been found yet the
        // operation is also ignored since there is no previous
        // term (or phrase or subquery) to and with
        if (state.currentOperation == null && state.top != null) {
          state.currentOperation = BooleanClause.Occur.MUST;
        }

        ++state.index;
      } else if (state.data[state.index] == '|' && (flags & OR_OPERATOR) != 0) {
        // an or operation has been explicitly set
        // if an operation has already been set this one is ignored
        // if a term (or phrase or subquery) has not been found yet the
        // operation is also ignored since there is no previous
        // term (or phrase or subquery) to or with
        if (state.currentOperation == null && state.top != null) {
          state.currentOperation = BooleanClause.Occur.SHOULD;
        }

        ++state.index;
      } else if (state.data[state.index] == '-' && (flags & NOT_OPERATOR) != 0) {
        // a not operator has been found, so increase the not count
        // two not operators in a row negate each other
        ++state.not;
        ++state.index;

        // continue so the not operator is not reset
        // before the next character is determined
        continue;
      } else if ((state.data[state.index] == ' '
          || state.data[state.index] == '\t'
          || state.data[state.index] == '\n'
          || state.data[state.index] == '\r') && (flags & WHITESPACE_OPERATOR) != 0) {
        // ignore any whitespace found as it may have already been
        // used a delimiter across a term (or phrase or subquery)
        // or is simply extraneous
        ++state.index;
      } else {
        // the beginning of a token has been found
        consumeToken(state);
      }

      // reset the not operator as even whitespace is not allowed when
      // specifying the not operation for a term (or phrase or subquery)
      state.not = 0;
    }
  }

  private void consumeSubQuery(State state) {
    assert (flags & PRECEDENCE_OPERATORS) != 0;
    int start = ++state.index;
    int precedence = 1;
    boolean escaped = false;

    while (state.index < state.length) {
      if (!escaped) {
        if (state.data[state.index] == '\\' && (flags & ESCAPE_OPERATOR) != 0) {
          // an escape character has been found so
          // whatever character is next will become
          // part of the subquery unless the escape
          // character is the last one in the data
          escaped = true;
          ++state.index;

          continue;
        } else if (state.data[state.index] == '(') {
          // increase the precedence as there is a
          // subquery in the current subquery
          ++precedence;
        } else if (state.data[state.index] == ')') {
          --precedence;

          if (precedence == 0) {
            // this should be the end of the subquery
            // all characters found will used for
            // creating the subquery
            break;
          }
        }
      }

      escaped = false;
      ++state.index;
    }

    if (state.index == state.length) {
      // a closing parenthesis was never found so the opening
      // parenthesis is considered extraneous and will be ignored
      state.index = start;
    } else if (state.index == start) {
      // a closing parenthesis was found immediately after the opening
      // parenthesis so the current operation is reset since it would
      // have been applied to this subquery
      state.currentOperation = null;

      ++state.index;
    } else {
      // a complete subquery has been found and is recursively parsed by
      // starting over with a new state object
      State subState = new State(state.data, state.buffer, start, state.index);
      parseSubQuery(subState);
      buildQueryTree(state, subState.top);

      ++state.index;
    }
  }

  private void consumePhrase(State state) {
    assert (flags & PHRASE_OPERATOR) != 0;
    int start = ++state.index;
    int copied = 0;
    boolean escaped = false;
    boolean hasSlop = false;

    while (state.index < state.length) {
      if (!escaped) {
        if (state.data[state.index] == '\\' && (flags & ESCAPE_OPERATOR) != 0) {
          // an escape character has been found so
          // whatever character is next will become
          // part of the phrase unless the escape
          // character is the last one in the data
          escaped = true;
          ++state.index;

          continue;
        } else if (state.data[state.index] == '"') {
          // if there are still characters after the closing ", check for a
          // tilde
          if (state.length > (state.index + 1) &&
              state.data[state.index+1] == '~' &&
              (flags & NEAR_OPERATOR) != 0) {
            state.index++;
            // check for characters after the tilde
            if (state.length > (state.index + 1)) {
              hasSlop = true;
            }
            break;
          } else {
            // this should be the end of the phrase
            // all characters found will used for
            // creating the phrase query
            break;
          }
        }
      }

      escaped = false;
      state.buffer[copied++] = state.data[state.index++];
    }

    if (state.index == state.length) {
      // a closing double quote was never found so the opening
      // double quote is considered extraneous and will be ignored
      state.index = start;
    } else if (state.index == start) {
      // a closing double quote was found immediately after the opening
      // double quote so the current operation is reset since it would
      // have been applied to this phrase
      state.currentOperation = null;

      ++state.index;
    } else {
      // a complete phrase has been found and is parsed through
      // through the analyzer from the given field
      String phrase = new String(state.buffer, 0, copied);
      Query branch;
      if (hasSlop) {
        branch = newPhraseQuery(phrase, parseFuzziness(state));
      } else {
        branch = newPhraseQuery(phrase, 0);
      }
      buildQueryTree(state, branch);

      ++state.index;
    }
  }

  private void consumeToken(State state) {
    int copied = 0;
    boolean escaped = false;
    boolean prefix = false;
    boolean fuzzy = false;

    while (state.index < state.length) {
      if (!escaped) {
        if (state.data[state.index] == '\\' && (flags & ESCAPE_OPERATOR) != 0) {
          // an escape character has been found so
          // whatever character is next will become
          // part of the term unless the escape
          // character is the last one in the data
          escaped = true;
          prefix = false;
          ++state.index;

          continue;
        } else if (tokenFinished(state)) {
          // this should be the end of the term
          // all characters found will used for
          // creating the term query
          break;
        } else if (copied > 0 && state.data[state.index] == '~' && (flags & FUZZY_OPERATOR) != 0) {
          fuzzy = true;
          break;
        }

        // wildcard tracks whether or not the last character
        // was a '*' operator that hasn't been escaped
        // there must be at least one valid character before
        // searching for a prefixed set of terms
        prefix = copied > 0 && state.data[state.index] == '*' && (flags & PREFIX_OPERATOR) != 0;
      }

      escaped = false;
      state.buffer[copied++] = state.data[state.index++];
    }

    if (copied > 0) {
      final Query branch;

      if (fuzzy && (flags & FUZZY_OPERATOR) != 0) {
        String token = new String(state.buffer, 0, copied);
        int fuzziness = parseFuzziness(state);
        // edit distance has a maximum, limit to the maximum supported
        fuzziness = Math.min(fuzziness, LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE);
        if (fuzziness == 0) {
          branch = newDefaultQuery(token);
        } else {
          branch = newFuzzyQuery(token, fuzziness);
        }
      } else if (prefix) {
        // if a term is found with a closing '*' it is considered to be a prefix query
        // and will have prefix added as an option
        String token = new String(state.buffer, 0, copied - 1);
        branch = newPrefixQuery(token);
      } else {
        // a standard term has been found so it will be run through
        // the entire analysis chain from the specified schema field
        String token = new String(state.buffer, 0, copied);
        branch = newDefaultQuery(token);
      }

      buildQueryTree(state, branch);
    }
  }

  private static BooleanQuery addClause(BooleanQuery bq, Query query, BooleanClause.Occur occur) {
    BooleanQuery.Builder newBq = new BooleanQuery.Builder();
    newBq.setMinimumNumberShouldMatch(bq.getMinimumNumberShouldMatch());
    for (BooleanClause clause : bq) {
      newBq.add(clause);
    }
    newBq.add(query, occur);
    return newBq.build();
  }

  // buildQueryTree should be called after a term, phrase, or subquery
  // is consumed to be added to our existing query tree
  // this method will only add to the existing tree if the branch contained in state is not null
  private void buildQueryTree(State state, Query branch) {
    if (branch != null) {
      // modify our branch to a BooleanQuery wrapper for not
      // this is necessary any time a term, phrase, or subquery is negated
      if (state.not % 2 == 1) {
        BooleanQuery.Builder nq = new BooleanQuery.Builder();
        nq.add(branch, BooleanClause.Occur.MUST_NOT);
        nq.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        branch = nq.build();
      }

      // first term (or phrase or subquery) found and will begin our query tree
      if (state.top == null) {
        state.top = branch;
      } else {
        // more than one term (or phrase or subquery) found
        // set currentOperation to the default if no other operation is explicitly set
        if (state.currentOperation == null) {
          state.currentOperation = defaultOperator;
        }

        // operational change requiring a new parent node
        // this occurs if the previous operation is not the same as current operation
        // because the previous operation must be evaluated separately to preserve
        // the proper precedence and the current operation will take over as the top of the tree
        if (state.previousOperation != state.currentOperation) {
          BooleanQuery.Builder bq = new BooleanQuery.Builder();
          bq.add(state.top, state.currentOperation);
          state.top = bq.build();
        }

        // reset all of the state for reuse
        state.top = addClause((BooleanQuery) state.top, branch, state.currentOperation);
        state.previousOperation = state.currentOperation;
      }

      // reset the current operation as it was intended to be applied to
      // the incoming term (or phrase or subquery) even if branch was null
      // due to other possible errors
      state.currentOperation = null;
    }
  }

  /**
   * Helper parsing fuzziness from parsing state
   * @return slop/edit distance, 0 in the case of non-parsing slop/edit string
   */
  private int parseFuzziness(State state) {
    char slopText[] = new char[state.length];
    int slopLength = 0;

    if (state.data[state.index] == '~') {
      while (state.index < state.length) {
        state.index++;
        // it's possible that the ~ was at the end, so check after incrementing
        // to make sure we don't go out of bounds
        if (state.index < state.length) {
          if (tokenFinished(state)) {
            break;
          }
          slopText[slopLength] = state.data[state.index];
          slopLength++;
        }
      }
      int fuzziness = 0;
      try {
        String fuzzyString =  new String(slopText, 0, slopLength);
        if ("".equals(fuzzyString)) {
          // Use automatic fuzziness, ~2
          fuzziness = 2;
        } else {
          fuzziness = Integer.parseInt(fuzzyString);
        }
      } catch (NumberFormatException e) {
        // swallow number format exceptions parsing fuzziness
      }
      // negative -> 0
      if (fuzziness < 0) {
        fuzziness = 0;
      }
      return fuzziness;
    }
    return 0;
  }

  /**
   * Helper returning true if the state has reached the end of token.
   */
  private boolean tokenFinished(State state) {
    if ((state.data[state.index] == '"' && (flags & PHRASE_OPERATOR) != 0)
        || (state.data[state.index] == '|' && (flags & OR_OPERATOR) != 0)
        || (state.data[state.index] == '+' && (flags & AND_OPERATOR) != 0)
        || (state.data[state.index] == '(' && (flags & PRECEDENCE_OPERATORS) != 0)
        || (state.data[state.index] == ')' && (flags & PRECEDENCE_OPERATORS) != 0)
        || ((state.data[state.index] == ' '
        || state.data[state.index] == '\t'
        || state.data[state.index] == '\n'
        || state.data[state.index] == '\r') && (flags & WHITESPACE_OPERATOR) != 0)) {
      return true;
    }
    return false;
  }

  /**
   * Factory method to generate a standard query (no phrase or prefix operators).
   */
  protected Query newDefaultQuery(String text) {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (Map.Entry<String,Float> entry : weights.entrySet()) {
      Query q = createBooleanQuery(entry.getKey(), text, defaultOperator);
      if (q != null) {
        float boost = entry.getValue();
        if (boost != 1f) {
          q = new BoostQuery(q, boost);
        }
        bq.add(q, BooleanClause.Occur.SHOULD);
      }
    }
    return simplify(bq.build());
  }

  /**
   * Factory method to generate a fuzzy query.
   */
  protected Query newFuzzyQuery(String text, int fuzziness) {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (Map.Entry<String,Float> entry : weights.entrySet()) {
      final String fieldName = entry.getKey();
      final BytesRef term = getAnalyzer().normalize(fieldName, text);
      Query q = new FuzzyQuery(new Term(fieldName, term), fuzziness);
      float boost = entry.getValue();
      if (boost != 1f) {
        q = new BoostQuery(q, boost);
      }
      bq.add(q, BooleanClause.Occur.SHOULD);
    }
    return simplify(bq.build());
  }

  /**
   * Factory method to generate a phrase query with slop.
   */
  protected Query newPhraseQuery(String text, int slop) {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (Map.Entry<String,Float> entry : weights.entrySet()) {
      Query q = createPhraseQuery(entry.getKey(), text, slop);
      if (q != null) {
        float boost = entry.getValue();
        if (boost != 1f) {
          q = new BoostQuery(q, boost);
        }
        bq.add(q, BooleanClause.Occur.SHOULD);
      }
    }
    return simplify(bq.build());
  }

  /**
   * Factory method to generate a prefix query.
   */
  protected Query newPrefixQuery(String text) {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (Map.Entry<String,Float> entry : weights.entrySet()) {
      final String fieldName = entry.getKey();
      final BytesRef term = getAnalyzer().normalize(fieldName, text);
      Query q = new PrefixQuery(new Term(fieldName, term));
      float boost = entry.getValue();
      if (boost != 1f) {
        q = new BoostQuery(q, boost);
      }
      bq.add(q, BooleanClause.Occur.SHOULD);
    }
    return simplify(bq.build());
  }

  /**
   * Helper to simplify boolean queries with 0 or 1 clause
   */
  protected Query simplify(BooleanQuery bq) {
    if (bq.clauses().isEmpty()) {
      return null;
    } else if (bq.clauses().size() == 1) {
      return bq.clauses().iterator().next().getQuery();
    } else {
      return bq;
    }
  }

  /**
   * Returns the implicit operator setting, which will be
   * either {@code SHOULD} or {@code MUST}.
   */
  public BooleanClause.Occur getDefaultOperator() {
    return defaultOperator;
  }

  /**
   * Sets the implicit operator setting, which must be
   * either {@code SHOULD} or {@code MUST}.
   */
  public void setDefaultOperator(BooleanClause.Occur operator) {
    if (operator != BooleanClause.Occur.SHOULD && operator != BooleanClause.Occur.MUST) {
      throw new IllegalArgumentException("invalid operator: only SHOULD or MUST are allowed");
    }
    this.defaultOperator = operator;
  }

  static class State {
    final char[] data;   // the characters in the query string
    final char[] buffer; // a temporary buffer used to reduce necessary allocations
    int index;
    int length;

    BooleanClause.Occur currentOperation;
    BooleanClause.Occur previousOperation;
    int not;

    Query top;

    State(char[] data, char[] buffer, int index, int length) {
      this.data = data;
      this.buffer = buffer;
      this.index = index;
      this.length = length;
    }
  }
}
