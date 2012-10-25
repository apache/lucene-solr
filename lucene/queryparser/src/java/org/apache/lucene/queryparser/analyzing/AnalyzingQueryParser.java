package org.apache.lucene.queryparser.analyzing;

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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

/**
 * Overrides Lucene's default QueryParser so that Fuzzy-, Prefix-, Range-, and WildcardQuerys
 * are also passed through the given analyzer, but wild card characters (like <code>*</code>) 
 * don't get removed from the search terms.
 * 
 * <p><b>Warning:</b> This class should only be used with analyzers that do not use stopwords
 * or that add tokens. Also, several stemming analyzers are inappropriate: for example, GermanAnalyzer 
 * will turn <code>H&auml;user</code> into <code>hau</code>, but <code>H?user</code> will 
 * become <code>h?user</code> when using this parser and thus no match would be found (i.e.
 * using this parser will be no improvement over QueryParser in such cases). 
 *
 */
public class AnalyzingQueryParser extends org.apache.lucene.queryparser.classic.QueryParser {

  /**
   * Constructs a query parser.
   * @param field    the default field for query terms.
   * @param analyzer used to find terms in the query text.
   */
  public AnalyzingQueryParser(Version matchVersion, String field, Analyzer analyzer) {
    super(matchVersion, field, analyzer);
    setAnalyzeRangeTerms(true);
  }

  /**
   * Called when parser
   * parses an input term token that contains one or more wildcard
   * characters (like <code>*</code>), but is not a prefix term token (one
   * that has just a single * character at the end).
   * <p>
   * Example: will be called for <code>H?user</code> or for <code>H*user</code> 
   * but not for <code>*user</code>.
   * <p>
   * Depending on analyzer and settings, a wildcard term may (most probably will)
   * be lower-cased automatically. It <b>will</b> go through the default Analyzer.
   * <p>
   * Overrides super class, by passing terms through analyzer.
   *
   * @param  field   Name of the field query will use.
   * @param  termStr Term token that contains one or more wild card
   *                 characters (? or *), but is not simple prefix term
   *
   * @return Resulting {@link Query} built for the term
   */
  @Override
  protected Query getWildcardQuery(String field, String termStr) throws ParseException {
    List<String> tlist = new ArrayList<String>();
    List<String> wlist = new ArrayList<String>();
    /* somewhat a hack: find/store wildcard chars
     * in order to put them back after analyzing */
    boolean isWithinToken = (!termStr.startsWith("?") && !termStr.startsWith("*"));
    StringBuilder tmpBuffer = new StringBuilder();
    char[] chars = termStr.toCharArray();
    for (int i = 0; i < termStr.length(); i++) {
      if (chars[i] == '?' || chars[i] == '*') {
        if (isWithinToken) {
          tlist.add(tmpBuffer.toString());
          tmpBuffer.setLength(0);
        }
        isWithinToken = false;
      } else {
        if (!isWithinToken) {
          wlist.add(tmpBuffer.toString());
          tmpBuffer.setLength(0);
        }
        isWithinToken = true;
      }
      tmpBuffer.append(chars[i]);
    }
    if (isWithinToken) {
      tlist.add(tmpBuffer.toString());
    } else {
      wlist.add(tmpBuffer.toString());
    }

    // get Analyzer from superclass and tokenize the term
    TokenStream source;
    
    int countTokens = 0;
    try {
      source = getAnalyzer().tokenStream(field, new StringReader(termStr));
      source.reset();
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
    CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);
    while (true) {
      try {
        if (!source.incrementToken()) break;
      } catch (IOException e) {
        break;
      }
      String term = termAtt.toString();
      if (!"".equals(term)) {
        try {
          tlist.set(countTokens++, term);
        } catch (IndexOutOfBoundsException ioobe) {
          countTokens = -1;
        }
      }
    }
    try {
      source.end();
      source.close();
    } catch (IOException e) {
      // ignore
    }

    if (countTokens != tlist.size()) {
      /* this means that the analyzer used either added or consumed 
       * (common for a stemmer) tokens, and we can't build a WildcardQuery */
      throw new ParseException("Cannot build WildcardQuery with analyzer "
          + getAnalyzer().getClass() + " - tokens added or lost");
    }

    if (tlist.size() == 0) {
      return null;
    } else if (tlist.size() == 1) {
      if (wlist != null && wlist.size() == 1) {
        /* if wlist contains one wildcard, it must be at the end, because:
         * 1) wildcards are not allowed in 1st position of a term by QueryParser
         * 2) if wildcard was *not* in end, there would be *two* or more tokens */
        return super.getWildcardQuery(field, tlist.get(0)
            + wlist.get(0).toString());
      } else {
        /* we should never get here! if so, this method was called
         * with a termStr containing no wildcard ... */
        throw new IllegalArgumentException("getWildcardQuery called without wildcard");
      }
    } else {
      /* the term was tokenized, let's rebuild to one token
       * with wildcards put back in postion */
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < tlist.size(); i++) {
        sb.append( tlist.get(i));
        if (wlist != null && wlist.size() > i) {
          sb.append(wlist.get(i));
        }
      }
      return super.getWildcardQuery(field, sb.toString());
    }
  }

  /**
   * Called when parser parses an input term
   * token that uses prefix notation; that is, contains a single '*' wildcard
   * character as its last character. Since this is a special case
   * of generic wildcard term, and such a query can be optimized easily,
   * this usually results in a different query object.
   * <p>
   * Depending on analyzer and settings, a prefix term may (most probably will)
   * be lower-cased automatically. It <b>will</b> go through the default Analyzer.
   * <p>
   * Overrides super class, by passing terms through analyzer.
   *
   * @param  field   Name of the field query will use.
   * @param  termStr Term token to use for building term for the query
   *                 (<b>without</b> trailing '*' character!)
   *
   * @return Resulting {@link Query} built for the term
   */
  @Override
  protected Query getPrefixQuery(String field, String termStr) throws ParseException {
    // get Analyzer from superclass and tokenize the term
    TokenStream source;
    List<String> tlist = new ArrayList<String>();
    try {
      source = getAnalyzer().tokenStream(field, new StringReader(termStr));
      source.reset();
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
    CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);
    while (true) {
      try {
        if (!source.incrementToken()) break;
      } catch (IOException e) {
        break;
      }
      tlist.add(termAtt.toString());
    }

    try {
      source.end();
      source.close();
    } catch (IOException e) {
      // ignore
    }

    if (tlist.size() == 1) {
      return super.getPrefixQuery(field, tlist.get(0));
    } else {
      /* this means that the analyzer used either added or consumed
       * (common for a stemmer) tokens, and we can't build a PrefixQuery */
      throw new ParseException("Cannot build PrefixQuery with analyzer "
          + getAnalyzer().getClass()
          + (tlist.size() > 1 ? " - token(s) added" : " - token consumed"));
    }
  }

  /**
   * Called when parser parses an input term token that has the fuzzy suffix (~) appended.
   * <p>
   * Depending on analyzer and settings, a fuzzy term may (most probably will)
   * be lower-cased automatically. It <b>will</b> go through the default Analyzer.
   * <p>
   * Overrides super class, by passing terms through analyzer.
   *
   * @param field Name of the field query will use.
   * @param termStr Term token to use for building term for the query
   *
   * @return Resulting {@link Query} built for the term
   */
  @Override
  protected Query getFuzzyQuery(String field, String termStr, float minSimilarity)
      throws ParseException {
    // get Analyzer from superclass and tokenize the term
    TokenStream source = null;
    String nextToken = null;
    boolean multipleTokens = false;
    
    try {
      source = getAnalyzer().tokenStream(field, new StringReader(termStr));
      CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);
      source.reset();
      if (source.incrementToken()) {
        nextToken = termAtt.toString();
      }
      multipleTokens = source.incrementToken();
    } catch (IOException e) {
      nextToken = null;
    }

    try {
      source.end();
      source.close();
    } catch (IOException e) {
      // ignore
    }

    if (multipleTokens) {
      throw new ParseException("Cannot build FuzzyQuery with analyzer " + getAnalyzer().getClass()
          + " - tokens were added");
    }

    return (nextToken == null) ? null : super.getFuzzyQuery(field, nextToken, minSimilarity);
  }
}
