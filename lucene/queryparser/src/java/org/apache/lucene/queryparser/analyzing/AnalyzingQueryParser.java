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
package org.apache.lucene.queryparser.analyzing;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Overrides Lucene's default QueryParser so that Fuzzy-, Prefix-, Range-, and WildcardQuerys
 * are also passed through the given analyzer, but wildcard characters <code>*</code> and
 * <code>?</code> don't get removed from the search terms.
 * 
 * <p><b>Warning:</b> This class should only be used with analyzers that do not use stopwords
 * or that add tokens. Also, several stemming analyzers are inappropriate: for example, GermanAnalyzer 
 * will turn <code>H&auml;user</code> into <code>hau</code>, but <code>H?user</code> will 
 * become <code>h?user</code> when using this parser and thus no match would be found (i.e.
 * using this parser will be no improvement over QueryParser in such cases). 
 */
public class AnalyzingQueryParser extends org.apache.lucene.queryparser.classic.QueryParser {
  // gobble escaped chars or find a wildcard character 
  private static final Pattern WILDCARD_PATTERN = Pattern.compile("(\\\\.)|([?*]+)");
  public AnalyzingQueryParser(String field, Analyzer analyzer) {
    super(field, analyzer);
    setAnalyzeRangeTerms(true);
  }

  /**
   * Called when parser parses an input term that contains one or more wildcard
   * characters (like <code>*</code>), but is not a prefix term (one that has
   * just a single <code>*</code> character at the end).
   * <p>
   * Example: will be called for <code>H?user</code> or for <code>H*user</code>.
   * <p>
   * Depending on analyzer and settings, a wildcard term may (most probably will)
   * be lower-cased automatically. It <b>will</b> go through the default Analyzer.
   * <p>
   * Overrides super class, by passing terms through analyzer.
   *
   * @param  field   Name of the field query will use.
   * @param  termStr Term that contains one or more wildcard
   *                 characters (? or *), but is not simple prefix term
   *
   * @return Resulting {@link Query} built for the term
   */
  @Override
  protected Query getWildcardQuery(String field, String termStr) throws ParseException {
    if ("*".equals(field)) {
      if ("*".equals(termStr)) return newMatchAllDocsQuery();
    }
    if (getAllowLeadingWildcard() == false && (termStr.startsWith("*") || termStr.startsWith("?")))
      throw new ParseException("'*' or '?' not allowed as first character in WildcardQuery");

    Term t = new Term(field, analyzeWildcard(field, termStr));
    return newWildcardQuery(t);
  }

  private BytesRef analyzeWildcard(String field, String termStr) {
    // best effort to not pass the wildcard characters and escaped characters through #normalize
    Matcher wildcardMatcher = WILDCARD_PATTERN.matcher(termStr);
    BytesRefBuilder sb = new BytesRefBuilder();
    int last = 0;

    while (wildcardMatcher.find()){
      if (wildcardMatcher.start() > 0) {
        String chunk = termStr.substring(last, wildcardMatcher.start());
        BytesRef normalized = getAnalyzer().normalize(field, chunk);
        sb.append(normalized);
      }
      //append the matched group - without normalizing
      sb.append(new BytesRef(wildcardMatcher.group()));

      last = wildcardMatcher.end();
    }
    if (last < termStr.length()){
      String chunk = termStr.substring(last);
      BytesRef normalized = getAnalyzer().normalize(field, chunk);
      sb.append(normalized);
    }
    return sb.toBytesRef();
  }

  /**
   * Called when parser parses an input term
   * that uses prefix notation; that is, contains a single '*' wildcard
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
   * @param  termStr Term to use for building term for the query
   *                 (<b>without</b> trailing '*' character!)
   *
   * @return Resulting {@link Query} built for the term
   */
  @Override
  protected Query getPrefixQuery(String field, String termStr) throws ParseException {
    if (!getAllowLeadingWildcard() && termStr.startsWith("*"))
      throw new ParseException("'*' not allowed as first character in PrefixQuery");
    if (getLowercaseExpandedTerms()) {
      termStr = termStr.toLowerCase(getLocale());
    }
    BytesRef term = getAnalyzer().normalize(field, termStr);
    Term t = new Term(field, term);
    return newPrefixQuery(t);
  }

  /**
   * Called when parser parses an input term that has the fuzzy suffix (~) appended.
   * <p>
   * Depending on analyzer and settings, a fuzzy term may (most probably will)
   * be lower-cased automatically. It <b>will</b> go through the default Analyzer.
   * <p>
   * Overrides super class, by passing terms through analyzer.
   *
   * @param field Name of the field query will use.
   * @param termStr Term to use for building term for the query
   *
   * @return Resulting {@link Query} built for the term
   */
  @Override
  protected Query getFuzzyQuery(String field, String termStr, float minSimilarity)
      throws ParseException {
   
    BytesRef term = getAnalyzer().normalize(field, termStr);
    Term t = new Term(field, term);
    return newFuzzyQuery(t, minSimilarity, getFuzzyPrefixLength());
  }

}
