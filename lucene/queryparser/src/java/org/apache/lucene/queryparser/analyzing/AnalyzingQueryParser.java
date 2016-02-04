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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;

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
  private final Pattern wildcardPattern = Pattern.compile("(\\.)|([?*]+)");
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

    if (termStr == null){
      //can't imagine this would ever happen
      throw new ParseException("Passed null value as term to getWildcardQuery");
    }
    if ( ! getAllowLeadingWildcard() && (termStr.startsWith("*") || termStr.startsWith("?"))) {
      throw new ParseException("'*' or '?' not allowed as first character in WildcardQuery"
                              + " unless getAllowLeadingWildcard() returns true");
    }
    
    Matcher wildcardMatcher = wildcardPattern.matcher(termStr);
    StringBuilder sb = new StringBuilder();
    int last = 0;
  
    while (wildcardMatcher.find()){
      // continue if escaped char
      if (wildcardMatcher.group(1) != null){
        continue;
      }
     
      if (wildcardMatcher.start() > 0){
        String chunk = termStr.substring(last, wildcardMatcher.start());
        String analyzed = analyzeSingleChunk(field, termStr, chunk);
        sb.append(analyzed);
      }
      //append the wildcard character
      sb.append(wildcardMatcher.group(2));
     
      last = wildcardMatcher.end();
    }
    if (last < termStr.length()){
      sb.append(analyzeSingleChunk(field, termStr, termStr.substring(last)));
    }
    return super.getWildcardQuery(field, sb.toString());
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
    String analyzed = analyzeSingleChunk(field, termStr, termStr);
    return super.getPrefixQuery(field, analyzed);
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
   
    String analyzed = analyzeSingleChunk(field, termStr, termStr);
    return super.getFuzzyQuery(field, analyzed, minSimilarity);
  }

  /**
   * Returns the analyzed form for the given chunk
   * 
   * If the analyzer produces more than one output token from the given chunk,
   * a ParseException is thrown.
   *
   * @param field The target field
   * @param termStr The full term from which the given chunk is excerpted
   * @param chunk The portion of the given termStr to be analyzed
   * @return The result of analyzing the given chunk
   * @throws ParseException when analysis returns other than one output token
   */
  protected String analyzeSingleChunk(String field, String termStr, String chunk) throws ParseException{
    String analyzed = null;
    try (TokenStream stream = getAnalyzer().tokenStream(field, chunk)) {
      stream.reset();
      CharTermAttribute termAtt = stream.getAttribute(CharTermAttribute.class);
      // get first and hopefully only output token
      if (stream.incrementToken()) {
        analyzed = termAtt.toString();
        
        // try to increment again, there should only be one output token
        StringBuilder multipleOutputs = null;
        while (stream.incrementToken()) {
          if (null == multipleOutputs) {
            multipleOutputs = new StringBuilder();
            multipleOutputs.append('"');
            multipleOutputs.append(analyzed);
            multipleOutputs.append('"');
          }
          multipleOutputs.append(',');
          multipleOutputs.append('"');
          multipleOutputs.append(termAtt.toString());
          multipleOutputs.append('"');
        }
        stream.end();
        if (null != multipleOutputs) {
          throw new ParseException(
              String.format(getLocale(),
                  "Analyzer created multiple terms for \"%s\": %s", chunk, multipleOutputs.toString()));
        }
      } else {
        // nothing returned by analyzer.  Was it a stop word and the user accidentally
        // used an analyzer with stop words?
        stream.end();
        throw new ParseException(String.format(getLocale(), "Analyzer returned nothing for \"%s\"", chunk));
      }
    } catch (IOException e){
      throw new ParseException(
          String.format(getLocale(), "IO error while trying to analyze single term: \"%s\"", termStr));
    }
    return analyzed;
  }
}
