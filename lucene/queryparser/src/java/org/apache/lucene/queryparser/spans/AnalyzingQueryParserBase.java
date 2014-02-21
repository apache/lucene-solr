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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

/**
 * Enables setting different analyzers for whole term vs. 
 * multiTerm (wildcard, fuzzy, prefix).
 * <p>
 * To set different analyzers per field, use PerFieldAnalyzerWrapper.
 * This class also has hooks to allow subclassing to enable different
 * strategies of per field analyzer handling.
 */
abstract class AnalyzingQueryParserBase extends QueryParserBase {

  public enum NORM_MULTI_TERMS {
    ANALYZE,
    LOWERCASE,
    NONE
  };

  private NORM_MULTI_TERMS normMultiTerms = NORM_MULTI_TERMS.LOWERCASE;

  private static final Pattern WILDCARD_PATTERN = Pattern.compile("(?s)(\\\\.)|([?*]+)");

  private Analyzer multiTermAnalyzer;
  
  /**
   * Default initialization. The analyzer is used for both whole terms and multiTerms.
   */
  @Override
  public void init(Version matchVersion, String f, Analyzer a) {
    super.init(matchVersion, f, a);
    this.multiTermAnalyzer = a;
  }

  /**
   * Expert.  Set a different analyzer for whole terms vs. multiTerm subcomponents.
   * <p> 
   * Warning: this initializer has a side effect of setting normMultiTerms = NORM_MULTI_TERMS.ANALYZE
   */
  public void init(Version matchVersion, String f, Analyzer a, Analyzer multiTermAnalyzer) {
    super.init(matchVersion, f, a);
    this.multiTermAnalyzer = multiTermAnalyzer;  
    setNormMultiTerms(NORM_MULTI_TERMS.ANALYZE);
  }

  //TODO: make this protected in QueryParserBase and then override it
  //modify to throw only parse exception

  /**
   * Notionally overrides functionality from analyzeMultitermTerm.  Differences 
   * are that this consumes the full tokenstream, and it throws ParseException
   * if it encounters no content terms or more than one.
   * 
   * If getMultitermAnalyzer(String fieldName) returns null, 
   * this returns "part" unaltered.
   * @return bytesRef to term part
   */
  protected BytesRef analyzeMultitermTermParseEx(String field, String part) throws ParseException {
    //TODO: Modify QueryParserBase, analyzeMultiTerm doesn't currently consume all tokens, and it 
    //throws RuntimeExceptions and IllegalArgumentExceptions instead of parse.
    //Otherwise this is copied verbatim.  
    TokenStream source;

    Analyzer multiTermAnalyzer = getMultiTermAnalyzer(field);
    if (multiTermAnalyzer == null) {
      return new BytesRef(part);
    }

    try {
      source = multiTermAnalyzer.tokenStream(field, part);
      source.reset();
    } catch (IOException e) {
      throw new ParseException("Unable to initialize TokenStream to analyze multiTerm term: " + part);
    }

    TermToBytesRefAttribute termAtt = source.getAttribute(TermToBytesRefAttribute.class);
    BytesRef bytes = termAtt.getBytesRef();

    int partCount = 0;
    try {
      if (!source.incrementToken()) {

      } else {
        partCount++;
        termAtt.fillBytesRef();
        while (source.incrementToken()) {
          partCount++;
        }

      }
    } catch (IOException e1) {
      throw new RuntimeException("IO error analyzing multiterm: " + part);
    }

    try {
      source.end();
      source.close();
    } catch (IOException e) {
      throw new RuntimeException("Unable to end & close TokenStream after analyzing multiTerm term: " + part);
    }
    if (partCount != 1) {
      throw new ParseException("Couldn't find any content in >"+ part+"<");
    }
    return BytesRef.deepCopyOf(bytes);
  }

  /**
   * Analysis of wildcards is a bit tricky.  This splits a term by wildcard
   * and then analyzes the subcomponents.
   * 
   * @return analyzed wildcard
   */
  protected String analyzeWildcard(String field, String termText) throws ParseException {    
    // plagiarized from AnalyzingQueryParser
    Matcher wildcardMatcher = WILDCARD_PATTERN.matcher(termText);
    StringBuilder sb = new StringBuilder();
    int last = 0;

    while (wildcardMatcher.find()) {
      // continue if escaped char
      if (wildcardMatcher.group(1) != null) {
        continue;
      }

      if (wildcardMatcher.start() > 0) {
        String chunk = termText.substring(last, wildcardMatcher.start());
        BytesRef analyzed = analyzeMultitermTermParseEx(field, chunk);
        sb.append(analyzed.utf8ToString());
      }
      // append the wildcard character
      sb.append(wildcardMatcher.group(2));

      last = wildcardMatcher.end();
    }
    if (last < termText.length()) {
      sb.append(analyzeMultitermTermParseEx(field, termText.substring(last)).utf8ToString());
    }
    return sb.toString();
  }


  /**
   * If set to true, normMultiTerms is set to NORM_MULTI_TERMS.LOWERCASE.
   * If set to false, this turns off all normalization and sets normMultiTerms to NORM_MULTI_TERMS.NONE.
   * 
   * @deprecated use {@link #setNormMultiTerms(NORM_MULTI_TERMS)}
   */
  @Override
  @Deprecated
  public void setLowercaseExpandedTerms(boolean lc) {
    if (lc == true) {
      normMultiTerms = NORM_MULTI_TERMS.LOWERCASE;
    } else {
      normMultiTerms = NORM_MULTI_TERMS.NONE;
    }
    super.setLowercaseExpandedTerms(lc);
  }

  /**
   * Returns true if normMultiTerms == NORM_MULTI_TERMS.LOWERCASE
   * @deprecated use {@link #getNormMultiTerms()}
   */
  @Override
  @Deprecated
  public boolean getLowercaseExpandedTerms() {
    if (normMultiTerms == NORM_MULTI_TERMS.LOWERCASE) {
      return true;
    }
    return false;
  }

  /**
   * In this base class, this simply returns 
   * the {@link #multiTermAnalyzer} no matter the value of fieldName.
   * This is useful as a hook for overriding.
   * 
   * @return analyzer to use for multiTerms
   */
  public Analyzer getMultiTermAnalyzer(String fieldName) {
    return multiTermAnalyzer;
  }

  /**
   * Simply returns {@link #getAnalyzer()} no matter the value of fieldName.
   * This is meant as a hook for overriding.
   * 
   * @return analyzer to use for full terms
   */
  public Analyzer getAnalyzer(String fieldName) {
    return getAnalyzer();
  }

  /**
   * 
   * @return type of normalization to perform on multiTerms
   */
  public NORM_MULTI_TERMS getNormMultiTerms() {
    return normMultiTerms;
  }

  public void setNormMultiTerms(NORM_MULTI_TERMS norm) {
    this.normMultiTerms = norm;
    //TODO: get rid of these side effects once deprecated setLowercaseExpandedTerms is gone.
    //These are currently needed because (at least) regexp creation
    //is driven by QueryParserBase, which still relies on these.
    if (norm == NORM_MULTI_TERMS.LOWERCASE) {
      setLowercaseExpandedTerms(true);
    } else if (norm == NORM_MULTI_TERMS.NONE) {
      setLowercaseExpandedTerms(false);
    }
  }
}
