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
package org.apache.solr.highlight;

import java.util.regex.Pattern;

import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.NullFragmenter;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * {@link org.apache.lucene.search.highlight.Fragmenter} that tries to produce snippets that "look" like a regular 
 * expression.
 *
 * <code>solrconfig.xml</code> parameters:
 * <ul>
 * <li><code>hl.regex.pattern</code>: regular expression corresponding to "nice" fragments.</li>
 * <li><code>hl.regex.slop</code>: how far the fragmenter can stray from the ideal fragment size.
       A slop of 0.2 means that the fragmenter can go over or under by 20%.</li>
 * <li><code>hl.regex.maxAnalyzedChars</code>: how many characters to apply the
       regular expression to (independent from the global highlighter setting).</li>
 * </ul>
 *
 * NOTE: the default for <code>maxAnalyzedChars</code> is much lower for this 
 * fragmenter.  After this limit is exhausted, fragments are produced in the
 * same way as <code>GapFragmenter</code>
 */

public class RegexFragmenter extends HighlightingPluginBase implements SolrFragmenter
{
  protected String defaultPatternRaw;
  protected Pattern defaultPattern;

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    super.init(args);
    defaultPatternRaw = LuceneRegexFragmenter.DEFAULT_PATTERN_RAW;
    if( defaults != null ) {
      defaultPatternRaw = defaults.get(HighlightParams.PATTERN, LuceneRegexFragmenter.DEFAULT_PATTERN_RAW);      
    }
    defaultPattern = Pattern.compile(defaultPatternRaw);
  }

  @Override
  public Fragmenter getFragmenter(String fieldName, SolrParams params )
  { 
    numRequests.inc();
    params = SolrParams.wrapDefaults(params, defaults);

    int fragsize  = params.getFieldInt(   fieldName, HighlightParams.FRAGSIZE,  LuceneRegexFragmenter.DEFAULT_FRAGMENT_SIZE );
    int increment = params.getFieldInt(   fieldName, HighlightParams.INCREMENT, LuceneRegexFragmenter.DEFAULT_INCREMENT_GAP );
    float slop    = params.getFieldFloat( fieldName, HighlightParams.SLOP,      LuceneRegexFragmenter.DEFAULT_SLOP );
    int maxchars  = params.getFieldInt(   fieldName, HighlightParams.MAX_RE_CHARS, LuceneRegexFragmenter.DEFAULT_MAX_ANALYZED_CHARS );
    String rawpat = params.getFieldParam( fieldName, HighlightParams.PATTERN,   LuceneRegexFragmenter.DEFAULT_PATTERN_RAW );

    Pattern p = rawpat == defaultPatternRaw ? defaultPattern : Pattern.compile(rawpat);

    if( fragsize <= 0 ) {
      return new NullFragmenter();
    }
    
    return new LuceneRegexFragmenter( fragsize, increment, slop, maxchars, p );
  }
  

  ///////////////////////////////////////////////////////////////////////
  //////////////////////// SolrInfoMBeans methods ///////////////////////
  ///////////////////////////////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "RegexFragmenter (" + defaultPatternRaw + ")";
  }
}
