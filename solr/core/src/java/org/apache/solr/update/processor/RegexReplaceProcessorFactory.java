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
package org.apache.solr.update.processor;

import org.apache.solr.core.SolrCore;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.regex.PatternSyntaxException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An updated processor that applies a configured regex to any 
 * CharSequence values found in the selected fields, and replaces 
 * any matches with the configured replacement string
 * <p>
 * By default this processor applies itself to no fields.
 * </p>
 *
 * <p>
 * For example, with the configuration listed below, any sequence of multiple 
 * whitespace characters found in values for field named <code>title</code> 
 * or <code>content</code> will be replaced by a single space character.
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.RegexReplaceProcessorFactory"&gt;
 *   &lt;str name="fieldName"&gt;content&lt;/str&gt;
 *   &lt;str name="fieldName"&gt;title&lt;/str&gt;
 *   &lt;str name="pattern"&gt;\s+&lt;/str&gt;
 *   &lt;str name="replacement"&gt; &lt;/str&gt;
 * &lt;/processor&gt;</pre>
 * 
 * @see java.util.regex.Pattern
 */
public final class RegexReplaceProcessorFactory extends FieldMutatingUpdateProcessorFactory {
  
  private static final Logger log = LoggerFactory.getLogger(RegexReplaceProcessorFactory.class);

  private static final String REPLACEMENT_PARAM = "replacement";
  private static final String PATTERN_PARAM = "pattern";
  
  private Pattern pattern;
  private String replacement;

  @SuppressWarnings("unchecked")
  @Override
  public void init(NamedList args) {

    Object patternParam = args.remove(PATTERN_PARAM);

    if(patternParam == null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, 
                              "Missing required init parameter: " + PATTERN_PARAM);
    }
    
    try {
      pattern = Pattern.compile(patternParam.toString());      
    } catch (PatternSyntaxException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, 
                              "Invalid regex: " + patternParam, e);
    }                                

    Object replacementParam = args.remove(REPLACEMENT_PARAM);
    if(replacementParam == null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, 
                              "Missing required init parameter: " + REPLACEMENT_PARAM);
    }
    replacement = Matcher.quoteReplacement(replacementParam.toString());

    super.init(args);
  }

  /** 
   * @see FieldMutatingUpdateProcessor#SELECT_NO_FIELDS
   */
  @Override
  protected FieldMutatingUpdateProcessor.FieldNameSelector 
    getDefaultSelector(final SolrCore core) {

    return FieldMutatingUpdateProcessor.SELECT_NO_FIELDS;

  }
  
  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest request,
                                            SolrQueryResponse response,
                                            UpdateRequestProcessor next) {
    return new FieldValueMutatingUpdateProcessor(getSelector(), next) {
      @Override
      protected Object mutateValue(final Object src) {
        if (src instanceof CharSequence) {
          CharSequence txt = (CharSequence)src;
          return pattern.matcher(txt).replaceAll(replacement);
        }
        return src;
      }
    };
  }
}
