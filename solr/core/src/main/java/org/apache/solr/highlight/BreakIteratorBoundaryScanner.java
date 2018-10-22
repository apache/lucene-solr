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

import java.text.BreakIterator;
import java.util.Locale;

import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;

public class BreakIteratorBoundaryScanner extends SolrBoundaryScanner {

  @Override
  protected BoundaryScanner get(String fieldName, SolrParams params) {
    // construct Locale
    String language = params.getFieldParam(fieldName, HighlightParams.BS_LANGUAGE);
    String country = params.getFieldParam(fieldName, HighlightParams.BS_COUNTRY);
    if(country != null && language == null){
      throw new SolrException(ErrorCode.BAD_REQUEST,
          HighlightParams.BS_LANGUAGE + " parameter cannot be null when you specify " + HighlightParams.BS_COUNTRY);
    }
    Locale locale = null;
    if(language != null){
      locale = country == null ? new Locale(language) : new Locale(language, country);
    } else {
      locale = Locale.ROOT;
    }

    // construct BreakIterator
    String type = params.getFieldParam(fieldName, HighlightParams.BS_TYPE, "WORD").toLowerCase(Locale.ROOT);
    BreakIterator bi = null;
    if(type.equals("character")){
      bi = BreakIterator.getCharacterInstance(locale);
    }
    else if(type.equals("word")){
      bi = BreakIterator.getWordInstance(locale);
    }
    else if(type.equals("line")){
      bi = BreakIterator.getLineInstance(locale);
    }
    else if(type.equals("sentence")){
      bi = BreakIterator.getSentenceInstance(locale);
    }
    else
      throw new SolrException(ErrorCode.BAD_REQUEST, type + " is invalid for parameter " + HighlightParams.BS_TYPE);

    return new org.apache.lucene.search.vectorhighlight.BreakIteratorBoundaryScanner(bi);
  }


  ///////////////////////////////////////////////////////////////////////
  //////////////////////// SolrInfoMBeans methods ///////////////////////
  ///////////////////////////////////////////////////////////////////////
  
  @Override
  public String getDescription() {
    return "BreakIteratorBoundaryScanner";
  }
}
