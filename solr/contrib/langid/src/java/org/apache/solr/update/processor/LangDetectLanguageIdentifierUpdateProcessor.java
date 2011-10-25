package org.apache.solr.update.processor;

/**
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
import java.util.Collections;
import java.util.List;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.cybozu.labs.langdetect.Language;

/**
 * Identifies the language of a set of input fields using http://code.google.com/p/language-detection
 * <p>
 * See <a href="http://wiki.apache.org/solr/LanguageDetection">http://wiki.apache.org/solr/LanguageDetection</a>
 * @since 3.5
 */
public class LangDetectLanguageIdentifierUpdateProcessor extends LanguageIdentifierUpdateProcessor {
  
  public LangDetectLanguageIdentifierUpdateProcessor(SolrQueryRequest req, 
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(req, rsp, next);
  }

  @Override
  protected List<DetectedLanguage> detectLanguage(String content) {
    if (content.trim().length() == 0) { // to be consistent with the tika impl?
      log.debug("No input text to detect language from, returning empty list");
      return Collections.emptyList();
    }
    
    try {
      Detector detector = DetectorFactory.create();
      detector.append(content);
      ArrayList<Language> langlist = detector.getProbabilities();
      ArrayList<DetectedLanguage> solrLangList = new ArrayList<DetectedLanguage>();
      for (Language l: langlist) {
        solrLangList.add(new DetectedLanguage(l.lang, l.prob));
      }
      return solrLangList;
    } catch (LangDetectException e) {
      log.debug("Could not determine language, returning empty list: ", e);
      return Collections.emptyList();
    }
  }
}
