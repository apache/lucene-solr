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

import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;

/**
 * Identifies the language of a set of input fields using <a href="https://opennlp.apache.org/">Apache OpenNLP</a>.
 * <p>
 * See "Language Detector" section of
 * <a href="https://opennlp.apache.org/docs/1.8.3/manual/opennlp.html">https://opennlp.apache.org/docs/1.8.3/manual/opennlp.html</a>
 */
public class OpenNLPLangDetectUpdateProcessor extends LanguageIdentifierUpdateProcessor {

  private final LanguageDetectorModel model;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Maps ISO 639-3 (3-letter language code) to ISO 639-1 (2-letter language code) */
  private static final Map<String,String> ISO639_MAP = make_ISO639_map();
  
  public OpenNLPLangDetectUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp,
      UpdateRequestProcessor next, LanguageDetectorModel model) {
    super(req, rsp, next);
    this.model = model;
  }

  @Override
  protected List<DetectedLanguage> detectLanguage(Reader solrDocReader) {
    List<DetectedLanguage> languages = new ArrayList<>();
    String content = SolrInputDocumentReader.asString(solrDocReader);
    if (content.length() != 0) {
      LanguageDetectorME ldme = new LanguageDetectorME(model);
      Language[] langs = ldme.predictLanguages(content);
      for(Language language: langs){
        languages.add(new DetectedLanguage(ISO639_MAP.get(language.getLang()), language.getConfidence()));
      }
    } else {
      log.debug("No input text to detect language from, returning empty list");
    }
    return languages;
  }

  private static Map<String,String> make_ISO639_map() {
    Map<String,String> map = new HashMap<>();
    for (String lang : Locale.getISOLanguages()) {
      Locale locale = new Locale(lang);
      map.put(locale.getISO3Language(), locale.getLanguage());
    }
    return map;
  }
}
