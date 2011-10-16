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
import java.util.List;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.tika.language.LanguageIdentifier;

/**
 * Identifies the language of a set of input fields using Tika's
 * LanguageIdentifier.
 * The tika-core-x.y.jar must be on the classpath
 * <p>
 * See <a href="http://wiki.apache.org/solr/LanguageDetection">http://wiki.apache.org/solr/LanguageDetection</a>
 * @since 3.5
 */
public class TikaLanguageIdentifierUpdateProcessor extends LanguageIdentifierUpdateProcessor {

  public TikaLanguageIdentifierUpdateProcessor(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(req, rsp, next);
  }
  
  @Override
  protected List<DetectedLanguage> detectLanguage(String content) {
    List<DetectedLanguage> languages = new ArrayList<DetectedLanguage>();
    if(content.trim().length() != 0) { 
      LanguageIdentifier identifier = new LanguageIdentifier(content);
      // FIXME: Hack - we get the distance from toString and calculate our own certainty score
      Double distance = Double.parseDouble(tikaSimilarityPattern.matcher(identifier.toString()).replaceFirst("$1"));
      // This formula gives: 0.02 => 0.8, 0.1 => 0.5 which is a better sweetspot than isReasonablyCertain()
      Double certainty = 1 - (5 * distance); 
      certainty = (certainty < 0) ? 0 : certainty;
      DetectedLanguage language = new DetectedLanguage(identifier.getLanguage(), certainty);
      languages.add(language);
      log.debug("Language detected as "+language+" with a certainty of "+language.getCertainty()+" (Tika distance="+identifier.toString()+")");
    } else {
      log.debug("No input text to detect language from, returning empty list");
    }
    return languages;
  }
}
