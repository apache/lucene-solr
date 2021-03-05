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
import java.util.List;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.tika.language.LanguageIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Identifies the language of a set of input fields using Tika's
 * LanguageIdentifier.
 * The tika-core-x.y.jar must be on the classpath
 * <p>
 * See <a href="http://wiki.apache.org/solr/LanguageDetection">http://wiki.apache.org/solr/LanguageDetection</a>
 * @since 3.5
 */
public class TikaLanguageIdentifierUpdateProcessor extends LanguageIdentifierUpdateProcessor {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public TikaLanguageIdentifierUpdateProcessor(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(req, rsp, next);
  }

  @Override
  protected List<DetectedLanguage> detectLanguage(Reader solrDocReader) {
    String content = SolrInputDocumentReader.asString(solrDocReader);
    List<DetectedLanguage> languages = new ArrayList<>();
    if (content.length() != 0) {
      LanguageIdentifier identifier = new LanguageIdentifier(content);
      // FIXME: Hack - we get the distance from toString and calculate our own certainty score
      Double distance = Double.parseDouble(tikaSimilarityPattern.matcher(identifier.toString()).replaceFirst("$1"));
      // This formula gives: 0.02 => 0.8, 0.1 => 0.5 which is a better sweetspot than isReasonablyCertain()
      Double certainty = 1 - (5 * distance);
      if (certainty < 0)
        certainty = 0d;
      DetectedLanguage language = new DetectedLanguage(identifier.getLanguage(), certainty);
      languages.add(language);
      if (log.isDebugEnabled()) {
        log.debug("Language detected as {} with a certainty of {} (Tika distance={})"
            , language, language.getCertainty(), identifier);
      }
    } else {
      log.debug("No input text to detect language from, returning empty list");
    }
    return languages;
  }
}
