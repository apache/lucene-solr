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

import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.cybozu.labs.langdetect.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Identifies the language of a set of input fields using http://code.google.com/p/language-detection
 * <p>
 * See <a href="http://wiki.apache.org/solr/LanguageDetection">http://wiki.apache.org/solr/LanguageDetection</a>
 * @since 3.5
 */
public class LangDetectLanguageIdentifierUpdateProcessor extends LanguageIdentifierUpdateProcessor {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public LangDetectLanguageIdentifierUpdateProcessor(SolrQueryRequest req, 
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(req, rsp, next);
  }

  /**
   * Detects language(s) from a reader, typically based on some fields in SolrInputDocument
   * Classes wishing to implement their own language detection module should override this method.
   *
   * @param solrDocReader A reader serving the text from the document to detect
   * @return List of detected language(s) according to RFC-3066
   */
  @Override
  protected List<DetectedLanguage> detectLanguage(Reader solrDocReader) {
    try {
      Detector detector = DetectorFactory.create();
      detector.setMaxTextLength(maxTotalChars);

      // TODO Work around bug in LangDetect 1.1 which does not expect a -1 return value at end of stream,
      // but instead only looks at ready()
      if (solrDocReader instanceof SolrInputDocumentReader) {
        ((SolrInputDocumentReader)solrDocReader).setEodReturnValue(0);
      }
      detector.append(solrDocReader);

      ArrayList<Language> langlist = detector.getProbabilities();
      ArrayList<DetectedLanguage> solrLangList = new ArrayList<>();
      for (Language l: langlist) {
        solrLangList.add(new DetectedLanguage(l.lang, l.prob));
      }
      return solrLangList;
    } catch (LangDetectException e) {
      log.debug("Could not determine language, returning empty list: ", e);
      return Collections.emptyList();
    } catch (IOException e) {
      log.warn("Could not determine language.", e);
      return Collections.emptyList();
    }
  }
}
