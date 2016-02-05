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

/**
 * Bean holding a language and a detection certainty 
 */
public class DetectedLanguage {
  private final String langCode;
  private final Double certainty;
  
  DetectedLanguage(String lang, Double certainty) {
    this.langCode = lang;
    this.certainty = certainty;
  }
  
  /**
   * Returns the detected language code
   * @return language code as a string
   */
  public String getLangCode() {
    return langCode;
  }

  /**
   * Returns the detected certainty for this language
   * @return certainty as a value between 0.0 and 1.0 where 1.0 is 100% certain
   */
  public Double getCertainty() {
    return certainty;
  }
}
