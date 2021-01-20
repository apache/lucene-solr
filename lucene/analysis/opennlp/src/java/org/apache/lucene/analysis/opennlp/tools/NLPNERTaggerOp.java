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

package org.apache.lucene.analysis.opennlp.tools;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinder;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;

/**
 * Supply OpenNLP Named Entity Resolution tool Requires binary models from OpenNLP project on
 * SourceForge.
 *
 * <p>Usage: from <a
 * href="http://opennlp.apache.org/docs/1.8.3/manual/opennlp.html#tools.namefind.recognition.api"
 * >the OpenNLP documentation</a>:
 *
 * <p>"The NameFinderME class is not thread safe, it must only be called from one thread. To use
 * multiple threads multiple NameFinderME instances sharing the same model instance can be created.
 * The input text should be segmented into documents, sentences and tokens. To perform entity
 * detection an application calls the find method for every sentence in the document. After every
 * document clearAdaptiveData must be called to clear the adaptive data in the feature generators.
 * Not calling clearAdaptiveData can lead to a sharp drop in the detection rate after a few
 * documents."
 */
public class NLPNERTaggerOp {
  private final TokenNameFinder nameFinder;

  public NLPNERTaggerOp(TokenNameFinderModel model) {
    this.nameFinder = new NameFinderME(model);
  }

  public Span[] getNames(String[] words) {
    Span[] names = nameFinder.find(words);
    return names;
  }

  public synchronized void reset() {
    nameFinder.clearAdaptiveData();
  }
}
