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

import java.io.IOException;
import java.io.InputStream;

import opennlp.tools.lemmatizer.DictionaryLemmatizer;
import opennlp.tools.lemmatizer.LemmatizerME;
import opennlp.tools.lemmatizer.LemmatizerModel;

/**
 * <p>Supply OpenNLP Lemmatizer tools.</p>
 * <p>
 *   Both a dictionary-based lemmatizer and a MaxEnt lemmatizer are supported.
 *   If both are configured, the dictionary-based lemmatizer is tried first,
 *   and then the MaxEnt lemmatizer is consulted for out-of-vocabulary tokens.
 * </p>
 * <p>
 *   The MaxEnt implementation requires binary models from OpenNLP project on SourceForge.
 * </p>
 */
public class NLPLemmatizerOp {
  private final DictionaryLemmatizer dictionaryLemmatizer;
  private final LemmatizerME lemmatizerME;

  public NLPLemmatizerOp(InputStream dictionary, LemmatizerModel lemmatizerModel) throws IOException {
    assert dictionary != null || lemmatizerModel != null : "At least one parameter must be non-null";
    dictionaryLemmatizer = dictionary == null ? null : new DictionaryLemmatizer(dictionary);
    lemmatizerME = lemmatizerModel == null ? null : new LemmatizerME(lemmatizerModel);
  }

  public String[] lemmatize(String[] words, String[] postags) {
    String[] lemmas = null;
    String[] maxEntLemmas = null;
    if (dictionaryLemmatizer != null) {
      lemmas = dictionaryLemmatizer.lemmatize(words, postags);
      for (int i = 0; i < lemmas.length; ++i) {
        if (lemmas[i].equals("O")) {   // this word is not in the dictionary
          if (lemmatizerME != null) {  // fall back to the MaxEnt lemmatizer if it's enabled
            if (maxEntLemmas == null) {
              maxEntLemmas = lemmatizerME.lemmatize(words, postags);
            }
            if ("_".equals(maxEntLemmas[i])) {
              lemmas[i] = words[i];    // put back the original word if no lemma is found
            } else {
              lemmas[i] = maxEntLemmas[i];
            }
          } else {                     // there is no MaxEnt lemmatizer
            lemmas[i] = words[i];      // put back the original word if no lemma is found
          }
        }
      }
    } else {                           // there is only a MaxEnt lemmatizer
      maxEntLemmas = lemmatizerME.lemmatize(words, postags);
      for (int i = 0 ; i < maxEntLemmas.length ; ++i) {
        if ("_".equals(maxEntLemmas[i])) {
          maxEntLemmas[i] = words[i];  // put back the original word if no lemma is found
        }
      }
      lemmas = maxEntLemmas;
    }
    return lemmas;
  }
}
