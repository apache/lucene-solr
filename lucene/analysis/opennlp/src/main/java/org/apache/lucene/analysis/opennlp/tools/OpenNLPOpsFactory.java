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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import opennlp.tools.chunker.ChunkerModel;
import opennlp.tools.lemmatizer.LemmatizerModel;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerModel;
import org.apache.lucene.analysis.util.ResourceLoader;

/**
 * Supply OpenNLP Named Entity Recognizer
 * Cache model file objects. Assumes model files are thread-safe.
 */
public class OpenNLPOpsFactory {
  private static Map<String,SentenceModel> sentenceModels = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String,TokenizerModel> tokenizerModels = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String,POSModel> posTaggerModels = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String,ChunkerModel> chunkerModels = new ConcurrentHashMap<>();
  private static Map<String,TokenNameFinderModel> nerModels = new ConcurrentHashMap<>();
  private static Map<String,LemmatizerModel> lemmatizerModels = new ConcurrentHashMap<>();
  private static Map<String,String> lemmaDictionaries = new ConcurrentHashMap<>();

  public static NLPSentenceDetectorOp getSentenceDetector(String modelName) throws IOException {
    if (modelName != null) {
      SentenceModel model = sentenceModels.get(modelName);
      return new NLPSentenceDetectorOp(model);
    } else {
      return new NLPSentenceDetectorOp();
    }
  }

  public static SentenceModel getSentenceModel(String modelName, ResourceLoader loader) throws IOException {
    SentenceModel model = sentenceModels.get(modelName);
    if (model == null) {
      try (InputStream resource = loader.openResource(modelName)) {
        model = new SentenceModel(resource);
      }
      sentenceModels.put(modelName, model);
    }
    return model;
  }

  public static NLPTokenizerOp getTokenizer(String modelName) throws IOException {
    if (modelName == null) {
      return new NLPTokenizerOp();
    } else {
      TokenizerModel model = tokenizerModels.get(modelName);
      return new NLPTokenizerOp(model);
    }
  }

  public static TokenizerModel getTokenizerModel(String modelName, ResourceLoader loader) throws IOException {
    TokenizerModel model = tokenizerModels.get(modelName);
    if (model == null) {
      try (InputStream resource = loader.openResource(modelName)) {
        model = new TokenizerModel(resource);
      }
      tokenizerModels.put(modelName, model);
    }
    return model;
  }

  public static NLPPOSTaggerOp getPOSTagger(String modelName) throws IOException {
    POSModel model = posTaggerModels.get(modelName);
    return new NLPPOSTaggerOp(model);
  }

  public static POSModel getPOSTaggerModel(String modelName, ResourceLoader loader) throws IOException {
    POSModel model = posTaggerModels.get(modelName);
    if (model == null) {
      try (InputStream resource = loader.openResource(modelName)) {
        model = new POSModel(resource);
      }
      posTaggerModels.put(modelName, model);
    }
    return model;
  }

  public static NLPChunkerOp getChunker(String modelName) throws IOException {
    ChunkerModel model = chunkerModels.get(modelName);
    return new NLPChunkerOp(model);
  }

  public static ChunkerModel getChunkerModel(String modelName, ResourceLoader loader) throws IOException {
    ChunkerModel model = chunkerModels.get(modelName);
    if (model == null) {
      try (InputStream resource = loader.openResource(modelName)) {
        model = new ChunkerModel(resource);
      }
      chunkerModels.put(modelName, model);
    }
    return model;
  }

  public static NLPNERTaggerOp getNERTagger(String modelName) throws IOException {
    TokenNameFinderModel model = nerModels.get(modelName);
    return new NLPNERTaggerOp(model);
  }

  public static TokenNameFinderModel getNERTaggerModel(String modelName, ResourceLoader loader) throws IOException {
    TokenNameFinderModel model = nerModels.get(modelName);
    if (model == null) {
      try (InputStream resource = loader.openResource(modelName)) {
        model = new TokenNameFinderModel(resource);
      }
      nerModels.put(modelName, model);
    }
    return model;
  }

  public static NLPLemmatizerOp getLemmatizer(String dictionaryFile, String lemmatizerModelFile) throws IOException {
    assert dictionaryFile != null || lemmatizerModelFile != null : "At least one parameter must be non-null";
    InputStream dictionaryInputStream = null;
    if (dictionaryFile != null) {
      String dictionary = lemmaDictionaries.get(dictionaryFile);
      dictionaryInputStream = new ByteArrayInputStream(dictionary.getBytes(StandardCharsets.UTF_8));
    }
    LemmatizerModel lemmatizerModel = lemmatizerModelFile == null ? null : lemmatizerModels.get(lemmatizerModelFile);
    return new NLPLemmatizerOp(dictionaryInputStream, lemmatizerModel);
  }

  public static String getLemmatizerDictionary(String dictionaryFile, ResourceLoader loader) throws IOException {
    String dictionary = lemmaDictionaries.get(dictionaryFile);
    if (dictionary == null) {
      try (Reader reader = new InputStreamReader(loader.openResource(dictionaryFile), StandardCharsets.UTF_8)) {
        StringBuilder builder = new StringBuilder();
        char[] chars = new char[8092];
        int numRead = 0;
        do {
          numRead = reader.read(chars, 0, chars.length);
          if (numRead > 0) {
            builder.append(chars, 0, numRead);
          }
        } while (numRead > 0);
        dictionary = builder.toString();
        lemmaDictionaries.put(dictionaryFile, dictionary);
      }
    }
    return dictionary;
  }

  public static LemmatizerModel getLemmatizerModel(String modelName, ResourceLoader loader) throws IOException {
    LemmatizerModel model = lemmatizerModels.get(modelName);
    if (model == null) {
      try (InputStream resource = loader.openResource(modelName)) {
        model = new LemmatizerModel(resource);
      }
      lemmatizerModels.put(modelName, model);
    }
    return model;
  }

  // keeps unit test from blowing out memory
  public static void clearModels() {
    sentenceModels.clear();
    tokenizerModels.clear();
    posTaggerModels.clear();
    chunkerModels.clear();
    nerModels.clear();
    lemmaDictionaries.clear();
  }
}
