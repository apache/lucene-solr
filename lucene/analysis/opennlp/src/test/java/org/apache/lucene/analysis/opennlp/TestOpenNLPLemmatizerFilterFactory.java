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

package org.apache.lucene.analysis.opennlp;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilterFactory;
import org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilterFactory;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;

public class TestOpenNLPLemmatizerFilterFactory extends BaseTokenStreamTestCase {

  private static final String SENTENCE = "They sent him running in the evening.";
  private static final String[] SENTENCE_dict_punc =   {"they", "send", "he",  "run",  "in", "the", "evening", "."};
  private static final String[] SENTENCE_maxent_punc = {"they", "send", "he",  "runn", "in", "the", "evening", "."};
  private static final String[] SENTENCE_posTags =     {"NNP",  "VBD",  "PRP", "VBG",  "IN", "DT",  "NN",      "."};

  private static final String SENTENCES = "They sent him running in the evening. He did not come back.";
  private static final String[] SENTENCES_dict_punc
      = {"they", "send", "he",  "run",  "in", "the", "evening", ".", "he",  "do",  "not", "come", "back", "."};
  private static final String[] SENTENCES_maxent_punc
      = {"they", "send", "he",  "runn", "in", "the", "evening", ".", "he",  "do",  "not", "come", "back", "."};
  private static final String[] SENTENCES_posTags
      = {"NNP",  "VBD",  "PRP", "VBG",  "IN", "DT",  "NN",      ".", "PRP", "VBD", "RB",  "VB",   "RB",   "."};

  private static final String SENTENCE_both = "Konstantin Kalashnitsov constantly caliphed.";
  private static final String[] SENTENCE_both_punc
      = {"konstantin", "kalashnitsov", "constantly", "caliph", "."};
  private static final String[] SENTENCE_both_posTags
      = {"IN",         "JJ",          "NN",          "VBN",    "."};

  private static final String SENTENCES_both = "Konstantin Kalashnitsov constantly caliphed. Coreena could care, completely.";
  private static final String[] SENTENCES_both_punc
      = {"konstantin", "kalashnitsov", "constantly", "caliph", ".", "coreena", "could", "care", ",", "completely", "."};
  private static final String[] SENTENCES_both_posTags
      = {"IN",         "JJ",           "NN",          "VBN",    ".", "NNP",     "VBN",   "NN",   ",", "NN",         "."};

  private static final String[] SENTENCES_dict_keep_orig_punc
      = {"They", "they", "sent", "send", "him", "he", "running", "run",  "in", "the", "evening", ".", "He", "he",   "did", "do", "not", "come", "back", "."};
  private static final String[] SENTENCES_max_ent_keep_orig_punc
      = {"They", "they", "sent", "send", "him", "he", "running", "runn", "in", "the", "evening", ".", "He", "he",   "did", "do", "not", "come", "back", "."};
  private static final String[] SENTENCES_keep_orig_posTags
      = {"NNP",  "NNP",  "VBD",  "VBD",  "PRP", "PRP", "VBG",    "VBG",  "IN", "DT",  "NN",      ".", "PRP", "PRP", "VBD", "VBD", "RB",  "VB",  "RB",   "."};

  private static final String[] SENTENCES_both_keep_orig_punc
      = {"Konstantin", "konstantin", "Kalashnitsov", "kalashnitsov", "constantly", "caliphed", "caliph", ".", "Coreena", "coreena", "could", "care", ",", "completely", "."};
  private static final String[] SENTENCES_both_keep_orig_posTags
      = {"IN",         "IN",         "JJ",           "JJ",           "NN",         "VBN",      "VBN",    ".", "NNP",     "NNP",     "VBN",   "NN",   ",", "NN",         "."};


  private static final String tokenizerModelFile = "en-test-tokenizer.bin";
  private static final String sentenceModelFile = "en-test-sent.bin";
  private static final String posTaggerModelFile = "en-test-pos-maxent.bin";
  private static final String lemmatizerModelFile = "en-test-lemmatizer.bin";
  private static final String lemmatizerDictFile = "en-test-lemmas.dict";


  public void test1SentenceDictionaryOnly() throws Exception {
    CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
        .withTokenizer("opennlp", "tokenizerModel", tokenizerModelFile, "sentenceModel", sentenceModelFile)
        .addTokenFilter("opennlpPOS", "posTaggerModel", "en-test-pos-maxent.bin")
        .addTokenFilter("opennlplemmatizer", "dictionary", "en-test-lemmas.dict")
        .build();
    assertAnalyzesTo(analyzer, SENTENCE, SENTENCE_dict_punc, null, null,
        SENTENCE_posTags, null, null, true);
  }

  public void test2SentencesDictionaryOnly() throws Exception {
    CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
        .withTokenizer("opennlp", "tokenizerModel", tokenizerModelFile, "sentenceModel", sentenceModelFile)
        .addTokenFilter("opennlpPOS", "posTaggerModel", posTaggerModelFile)
        .addTokenFilter("opennlplemmatizer", "dictionary", lemmatizerDictFile)
        .build();
    assertAnalyzesTo(analyzer, SENTENCES, SENTENCES_dict_punc, null, null,
        SENTENCES_posTags, null, null, true);
  }

  public void test1SentenceMaxEntOnly() throws Exception {
    CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
        .withTokenizer("opennlp", "tokenizerModel", tokenizerModelFile, "sentenceModel", sentenceModelFile)
        .addTokenFilter("opennlpPOS", "posTaggerModel", posTaggerModelFile)
        .addTokenFilter("opennlplemmatizer", "lemmatizerModel", lemmatizerModelFile)
        .build();
    assertAnalyzesTo(analyzer, SENTENCE, SENTENCE_maxent_punc, null, null,
        SENTENCE_posTags, null, null, true);
  }

  public void test2SentencesMaxEntOnly() throws Exception {
    CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
        .withTokenizer("opennlp", "tokenizerModel", tokenizerModelFile, "sentenceModel", sentenceModelFile)
        .addTokenFilter("opennlpPOS", "posTaggerModel", posTaggerModelFile)
        .addTokenFilter("OpenNLPLemmatizer", "lemmatizerModel", lemmatizerModelFile)
        .build();
    assertAnalyzesTo(analyzer, SENTENCES, SENTENCES_maxent_punc, null, null,
        SENTENCES_posTags, null, null, true);
  }

  public void test1SentenceDictionaryAndMaxEnt() throws Exception {
    CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
        .withTokenizer("opennlp", "tokenizerModel", tokenizerModelFile, "sentenceModel", sentenceModelFile)
        .addTokenFilter("opennlpPOS", "posTaggerModel", "en-test-pos-maxent.bin")
        .addTokenFilter("opennlplemmatizer", "dictionary", "en-test-lemmas.dict", "lemmatizerModel", lemmatizerModelFile)
        .build();
    assertAnalyzesTo(analyzer, SENTENCE_both, SENTENCE_both_punc, null, null,
        SENTENCE_both_posTags, null, null, true);
  }

  public void test2SentencesDictionaryAndMaxEnt() throws Exception {
    CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
        .withTokenizer("opennlp", "tokenizerModel", tokenizerModelFile, "sentenceModel", sentenceModelFile)
        .addTokenFilter("opennlpPOS", "posTaggerModel", posTaggerModelFile)
        .addTokenFilter("opennlplemmatizer", "dictionary", lemmatizerDictFile, "lemmatizerModel", lemmatizerModelFile)
        .build();
    assertAnalyzesTo(analyzer, SENTENCES_both, SENTENCES_both_punc, null, null,
        SENTENCES_both_posTags, null, null, true);
  }

  public void testKeywordAttributeAwarenessDictionaryOnly() throws Exception {
    CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
        .withTokenizer("opennlp", "tokenizerModel", tokenizerModelFile, "sentenceModel", sentenceModelFile)
        .addTokenFilter("opennlpPOS", "posTaggerModel", posTaggerModelFile)
        .addTokenFilter(KeywordRepeatFilterFactory.class)
        .addTokenFilter("opennlplemmatizer", "dictionary", lemmatizerDictFile)
        .addTokenFilter(RemoveDuplicatesTokenFilterFactory.class)
        .build();
    assertAnalyzesTo(analyzer, SENTENCES, SENTENCES_dict_keep_orig_punc, null, null,
        SENTENCES_keep_orig_posTags, null, null, true);
  }

  public void testKeywordAttributeAwarenessMaxEntOnly() throws Exception {
    CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
        .withTokenizer("opennlp", "tokenizerModel", tokenizerModelFile, "sentenceModel", sentenceModelFile)
        .addTokenFilter("opennlpPOS", "posTaggerModel", posTaggerModelFile)
        .addTokenFilter(KeywordRepeatFilterFactory.class)
        .addTokenFilter("opennlplemmatizer", "lemmatizerModel", lemmatizerModelFile)
        .addTokenFilter(RemoveDuplicatesTokenFilterFactory.class)
        .build();
    assertAnalyzesTo(analyzer, SENTENCES, SENTENCES_max_ent_keep_orig_punc, null, null,
        SENTENCES_keep_orig_posTags, null, null, true);
  }

  public void testKeywordAttributeAwarenessDictionaryAndMaxEnt() throws Exception {
    CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
        .withTokenizer("opennlp", "tokenizerModel", tokenizerModelFile, "sentenceModel", sentenceModelFile)
        .addTokenFilter("opennlpPOS", "posTaggerModel", posTaggerModelFile)
        .addTokenFilter(KeywordRepeatFilterFactory.class)
        .addTokenFilter("opennlplemmatizer", "dictionary", lemmatizerDictFile, "lemmatizerModel", lemmatizerModelFile)
        .addTokenFilter(RemoveDuplicatesTokenFilterFactory.class)
        .build();
    assertAnalyzesTo(analyzer, SENTENCES_both, SENTENCES_both_keep_orig_punc, null, null,
        SENTENCES_both_keep_orig_posTags, null, null, true);
  }

}
