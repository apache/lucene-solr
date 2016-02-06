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
package org.apache.lucene.analysis.id;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.util.IOUtils;

/**
 * Tests {@link IndonesianStemmer}
 */
public class TestIndonesianStemmer extends BaseTokenStreamTestCase {
  private Analyzer a, b;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    /* full stemming, no stopwords */
    a = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.KEYWORD, false);
        return new TokenStreamComponents(tokenizer, new IndonesianStemFilter(tokenizer));
      }
    };
    /* inflectional-only stemming */
    b = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.KEYWORD, false);
        return new TokenStreamComponents(tokenizer, new IndonesianStemFilter(tokenizer, false));
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    IOUtils.close(a, b);
    super.tearDown();
  }
  
  /** Some examples from the paper */
  public void testExamples() throws IOException {
    checkOneTerm(a, "bukukah", "buku");
    checkOneTerm(a, "adalah", "ada");
    checkOneTerm(a, "bukupun", "buku");
    checkOneTerm(a, "bukuku", "buku");
    checkOneTerm(a, "bukumu", "buku");
    checkOneTerm(a, "bukunya", "buku");
    checkOneTerm(a, "mengukur", "ukur");
    checkOneTerm(a, "menyapu", "sapu");
    checkOneTerm(a, "menduga", "duga");
    checkOneTerm(a, "menuduh", "uduh");
    checkOneTerm(a, "membaca", "baca");
    checkOneTerm(a, "merusak", "rusak");
    checkOneTerm(a, "pengukur", "ukur");
    checkOneTerm(a, "penyapu", "sapu");
    checkOneTerm(a, "penduga", "duga");
    checkOneTerm(a, "pembaca", "baca");
    checkOneTerm(a, "diukur", "ukur");
    checkOneTerm(a, "tersapu", "sapu");
    checkOneTerm(a, "kekasih", "kasih");
    checkOneTerm(a, "berlari", "lari");
    checkOneTerm(a, "belajar", "ajar");
    checkOneTerm(a, "bekerja", "kerja");
    checkOneTerm(a, "perjelas", "jelas");
    checkOneTerm(a, "pelajar", "ajar");
    checkOneTerm(a, "pekerja", "kerja");
    checkOneTerm(a, "tarikkan", "tarik");
    checkOneTerm(a, "ambilkan", "ambil");
    checkOneTerm(a, "mengambilkan", "ambil");
    checkOneTerm(a, "makanan", "makan");
    checkOneTerm(a, "janjian", "janji");
    checkOneTerm(a, "perjanjian", "janji");
    checkOneTerm(a, "tandai", "tanda");
    checkOneTerm(a, "dapati", "dapat");
    checkOneTerm(a, "mendapati", "dapat");
    checkOneTerm(a, "pantai", "panta");
  }
  
  /** Some detailed analysis examples (that might not be the best) */
  public void testIRExamples() throws IOException {
    checkOneTerm(a, "penyalahgunaan", "salahguna");
    checkOneTerm(a, "menyalahgunakan", "salahguna");
    checkOneTerm(a, "disalahgunakan", "salahguna");
       
    checkOneTerm(a, "pertanggungjawaban", "tanggungjawab");
    checkOneTerm(a, "mempertanggungjawabkan", "tanggungjawab");
    checkOneTerm(a, "dipertanggungjawabkan", "tanggungjawab");
    
    checkOneTerm(a, "pelaksanaan", "laksana");
    checkOneTerm(a, "pelaksana", "laksana");
    checkOneTerm(a, "melaksanakan", "laksana");
    checkOneTerm(a, "dilaksanakan", "laksana");
    
    checkOneTerm(a, "melibatkan", "libat");
    checkOneTerm(a, "terlibat", "libat");
    
    checkOneTerm(a, "penculikan", "culik");
    checkOneTerm(a, "menculik", "culik");
    checkOneTerm(a, "diculik", "culik");
    checkOneTerm(a, "penculik", "culik");
    
    checkOneTerm(a, "perubahan", "ubah");
    checkOneTerm(a, "peledakan", "ledak");
    checkOneTerm(a, "penanganan", "tangan");
    checkOneTerm(a, "kepolisian", "polisi");
    checkOneTerm(a, "kenaikan", "naik");
    checkOneTerm(a, "bersenjata", "senjata");
    checkOneTerm(a, "penyelewengan", "seleweng");
    checkOneTerm(a, "kecelakaan", "celaka");
  }
  
  /** Test stemming only inflectional suffixes */
  public void testInflectionalOnly() throws IOException {
    checkOneTerm(b, "bukunya", "buku");
    checkOneTerm(b, "bukukah", "buku");
    checkOneTerm(b, "bukunyakah", "buku");
    checkOneTerm(b, "dibukukannya", "dibukukan");
  }
  
  public void testShouldntStem() throws IOException {
    checkOneTerm(a, "bersenjata", "senjata");
    checkOneTerm(a, "bukukah", "buku");
    checkOneTerm(a, "gigi", "gigi");
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new IndonesianStemFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
