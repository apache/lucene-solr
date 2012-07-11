package org.apache.lucene.analysis.id;

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

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 * Tests {@link IndonesianStemmer}
 */
public class TestIndonesianStemmer extends BaseTokenStreamTestCase {
  /* full stemming, no stopwords */
  Analyzer a = new Analyzer() {
    @Override
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tokenizer = new KeywordTokenizer(reader);
      return new TokenStreamComponents(tokenizer, new IndonesianStemFilter(tokenizer));
    }
  };
  
  /** Some examples from the paper */
  public void testExamples() throws IOException {
    checkOneTerm(a, "bukukah", "buku");
    checkOneTermReuse(a, "adalah", "ada");
    checkOneTermReuse(a, "bukupun", "buku");
    checkOneTermReuse(a, "bukuku", "buku");
    checkOneTermReuse(a, "bukumu", "buku");
    checkOneTermReuse(a, "bukunya", "buku");
    checkOneTermReuse(a, "mengukur", "ukur");
    checkOneTermReuse(a, "menyapu", "sapu");
    checkOneTermReuse(a, "menduga", "duga");
    checkOneTermReuse(a, "menuduh", "uduh");
    checkOneTermReuse(a, "membaca", "baca");
    checkOneTermReuse(a, "merusak", "rusak");
    checkOneTermReuse(a, "pengukur", "ukur");
    checkOneTermReuse(a, "penyapu", "sapu");
    checkOneTermReuse(a, "penduga", "duga");
    checkOneTermReuse(a, "pembaca", "baca");
    checkOneTermReuse(a, "diukur", "ukur");
    checkOneTermReuse(a, "tersapu", "sapu");
    checkOneTermReuse(a, "kekasih", "kasih");
    checkOneTermReuse(a, "berlari", "lari");
    checkOneTermReuse(a, "belajar", "ajar");
    checkOneTermReuse(a, "bekerja", "kerja");
    checkOneTermReuse(a, "perjelas", "jelas");
    checkOneTermReuse(a, "pelajar", "ajar");
    checkOneTermReuse(a, "pekerja", "kerja");
    checkOneTermReuse(a, "tarikkan", "tarik");
    checkOneTermReuse(a, "ambilkan", "ambil");
    checkOneTermReuse(a, "mengambilkan", "ambil");
    checkOneTermReuse(a, "makanan", "makan");
    checkOneTermReuse(a, "janjian", "janji");
    checkOneTermReuse(a, "perjanjian", "janji");
    checkOneTermReuse(a, "tandai", "tanda");
    checkOneTermReuse(a, "dapati", "dapat");
    checkOneTermReuse(a, "mendapati", "dapat");
    checkOneTermReuse(a, "pantai", "panta");
  }
  
  /** Some detailed analysis examples (that might not be the best) */
  public void testIRExamples() throws IOException {
    checkOneTerm(a, "penyalahgunaan", "salahguna");
    checkOneTermReuse(a, "menyalahgunakan", "salahguna");
    checkOneTermReuse(a, "disalahgunakan", "salahguna");
       
    checkOneTermReuse(a, "pertanggungjawaban", "tanggungjawab");
    checkOneTermReuse(a, "mempertanggungjawabkan", "tanggungjawab");
    checkOneTermReuse(a, "dipertanggungjawabkan", "tanggungjawab");
    
    checkOneTermReuse(a, "pelaksanaan", "laksana");
    checkOneTermReuse(a, "pelaksana", "laksana");
    checkOneTermReuse(a, "melaksanakan", "laksana");
    checkOneTermReuse(a, "dilaksanakan", "laksana");
    
    checkOneTermReuse(a, "melibatkan", "libat");
    checkOneTermReuse(a, "terlibat", "libat");
    
    checkOneTermReuse(a, "penculikan", "culik");
    checkOneTermReuse(a, "menculik", "culik");
    checkOneTermReuse(a, "diculik", "culik");
    checkOneTermReuse(a, "penculik", "culik");
    
    checkOneTermReuse(a, "perubahan", "ubah");
    checkOneTermReuse(a, "peledakan", "ledak");
    checkOneTermReuse(a, "penanganan", "tangan");
    checkOneTermReuse(a, "kepolisian", "polisi");
    checkOneTermReuse(a, "kenaikan", "naik");
    checkOneTermReuse(a, "bersenjata", "senjata");
    checkOneTermReuse(a, "penyelewengan", "seleweng");
    checkOneTermReuse(a, "kecelakaan", "celaka");
  }
  
  /* inflectional-only stemming */
  Analyzer b = new Analyzer() {
    @Override
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tokenizer = new KeywordTokenizer(reader);
      return new TokenStreamComponents(tokenizer, new IndonesianStemFilter(tokenizer, false));
    }
  };
  
  /** Test stemming only inflectional suffixes */
  public void testInflectionalOnly() throws IOException {
    checkOneTerm(b, "bukunya", "buku");
    checkOneTermReuse(b, "bukukah", "buku");
    checkOneTermReuse(b, "bukunyakah", "buku");
    checkOneTermReuse(b, "dibukukannya", "dibukukan");
  }
  
  public void testShouldntStem() throws IOException {
    checkOneTerm(a, "bersenjata", "senjata");
    checkOneTermReuse(a, "bukukah", "buku");
    checkOneTermReuse(a, "gigi", "gigi");
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        return new TokenStreamComponents(tokenizer, new IndonesianStemFilter(tokenizer));
      }
    };
    checkOneTermReuse(a, "", "");
  }
}
