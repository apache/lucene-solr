package org.apache.lucene.analysis.hunspell;

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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class HunspellStemmerTest extends LuceneTestCase {

  private static HunspellStemmer stemmer;

  @BeforeClass
  public static void beforeClass() throws IOException, ParseException {
    createStemmer(true);
  }
  
  @AfterClass
  public static void afterClass() {
    stemmer = null;
  }

  @Test
  public void testStem_simpleSuffix() {
    List<HunspellStemmer.Stem> stems = stemmer.stem("lucene");

    assertEquals(2, stems.size());
    assertEquals("lucene", stems.get(0).getStemString());
    assertEquals("lucen", stems.get(1).getStemString());

    stems = stemmer.stem("mahoute");
    assertEquals(1, stems.size());
    assertEquals("mahout", stems.get(0).getStemString());
  }

  @Test
  public void testStem_simplePrefix() {
    List<HunspellStemmer.Stem> stems = stemmer.stem("solr");

    assertEquals(1, stems.size());
    assertEquals("olr", stems.get(0).getStemString());
  }

  @Test
  public void testStem_recursiveSuffix() {
    List<HunspellStemmer.Stem> stems = stemmer.stem("abcd");

    assertEquals(1, stems.size());
    assertEquals("ab", stems.get(0).getStemString());
  }

  @Test
  public void testStem_ignoreCase() throws IOException, ParseException {
    List<HunspellStemmer.Stem> stems;
    createStemmer(true);

    stems = stemmer.stem("apache");
    assertEquals(1, stems.size());
    assertEquals("apach", stems.get(0).getStemString());

    stems = stemmer.stem("APACHE");
    assertEquals(1, stems.size());
    assertEquals("apach", stems.get(0).getStemString());

    stems = stemmer.stem("Apache");
    assertEquals(1, stems.size());
    assertEquals("apach", stems.get(0).getStemString());
    
    stems = stemmer.stem("foos");
    assertEquals(1, stems.size());
    assertEquals("foo", stems.get(0).getStemString());
    
    stems = stemmer.stem("food");
    assertEquals(1, stems.size());
    assertEquals("foo", stems.get(0).getStemString());
    
    stems = stemmer.stem("Foos");
    assertEquals(1, stems.size());
    assertEquals("foo", stems.get(0).getStemString());
    
    stems = stemmer.stem("Food");
    assertEquals(1, stems.size());
    assertEquals("foo", stems.get(0).getStemString());
  }

  @Test
  public void testStem_caseSensitive() throws IOException, ParseException {
    createStemmer(false);
    List<HunspellStemmer.Stem> stems = stemmer.stem("apache");
    assertEquals(0, stems.size());

    stems = stemmer.stem("Apache");
    assertEquals(1, stems.size());
    assertEquals("Apach", stems.get(0).getStemString());
  }

  
  private static void createStemmer(boolean ignoreCase) throws IOException, ParseException {
    InputStream affixStream = HunspellStemmerTest.class.getResourceAsStream("test.aff");
    InputStream dictStream = HunspellStemmerTest.class.getResourceAsStream("test.dic");

    HunspellDictionary dictionary = new HunspellDictionary(affixStream, dictStream, TEST_VERSION_CURRENT, ignoreCase);
    stemmer = new HunspellStemmer(dictionary);

    affixStream.close();
    dictStream.close();
  }

}
