package org.apache.lucene.analysis.ko.dic;

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

import static org.apache.lucene.analysis.ko.dic.SyllableFeatures.EOGAN;
import static org.apache.lucene.analysis.ko.dic.SyllableFeatures.EOMI2;
import static org.apache.lucene.analysis.ko.dic.SyllableFeatures.JOSA1;
import static org.apache.lucene.analysis.ko.dic.SyllableFeatures.JOSA2;
import static org.apache.lucene.analysis.ko.dic.SyllableFeatures.WDSURF;
import static org.apache.lucene.analysis.ko.dic.SyllableFeatures.YNPAH;
import static org.apache.lucene.analysis.ko.dic.SyllableFeatures.YNPBA;
import static org.apache.lucene.analysis.ko.dic.SyllableFeatures.YNPLA;
import static org.apache.lucene.analysis.ko.dic.SyllableFeatures.YNPLN;
import static org.apache.lucene.analysis.ko.dic.SyllableFeatures.YNPMA;
import static org.apache.lucene.analysis.ko.dic.SyllableFeatures.YNPNA;
import static org.apache.lucene.analysis.ko.dic.SyllableFeatures.hasFeature;

public class TestSyllableFeatures extends LuceneTestCase {
  
  public void testGa() {
    assertTrue(hasFeature('가', JOSA1));
    assertTrue(hasFeature('가', JOSA2));
    assertTrue(hasFeature('가', EOMI2));
    assertFalse(hasFeature('가', YNPNA));
    assertFalse(hasFeature('가', YNPLA));
    assertFalse(hasFeature('가', YNPMA));
    assertFalse(hasFeature('가', YNPBA));
    assertFalse(hasFeature('가', YNPAH));
    assertFalse(hasFeature('가', YNPLN));
    assertFalse(hasFeature('가', WDSURF));
    assertTrue(hasFeature('가', EOGAN));
  }
  
  public void testGagg() {
    assertNoFeatures('갂');
  }
  
  public void testGan() {
    assertFalse(hasFeature('간', JOSA1));
    assertFalse(hasFeature('간', JOSA2));
    assertTrue(hasFeature('간', EOMI2));
    assertTrue(hasFeature('간', YNPNA));
    assertFalse(hasFeature('간', YNPLA));
    assertFalse(hasFeature('간', YNPMA));
    assertFalse(hasFeature('간', YNPBA));
    assertFalse(hasFeature('간', YNPAH));
    assertTrue(hasFeature('간', YNPLN));
    assertFalse(hasFeature('간', WDSURF));
    assertTrue(hasFeature('간', EOGAN));
  }
  
  public void testSomeFeatures() {
    assertTrue(hasFeature('갔', WDSURF));
    assertTrue(hasFeature('갔', YNPAH));
    assertTrue(hasFeature('갑', YNPBA));
    assertTrue(hasFeature('감', YNPMA));
    assertTrue(hasFeature('갈', YNPLA));
  }
  
  public void testOutOfBounds() {
    for (int i = 0; i < 0xAC00; i++) {
      assertNoFeatures((char)i);
    }
    for (int i = 0xD7B0; i <= 0xFFFF; i++) {
      assertNoFeatures((char)i);
    }
  }
  
  private void assertNoFeatures(char ch) {
    assertFalse(hasFeature(ch, JOSA1));
    assertFalse(hasFeature(ch, JOSA2));
    assertFalse(hasFeature(ch, EOMI2));
    assertFalse(hasFeature(ch, YNPNA));
    assertFalse(hasFeature(ch, YNPLA));
    assertFalse(hasFeature(ch, YNPMA));
    assertFalse(hasFeature(ch, YNPBA));
    assertFalse(hasFeature(ch, YNPAH));
    assertFalse(hasFeature(ch, YNPLN));
    assertFalse(hasFeature(ch, WDSURF));
    assertFalse(hasFeature(ch, EOGAN));
  }
}
