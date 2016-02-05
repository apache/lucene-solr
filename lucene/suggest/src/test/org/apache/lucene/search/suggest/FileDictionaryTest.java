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
package org.apache.lucene.search.suggest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;


public class FileDictionaryTest extends LuceneTestCase {
  
  private Map.Entry<List<String>, String> generateFileEntry(String fieldDelimiter, boolean hasWeight, boolean hasPayload) {
    List<String> entryValues = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    String term = TestUtil.randomSimpleString(random(), 1, 300);
    sb.append(term);
    entryValues.add(term);
    if (hasWeight) {
      sb.append(fieldDelimiter);
      long weight = TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE);
      sb.append(weight);
      entryValues.add(String.valueOf(weight));
    }
    if (hasPayload) {
      sb.append(fieldDelimiter);
      String payload = TestUtil.randomSimpleString(random(), 1, 300);
      sb.append(payload);
      entryValues.add(payload);
    }
    sb.append("\n");
    return new SimpleEntry<>(entryValues, sb.toString());
  }
  
  private Map.Entry<List<List<String>>,String> generateFileInput(int count, String fieldDelimiter, boolean hasWeights, boolean hasPayloads) {
    List<List<String>> entries = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    boolean hasPayload = hasPayloads;
    for (int i = 0; i < count; i++) {
      if (hasPayloads) {
        hasPayload = (i==0) ? true : random().nextBoolean();
      } 
      Map.Entry<List<String>, String> entrySet = generateFileEntry(fieldDelimiter, (!hasPayloads && hasWeights) ? random().nextBoolean() : hasWeights, hasPayload);
      entries.add(entrySet.getKey());
      sb.append(entrySet.getValue());
    }
    return new SimpleEntry<>(entries, sb.toString());
  }
  
  @Test
  public void testFileWithTerm() throws IOException {
    Map.Entry<List<List<String>>,String> fileInput = generateFileInput(atLeast(100), FileDictionary.DEFAULT_FIELD_DELIMITER, false, false);
    InputStream inputReader = new ByteArrayInputStream(fileInput.getValue().getBytes(StandardCharsets.UTF_8));
    FileDictionary dictionary = new FileDictionary(inputReader);
    List<List<String>> entries = fileInput.getKey();
    InputIterator inputIter = dictionary.getEntryIterator();
    assertFalse(inputIter.hasPayloads());
    BytesRef term;
    int count = 0;
    while((term = inputIter.next()) != null) {
      assertTrue(entries.size() > count);
      List<String> entry = entries.get(count);
      assertTrue(entry.size() >= 1); // at least a term
      assertEquals(entry.get(0), term.utf8ToString());
      assertEquals(1, inputIter.weight());
      assertNull(inputIter.payload());
      count++;
    }
    assertEquals(count, entries.size());
  }
  
  @Test
  public void testFileWithWeight() throws IOException {
    Map.Entry<List<List<String>>,String> fileInput = generateFileInput(atLeast(100), FileDictionary.DEFAULT_FIELD_DELIMITER, true, false);
    InputStream inputReader = new ByteArrayInputStream(fileInput.getValue().getBytes(StandardCharsets.UTF_8));
    FileDictionary dictionary = new FileDictionary(inputReader);
    List<List<String>> entries = fileInput.getKey();
    InputIterator inputIter = dictionary.getEntryIterator();
    assertFalse(inputIter.hasPayloads());
    BytesRef term;
    int count = 0;
    while((term = inputIter.next()) != null) {
      assertTrue(entries.size() > count);
      List<String> entry = entries.get(count);
      assertTrue(entry.size() >= 1); // at least a term
      assertEquals(entry.get(0), term.utf8ToString());
      assertEquals((entry.size() == 2) ? Long.parseLong(entry.get(1)) : 1, inputIter.weight());
      assertNull(inputIter.payload());
      count++;
    }
    assertEquals(count, entries.size());
  }
  
  @Test
  public void testFileWithWeightAndPayload() throws IOException {
    Map.Entry<List<List<String>>,String> fileInput = generateFileInput(atLeast(100), FileDictionary.DEFAULT_FIELD_DELIMITER, true, true);
    InputStream inputReader = new ByteArrayInputStream(fileInput.getValue().getBytes(StandardCharsets.UTF_8));
    FileDictionary dictionary = new FileDictionary(inputReader);
    List<List<String>> entries = fileInput.getKey();
    InputIterator inputIter = dictionary.getEntryIterator();
    assertTrue(inputIter.hasPayloads());
    BytesRef term;
    int count = 0;
    while((term = inputIter.next()) != null) {
      assertTrue(entries.size() > count);
      List<String> entry = entries.get(count);
      assertTrue(entry.size() >= 2); // at least term and weight
      assertEquals(entry.get(0), term.utf8ToString());
      assertEquals(Long.parseLong(entry.get(1)), inputIter.weight());
      if (entry.size() == 3) {
        assertEquals(entry.get(2), inputIter.payload().utf8ToString());
      } else {
        assertEquals(inputIter.payload().length, 0);
      }
      count++;
    }
    assertEquals(count, entries.size());
  }
  
  @Test
  public void testFileWithOneEntry() throws IOException {
    Map.Entry<List<List<String>>,String> fileInput = generateFileInput(1, FileDictionary.DEFAULT_FIELD_DELIMITER, true, true);
    InputStream inputReader = new ByteArrayInputStream(fileInput.getValue().getBytes(StandardCharsets.UTF_8));
    FileDictionary dictionary = new FileDictionary(inputReader);
    List<List<String>> entries = fileInput.getKey();
    InputIterator inputIter = dictionary.getEntryIterator();
    assertTrue(inputIter.hasPayloads());
    BytesRef term;
    int count = 0;
    while((term = inputIter.next()) != null) {
      assertTrue(entries.size() > count);
      List<String> entry = entries.get(count);
      assertTrue(entry.size() >= 2); // at least term and weight
      assertEquals(entry.get(0), term.utf8ToString());
      assertEquals(Long.parseLong(entry.get(1)), inputIter.weight());
      if (entry.size() == 3) {
        assertEquals(entry.get(2), inputIter.payload().utf8ToString());
      } else {
        assertEquals(inputIter.payload().length, 0);
      }
      count++;
    }
    assertEquals(count, entries.size());
  }
  
  
  @Test
  public void testFileWithDifferentDelimiter() throws IOException {
    Map.Entry<List<List<String>>,String> fileInput = generateFileInput(atLeast(100), " , ", true, true);
    InputStream inputReader = new ByteArrayInputStream(fileInput.getValue().getBytes(StandardCharsets.UTF_8));
    FileDictionary dictionary = new FileDictionary(inputReader, " , ");
    List<List<String>> entries = fileInput.getKey();
    InputIterator inputIter = dictionary.getEntryIterator();
    assertTrue(inputIter.hasPayloads());
    BytesRef term;
    int count = 0;
    while((term = inputIter.next()) != null) {
      assertTrue(entries.size() > count);
      List<String> entry = entries.get(count);
      assertTrue(entry.size() >= 2); // at least term and weight
      assertEquals(entry.get(0), term.utf8ToString());
      assertEquals(Long.parseLong(entry.get(1)), inputIter.weight());
      if (entry.size() == 3) {
        assertEquals(entry.get(2), inputIter.payload().utf8ToString());
      } else {
        assertEquals(inputIter.payload().length, 0);
      }
      count++;
    }
    assertEquals(count, entries.size());
  }
  
}
