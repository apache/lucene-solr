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

package org.apache.lucene.luke.models.documents;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.junit.Test;

public class TermVectorsAdapterTest extends DocumentsTestBase {

  @Override
  protected void createIndex() throws IOException {
    indexDir = createTempDir("testIndex");

    Directory dir = newFSDirectory(indexDir);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, new StandardAnalyzer());

    FieldType textType = new FieldType();
    textType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    textType.setTokenized(true);
    textType.setStoreTermVectors(true);

    FieldType textType_pos = new FieldType();
    textType_pos.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    textType_pos.setTokenized(true);
    textType_pos.setStoreTermVectors(true);
    textType_pos.setStoreTermVectorPositions(true);

    FieldType textType_pos_offset = new FieldType();
    textType_pos_offset.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    textType_pos_offset.setTokenized(true);
    textType_pos_offset.setStoreTermVectors(true);
    textType_pos_offset.setStoreTermVectorPositions(true);
    textType_pos_offset.setStoreTermVectorOffsets(true);

    String text = "It is a truth universally acknowledged, that a single man in possession of a good fortune, must be in want of a wife.";
    Document doc = new Document();
    doc.add(newField("text1", text, textType));
    doc.add(newField("text2", text, textType_pos));
    doc.add(newField("text3", text, textType_pos_offset));
    writer.addDocument(doc);

    writer.commit();
    writer.close();
    dir.close();
  }

  @Test
  public void testGetTermVector() throws Exception {
    TermVectorsAdapter adapterImpl = new TermVectorsAdapter(reader);
    List<TermVectorEntry> tvEntries = adapterImpl.getTermVector(0, "text1");

    assertEquals(18, tvEntries.size());

    assertEquals("a", tvEntries.get(0).getTermText());
    assertEquals(4, tvEntries.get(0).getFreq());

    assertEquals("acknowledged", tvEntries.get(1).getTermText());
    assertEquals(1, tvEntries.get(1).getFreq());

    assertEquals("be", tvEntries.get(2).getTermText());
    assertEquals(1, tvEntries.get(2).getFreq());

    assertEquals("fortune", tvEntries.get(3).getTermText());
    assertEquals(1, tvEntries.get(3).getFreq());

    assertEquals("good", tvEntries.get(4).getTermText());
    assertEquals(1, tvEntries.get(4).getFreq());

    assertEquals("in", tvEntries.get(5).getTermText());
    assertEquals(2, tvEntries.get(5).getFreq());

    assertEquals("is", tvEntries.get(6).getTermText());
    assertEquals(1, tvEntries.get(6).getFreq());

    assertEquals("it", tvEntries.get(7).getTermText());
    assertEquals(1, tvEntries.get(7).getFreq());

    assertEquals("man", tvEntries.get(8).getTermText());
    assertEquals(1, tvEntries.get(8).getFreq());

    assertEquals("must", tvEntries.get(9).getTermText());
    assertEquals(1, tvEntries.get(9).getFreq());

    assertEquals("of", tvEntries.get(10).getTermText());
    assertEquals(1, tvEntries.get(2).getFreq());

    assertEquals("possession", tvEntries.get(11).getTermText());
    assertEquals(1, tvEntries.get(11).getFreq());

    assertEquals("single", tvEntries.get(12).getTermText());
    assertEquals(1, tvEntries.get(12).getFreq());

    assertEquals("that", tvEntries.get(13).getTermText());
    assertEquals(1, tvEntries.get(13).getFreq());

    assertEquals("truth", tvEntries.get(14).getTermText());
    assertEquals(1, tvEntries.get(14).getFreq());

    assertEquals("universally", tvEntries.get(15).getTermText());
    assertEquals(1, tvEntries.get(15).getFreq());

    assertEquals("want", tvEntries.get(16).getTermText());
    assertEquals(1, tvEntries.get(16).getFreq());

    assertEquals("wife", tvEntries.get(17).getTermText());
    assertEquals(1, tvEntries.get(17).getFreq());
  }

  @Test
  public void testGetTermVector_with_positions() throws Exception {
    TermVectorsAdapter adapterImpl = new TermVectorsAdapter(reader);
    List<TermVectorEntry> tvEntries = adapterImpl.getTermVector(0, "text2");

    assertEquals(18, tvEntries.size());

    assertEquals("acknowledged", tvEntries.get(1).getTermText());
    assertEquals(1, tvEntries.get(1).getFreq());
    assertEquals(5, tvEntries.get(1).getPositions().get(0).getPosition());
    assertFalse(tvEntries.get(1).getPositions().get(0).getStartOffset().isPresent());
    assertFalse(tvEntries.get(1).getPositions().get(0).getEndOffset().isPresent());
  }

  @Test
  public void testGetTermVector_with_positions_offsets() throws Exception {
    TermVectorsAdapter adapterImpl = new TermVectorsAdapter(reader);
    List<TermVectorEntry> tvEntries = adapterImpl.getTermVector(0, "text3");

    assertEquals(18, tvEntries.size());

    assertEquals("acknowledged", tvEntries.get(1).getTermText());
    assertEquals(1, tvEntries.get(1).getFreq());
    assertEquals(5, tvEntries.get(1).getPositions().get(0).getPosition());
    assertEquals(26, tvEntries.get(1).getPositions().get(0).getStartOffset().orElse(-1));
    assertEquals(38, tvEntries.get(1).getPositions().get(0).getEndOffset().orElse(-1));
  }

  @Test
  public void testGetTermVectors_notAvailable() throws Exception {
    TermVectorsAdapter adapterImpl = new TermVectorsAdapter(reader);
    assertEquals(0, adapterImpl.getTermVector(0, "title").size());
  }
}
