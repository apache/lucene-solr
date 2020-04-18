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

package org.apache.lucene.luke.models.overview;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.lucene.store.AlreadyClosedException;
import org.junit.Test;

public class OverviewImplTest extends OverviewTestBase {

  @Test
  public void testGetIndexPath() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    assertEquals(indexDir.toString(), overview.getIndexPath());
  }

  @Test
  public void testGetNumFields() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    assertEquals(2, (long) overview.getNumFields());
  }

  @Test
  public void testGetFieldNames() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    assertEquals(
        new HashSet<>(Arrays.asList("f1", "f2")),
        new HashSet<>(overview.getFieldNames()));
  }

  @Test
  public void testGetNumDocuments() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    assertEquals(3, (long) overview.getNumDocuments());
  }

  @Test
  public void testGetNumTerms() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    assertEquals(9, overview.getNumTerms());
  }

  @Test
  public void testHasDeletions() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    assertFalse(overview.hasDeletions());
  }

  @Test
  public void testGetNumDeletedDocs() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    assertEquals(0, (long) overview.getNumDeletedDocs());
  }

  @Test
  public void testIsOptimized() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    assertTrue(overview.isOptimized().orElse(false));
  }

  @Test
  public void testGetIndexVersion() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    assertTrue(overview.getIndexVersion().orElseThrow(IllegalStateException::new) > 0);
  }

  @Test
  public void testGetIndexFormat() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    assertEquals("Lucene 8.6 or later", overview.getIndexFormat().get());
  }

  @Test
  public void testGetDirImpl() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    assertEquals(dir.getClass().getName(), overview.getDirImpl().get());
  }

  @Test
  public void testGetCommitDescription() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    assertTrue(overview.getCommitDescription().isPresent());
  }

  @Test
  public void testGetCommitUserData() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    assertTrue(overview.getCommitUserData().isPresent());
  }

  @Test
  public void testGetSortedTermCounts() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    Map<String, Long>  countsMap = overview.getSortedTermCounts(TermCountsOrder.COUNT_DESC);
    assertEquals(Arrays.asList("f2", "f1"), new ArrayList<>(countsMap.keySet()));
  }

  @Test
  public void testGetTopTerms() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    List<TermStats> result = overview.getTopTerms("f2", 2);
    assertEquals("a", result.get(0).getDecodedTermText());
    assertEquals(3, result.get(0).getDocFreq());
    assertEquals("f2", result.get(0).getField());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTopTerms_illegal_numterms() {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    overview.getTopTerms("f2", -1);
  }

  @Test(expected = AlreadyClosedException.class)
  public void testClose() throws Exception {
    OverviewImpl overview = new OverviewImpl(reader, indexDir.toString());
    reader.close();
    overview.getNumFields();
  }

}
