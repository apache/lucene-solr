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

package org.apache.lucene.luke.models.commits;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CommitsImplTest extends LuceneTestCase {

  private DirectoryReader reader;

  private Directory dir;

  private Path indexDir;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    indexDir = createIndex();
    dir = newFSDirectory(indexDir);
    reader = DirectoryReader.open(dir);
  }

  private Path createIndex() throws IOException {
    Path indexDir = createTempDir();

    Directory dir = newFSDirectory(indexDir);

    IndexWriterConfig config = new IndexWriterConfig(new MockAnalyzer(random())).setCodec(TestUtil.getDefaultCodec());
    config.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    Document doc1 = new Document();
    doc1.add(newStringField("f1", "1", Field.Store.NO));
    writer.addDocument(doc1);

    writer.commit();

    Document doc2 = new Document();
    doc2.add(newStringField("f1", "2", Field.Store.NO));
    writer.addDocument(doc2);

    Document doc3 = new Document();
    doc3.add(newStringField("f1", "3", Field.Store.NO));
    writer.addDocument(doc3);

    writer.commit();

    writer.close();
    dir.close();

    return indexDir;
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    reader.close();
    dir.close();
  }

  @Test
  public void testListCommits() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    List<Commit> commitList = commits.listCommits();
    assertTrue(commitList.size() > 0);
    // should be sorted by descending order in generation
    assertEquals(commitList.size(), commitList.get(0).getGeneration());
    assertEquals(1, commitList.get(commitList.size()-1).getGeneration());
  }

  @Test
  public void testGetCommit() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    Optional<Commit> commit = commits.getCommit(1);
    assertTrue(commit.isPresent());
    assertEquals(1, commit.get().getGeneration());
  }

  @Test
  public void testGetCommit_generation_notfound() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    assertFalse(commits.getCommit(10).isPresent());
  }

  @Test
  public void testGetFiles() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    List<File> files = commits.getFiles(1);
    assertTrue(files.size() > 0);
    assertTrue(files.stream().anyMatch(file -> file.getFileName().equals("segments_1")));
  }

  @Test
  public void testGetFiles_generation_notfound() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    assertTrue(commits.getFiles(10).isEmpty());
  }

  @Test
  public void testGetSegments() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    List<Segment> segments = commits.getSegments(1);
    assertTrue(segments.size() > 0);
  }

  @Test
  public void testGetSegments_generation_notfound() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    assertTrue(commits.getSegments(10).isEmpty());
  }

  @Test
  public void testGetSegmentAttributes() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    Map<String, String> attributes = commits.getSegmentAttributes(1, "_0");
    assertTrue(attributes.size() > 0);
  }

  @Test
  public void testGetSegmentAttributes_generation_notfound() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    Map<String, String> attributes = commits.getSegmentAttributes(3, "_0");
    assertTrue(attributes.isEmpty());
  }

  @Test
  public void testGetSegmentAttributes_invalid_name() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    Map<String, String> attributes = commits.getSegmentAttributes(1, "xxx");
    assertTrue(attributes.isEmpty());
  }

  @Test
  public void testGetSegmentDiagnostics() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    Map<String, String> diagnostics = commits.getSegmentDiagnostics(1, "_0");
    assertTrue(diagnostics.size() > 0);
  }

  @Test
  public void testGetSegmentDiagnostics_generation_notfound() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    assertTrue(commits.getSegmentDiagnostics(10, "_0").isEmpty());
  }


  @Test
  public void testGetSegmentDiagnostics_invalid_name() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    Map<String, String> diagnostics = commits.getSegmentDiagnostics(1,"xxx");
    assertTrue(diagnostics.isEmpty());
  }

  @Test
  public void testSegmentCodec() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    Optional<Codec> codec = commits.getSegmentCodec(1, "_0");
    assertTrue(codec.isPresent());
  }

  @Test
  public void testSegmentCodec_generation_notfound() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    Optional<Codec> codec = commits.getSegmentCodec(10, "_0");
    assertFalse(codec.isPresent());
  }

  @Test
  public void testSegmentCodec_invalid_name() {
    CommitsImpl commits = new CommitsImpl(reader, indexDir.toString());
    Optional<Codec> codec = commits.getSegmentCodec(1, "xxx");
    assertFalse(codec.isPresent());

  }
}
