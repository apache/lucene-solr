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
package org.apache.lucene.index;


import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.mockfile.ExtrasFS;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestSegmentInfos extends LuceneTestCase {

  public void testIllegalCreatedVersion() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SegmentInfos(5));
    assertEquals("indexCreatedVersionMajor must be >= 6, got: 5", e.getMessage());
    e = expectThrows(IllegalArgumentException.class, () -> new SegmentInfos(Version.LATEST.major + 1));
    assertEquals("indexCreatedVersionMajor is in the future: " + (Version.LATEST.major + 1), e.getMessage());
  }

  // LUCENE-5954
  public void testVersionsNoSegments() throws IOException {
    SegmentInfos sis = new SegmentInfos(Version.LATEST.major);
    BaseDirectoryWrapper dir = newDirectory();
    dir.setCheckIndexOnClose(false);
    sis.commit(dir);
    sis = SegmentInfos.readLatestCommit(dir);
    assertNull(sis.getMinSegmentLuceneVersion());
    assertEquals(Version.LATEST, sis.getCommitLuceneVersion());
    dir.close();
  }

  // LUCENE-5954
  public void testVersionsOneSegment() throws IOException {
    BaseDirectoryWrapper dir = newDirectory();
    dir.setCheckIndexOnClose(false);
    byte id[] = StringHelper.randomId();
    Codec codec = Codec.getDefault();

    SegmentInfos sis = new SegmentInfos(Version.LATEST.major);
    SegmentInfo info = new SegmentInfo(dir, Version.LUCENE_8_0_0, Version.LUCENE_8_0_0, "_0", 1, false, Codec.getDefault(),
                                       Collections.<String,String>emptyMap(), id, Collections.<String,String>emptyMap(), null);
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    SegmentCommitInfo commitInfo = new SegmentCommitInfo(info, 0, 0, -1, -1, -1, StringHelper.randomId());

    sis.add(commitInfo);
    sis.commit(dir);
    sis = SegmentInfos.readLatestCommit(dir);
    assertEquals(Version.LUCENE_8_0_0, sis.getMinSegmentLuceneVersion());
    assertEquals(Version.LATEST, sis.getCommitLuceneVersion());
    dir.close();
  }

  // LUCENE-5954
  public void testVersionsTwoSegments() throws IOException {
    BaseDirectoryWrapper dir = newDirectory();
    dir.setCheckIndexOnClose(false);
    byte id[] = StringHelper.randomId();
    Codec codec = Codec.getDefault();

    SegmentInfos sis = new SegmentInfos(Version.LATEST.major);
    SegmentInfo info = new SegmentInfo(dir, Version.LUCENE_8_0_0, Version.LUCENE_8_0_0, "_0", 1, false, Codec.getDefault(),
                                       Collections.<String,String>emptyMap(), id, Collections.<String,String>emptyMap(), null);
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    SegmentCommitInfo commitInfo = new SegmentCommitInfo(info, 0, 0, -1, -1, -1, StringHelper.randomId());
    sis.add(commitInfo);

    info = new SegmentInfo(dir, Version.LUCENE_8_0_0, Version.LUCENE_8_0_0, "_1", 1, false, Codec.getDefault(),
                           Collections.<String,String>emptyMap(), id, Collections.<String,String>emptyMap(), null);
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    commitInfo = new SegmentCommitInfo(info, 0, 0,-1, -1, -1, StringHelper.randomId());
    sis.add(commitInfo);

    sis.commit(dir);
    byte[] commitInfoId0 = sis.info(0).getId();
    byte[] commitInfoId1 = sis.info(1).getId();
    sis = SegmentInfos.readLatestCommit(dir);
    assertEquals(Version.LUCENE_8_0_0, sis.getMinSegmentLuceneVersion());
    assertEquals(Version.LATEST, sis.getCommitLuceneVersion());
    assertEquals(StringHelper.idToString(commitInfoId0), StringHelper.idToString(sis.info(0).getId()));
    assertEquals(StringHelper.idToString(commitInfoId1), StringHelper.idToString(sis.info(1).getId()));
    dir.close();
  }

  /** Test toString method */
  public void testToString() throws Throwable{
    SegmentInfo si;
    final Directory dir = newDirectory();
    Codec codec = Codec.getDefault();

    // diagnostics map
    Map<String, String> diagnostics = new LinkedHashMap<>();
    diagnostics.put("key1", "value1");
    diagnostics.put("key2", "value2");

    // attributes map
    Map<String,String> attributes = new LinkedHashMap<>();
    attributes.put("key1", "value1");
    attributes.put("key2", "value2");

    // diagnostics X, attributes X
    si = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "TEST", 10000, false, codec, Collections.emptyMap(), StringHelper.randomId(), new HashMap<>(), Sort.INDEXORDER);
    assertEquals("TEST(" + Version.LATEST.toString() + ")" +
        ":C10000" +
        ":[indexSort=<doc>]", si.toString());

    // diagnostics O, attributes X
    si = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "TEST", 10000, false, codec, diagnostics, StringHelper.randomId(), new HashMap<>(), Sort.INDEXORDER);
    assertEquals("TEST(" + Version.LATEST.toString() + ")" +
        ":C10000" +
        ":[indexSort=<doc>]" +
        ":[diagnostics={key1=value1, key2=value2}]", si.toString());

    // diagnostics X, attributes O
    si = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "TEST", 10000, false, codec, Collections.emptyMap(), StringHelper.randomId(), attributes, Sort.INDEXORDER);
    assertEquals("TEST(" + Version.LATEST.toString() + ")" +
        ":C10000" +
        ":[indexSort=<doc>]" +
        ":[attributes={key1=value1, key2=value2}]", si.toString());

    // diagnostics O, attributes O
    si = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "TEST", 10000, false, codec, diagnostics, StringHelper.randomId(), attributes, Sort.INDEXORDER);
    System.out.println(si.toString());
    assertEquals("TEST(" + Version.LATEST.toString() + ")" +
        ":C10000" +
        ":[indexSort=<doc>]" +
        ":[diagnostics={key1=value1, key2=value2}]" +
        ":[attributes={key1=value1, key2=value2}]", si.toString());

    dir.close();
  }

  public void testIDChangesOnAdvance() throws IOException {
    try (BaseDirectoryWrapper dir = newDirectory()) {
      dir.setCheckIndexOnClose(false);
      byte id[] = StringHelper.randomId();
      SegmentInfo info = new SegmentInfo(dir, Version.LUCENE_8_6_0, Version.LUCENE_8_6_0, "_0", 1, false, Codec.getDefault(),
          Collections.<String, String>emptyMap(), StringHelper.randomId(), Collections.<String, String>emptyMap(), null);
      SegmentCommitInfo commitInfo = new SegmentCommitInfo(info, 0, 0, -1, -1, -1, id);
      assertEquals(StringHelper.idToString(id), StringHelper.idToString(commitInfo.getId()));
      commitInfo.advanceDelGen();
      assertNotEquals(StringHelper.idToString(id), StringHelper.idToString(commitInfo.getId()));

      id = commitInfo.getId();
      commitInfo.advanceDocValuesGen();
      assertNotEquals(StringHelper.idToString(id), StringHelper.idToString(commitInfo.getId()));

      id = commitInfo.getId();
      commitInfo.advanceFieldInfosGen();
      assertNotEquals(StringHelper.idToString(id), StringHelper.idToString(commitInfo.getId()));
      SegmentCommitInfo clone = commitInfo.clone();
      id = commitInfo.getId();
      assertEquals(StringHelper.idToString(id), StringHelper.idToString(commitInfo.getId()));
      assertEquals(StringHelper.idToString(id), StringHelper.idToString(clone.getId()));

      commitInfo.advanceFieldInfosGen();
      assertNotEquals(StringHelper.idToString(id), StringHelper.idToString(commitInfo.getId()));
      assertEquals("clone changed but shouldn't", StringHelper.idToString(id), StringHelper.idToString(clone.getId()));
    }
  }

  public void testBitFlippedTriggersCorruptIndexException() throws IOException {
    BaseDirectoryWrapper dir = newDirectory();
    dir.setCheckIndexOnClose(false);
    byte id[] = StringHelper.randomId();
    Codec codec = Codec.getDefault();

    SegmentInfos sis = new SegmentInfos(Version.LATEST.major);
    SegmentInfo info = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "_0", 1, false, Codec.getDefault(),
                                       Collections.<String,String>emptyMap(), id, Collections.<String,String>emptyMap(), null);
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    SegmentCommitInfo commitInfo = new SegmentCommitInfo(info, 0, 0, -1, -1, -1, StringHelper.randomId());
    sis.add(commitInfo);

    info = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "_1", 1, false, Codec.getDefault(),
                           Collections.<String,String>emptyMap(), id, Collections.<String,String>emptyMap(), null);
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    commitInfo = new SegmentCommitInfo(info, 0, 0,-1, -1, -1, StringHelper.randomId());
    sis.add(commitInfo);

    sis.commit(dir);

    BaseDirectoryWrapper corruptDir = newDirectory();
    corruptDir.setCheckIndexOnClose(false);
    boolean corrupt = false;
    for (String file : dir.listAll()) {
      if (file.startsWith(IndexFileNames.SEGMENTS)) {
        try (IndexInput in = dir.openInput(file, IOContext.DEFAULT);
            IndexOutput out = corruptDir.createOutput(file, IOContext.DEFAULT)) {
          final long corruptIndex = TestUtil.nextLong(random(), 0, in.length() - 1);
          out.copyBytes(in, corruptIndex);
          final int b = Byte.toUnsignedInt(in.readByte()) + TestUtil.nextInt(random(), 0x01, 0xff);
          out.writeByte((byte) b);
          out.copyBytes(in, in.length() - in.getFilePointer());
        }
        try (IndexInput in = corruptDir.openInput(file, IOContext.DEFAULT)) {
          CodecUtil.checksumEntireFile(in);
          if (VERBOSE) {
            System.out.println("TEST: Altering the file did not update the checksum, aborting...");
          }
          return;
        } catch (CorruptIndexException e) {
          // ok
        }
        corrupt = true;
      } else if (ExtrasFS.isExtra(file) == false) {
        corruptDir.copyFrom(dir, file, file, IOContext.DEFAULT);
      }
    }
    assertTrue("No segments file found", corrupt);

    expectThrowsAnyOf(
        Arrays.asList(CorruptIndexException.class, IndexFormatTooOldException.class, IndexFormatTooNewException.class),
        () -> SegmentInfos.readLatestCommit(corruptDir));
    dir.close();
    corruptDir.close();
  }
}

