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
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.Collections;

public class TestSegmentInfos extends LuceneTestCase {

  // LUCENE-5954
  public void testVersionsNoSegments() throws IOException {
    SegmentInfos sis = new SegmentInfos();
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

    SegmentInfos sis = new SegmentInfos();
    SegmentInfo info = new SegmentInfo(dir, Version.LUCENE_5_0_0, "_0", 1, false, Codec.getDefault(), 
                                       Collections.<String,String>emptyMap(), id, Collections.<String,String>emptyMap());
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    SegmentCommitInfo commitInfo = new SegmentCommitInfo(info, 0, -1, -1, -1);

    sis.add(commitInfo);
    sis.commit(dir);
    sis = SegmentInfos.readLatestCommit(dir);
    assertEquals(Version.LUCENE_5_0_0, sis.getMinSegmentLuceneVersion());
    assertEquals(Version.LATEST, sis.getCommitLuceneVersion());
    dir.close();
  }

  // LUCENE-5954
  public void testVersionsTwoSegments() throws IOException {
    BaseDirectoryWrapper dir = newDirectory();
    dir.setCheckIndexOnClose(false);
    byte id[] = StringHelper.randomId();
    Codec codec = Codec.getDefault();

    SegmentInfos sis = new SegmentInfos();
    SegmentInfo info = new SegmentInfo(dir, Version.LUCENE_5_0_0, "_0", 1, false, Codec.getDefault(), 
                                       Collections.<String,String>emptyMap(), id, Collections.<String,String>emptyMap());
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    SegmentCommitInfo commitInfo = new SegmentCommitInfo(info, 0, -1, -1, -1);
    sis.add(commitInfo);

    info = new SegmentInfo(dir, Version.LUCENE_5_1_0, "_1", 1, false, Codec.getDefault(), 
                                       Collections.<String,String>emptyMap(), id, Collections.<String,String>emptyMap());
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    commitInfo = new SegmentCommitInfo(info, 0, -1, -1, -1);
    sis.add(commitInfo);

    sis.commit(dir);
    sis = SegmentInfos.readLatestCommit(dir);
    assertEquals(Version.LUCENE_5_0_0, sis.getMinSegmentLuceneVersion());
    assertEquals(Version.LATEST, sis.getCommitLuceneVersion());
    dir.close();
  }
}

