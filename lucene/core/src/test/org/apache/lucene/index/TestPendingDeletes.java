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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

public class TestPendingDeletes extends LuceneTestCase {

  protected PendingDeletes newPendingDeletes(SegmentCommitInfo commitInfo) {
    return new PendingDeletes(commitInfo);
  }

  public void testDeleteDoc() throws IOException {
    RAMDirectory dir = new RAMDirectory();
    SegmentInfo si = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "test", 10, false, Codec.getDefault(),
        Collections.emptyMap(), StringHelper.randomId(), new HashMap<>(), null);
    SegmentCommitInfo commitInfo = new SegmentCommitInfo(si, 0, 0, -1, -1, -1, StringHelper.randomId());
    PendingDeletes deletes = newPendingDeletes(commitInfo);
    assertNull(deletes.getLiveDocs());
    int docToDelete = TestUtil.nextInt(random(), 0, 7);
    assertTrue(deletes.delete(docToDelete));
    assertNotNull(deletes.getLiveDocs());
    assertEquals(1, deletes.numPendingDeletes());

    Bits liveDocs = deletes.getLiveDocs();
    assertFalse(liveDocs.get(docToDelete));
    assertFalse(deletes.delete(docToDelete)); // delete again

    assertTrue(liveDocs.get(8));
    assertTrue(deletes.delete(8));
    assertTrue(liveDocs.get(8)); // we have a snapshot
    assertEquals(2, deletes.numPendingDeletes());

    assertTrue(liveDocs.get(9));
    assertTrue(deletes.delete(9));
    assertTrue(liveDocs.get(9));

    // now make sure new live docs see the deletions
    liveDocs = deletes.getLiveDocs();
    assertFalse(liveDocs.get(9));
    assertFalse(liveDocs.get(8));
    assertFalse(liveDocs.get(docToDelete));
    assertEquals(3, deletes.numPendingDeletes());
    dir.close();
  }

  public void testWriteLiveDocs() throws IOException {
    RAMDirectory dir = new RAMDirectory();
    SegmentInfo si = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "test", 6, false, Codec.getDefault(),
        Collections.emptyMap(), StringHelper.randomId(), new HashMap<>(), null);
    SegmentCommitInfo commitInfo = new SegmentCommitInfo(si, 0, 0,  -1, -1, -1, StringHelper.randomId());
    PendingDeletes deletes = newPendingDeletes(commitInfo);
    assertFalse(deletes.writeLiveDocs(dir));
    assertEquals(0, dir.listAll().length);
    boolean secondDocDeletes = random().nextBoolean();
    deletes.delete(5);
    if (secondDocDeletes) {
      deletes.getLiveDocs();
      deletes.delete(2);
    }
    assertEquals(-1, commitInfo.getDelGen());
    assertEquals(0, commitInfo.getDelCount());

    assertEquals(secondDocDeletes ? 2 : 1, deletes.numPendingDeletes());
    assertTrue(deletes.writeLiveDocs(dir));
    assertEquals(1, dir.listAll().length);
    Bits liveDocs = Codec.getDefault().liveDocsFormat().readLiveDocs(dir, commitInfo, IOContext.DEFAULT);
    assertFalse(liveDocs.get(5));
    if (secondDocDeletes) {
      assertFalse(liveDocs.get(2));
    } else {
      assertTrue(liveDocs.get(2));
    }
    assertTrue(liveDocs.get(0));
    assertTrue(liveDocs.get(1));
    assertTrue(liveDocs.get(3));
    assertTrue(liveDocs.get(4));

    assertEquals(0, deletes.numPendingDeletes());
    assertEquals(secondDocDeletes ? 2 : 1, commitInfo.getDelCount());
    assertEquals(1, commitInfo.getDelGen());

    deletes.delete(0);
    assertTrue(deletes.writeLiveDocs(dir));
    assertEquals(2, dir.listAll().length);
    liveDocs = Codec.getDefault().liveDocsFormat().readLiveDocs(dir, commitInfo, IOContext.DEFAULT);
    assertFalse(liveDocs.get(5));
    if (secondDocDeletes) {
      assertFalse(liveDocs.get(2));
    } else {
      assertTrue(liveDocs.get(2));
    }
    assertFalse(liveDocs.get(0));
    assertTrue(liveDocs.get(1));
    assertTrue(liveDocs.get(3));
    assertTrue(liveDocs.get(4));

    assertEquals(0, deletes.numPendingDeletes());
    assertEquals(secondDocDeletes ? 3 : 2, commitInfo.getDelCount());
    assertEquals(2, commitInfo.getDelGen());
    dir.close();
  }

  public void testIsFullyDeleted() throws IOException {
    RAMDirectory dir = new RAMDirectory();
    SegmentInfo si = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "test", 3, false, Codec.getDefault(),
        Collections.emptyMap(), StringHelper.randomId(), new HashMap<>(), null);
    SegmentCommitInfo commitInfo = new SegmentCommitInfo(si, 0, 0, -1, -1, -1, StringHelper.randomId());
    FieldInfos fieldInfos = FieldInfos.EMPTY;
    si.getCodec().fieldInfosFormat().write(dir, si, "", fieldInfos, IOContext.DEFAULT);
    PendingDeletes deletes = newPendingDeletes(commitInfo);
    for (int i = 0; i < 3; i++) {
      assertTrue(deletes.delete(i));
      if (random().nextBoolean()) {
        assertTrue(deletes.writeLiveDocs(dir));
      }
      assertEquals(i == 2, deletes.isFullyDeleted(() -> null));
    }
  }
}
