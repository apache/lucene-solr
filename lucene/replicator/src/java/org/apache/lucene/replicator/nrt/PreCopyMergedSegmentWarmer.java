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

package org.apache.lucene.replicator.nrt;

/** A merged segment warmer that pre-copies the merged segment out to
 *  replicas before primary cuts over to the merged segment.  This
 *  ensures that NRT reopen time on replicas is only in proportion to
 *  flushed segment sizes, not merged segments. */

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;

// TODO: or ... replica node can do merging locally?  tricky to keep things in sync, when one node merges more slowly than others...

class PreCopyMergedSegmentWarmer implements IndexReaderWarmer {

  private final PrimaryNode primary;

  public PreCopyMergedSegmentWarmer(PrimaryNode primary) {
    this.primary = primary;
  }

  @Override
  public void warm(LeafReader reader) throws IOException {
    long startNS = System.nanoTime();
    final SegmentCommitInfo info = ((SegmentReader) reader).getSegmentInfo();
    //System.out.println("TEST: warm merged segment files " + info);
    Map<String,FileMetaData> filesMetaData = new HashMap<>();
    for (String fileName : info.files()) {
      FileMetaData metaData = primary.readLocalFileMetaData(fileName);
      assert metaData != null;
      assert filesMetaData.containsKey(fileName) == false;
      filesMetaData.put(fileName, metaData);
    }

    primary.preCopyMergedSegmentFiles(info, filesMetaData);
    primary.message(String.format(Locale.ROOT, "top: done warm merge " + info + ": took %.3f sec, %.1f MB", (System.nanoTime()-startNS)/1000000000., info.sizeInBytes()/1024/1024.));
    primary.finishedMergedFiles.addAll(filesMetaData.keySet());
  }
}
