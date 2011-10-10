package org.apache.lucene.index.codecs;

/**
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

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.codecs.DocValuesWriterBase;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;

/**
 * Default PerDocConsumer implementation that uses compound file.
 * @lucene.experimental
 */
public class DefaultDocValuesConsumer extends DocValuesWriterBase {
  private final Directory directory;
  
  public DefaultDocValuesConsumer(PerDocWriteState state) throws IOException {
    super(state);
    //TODO maybe we should enable a global CFS that all codecs can pull on demand to further reduce the number of files?
    this.directory = new CompoundFileDirectory(state.directory,
        IndexFileNames.segmentFileName(state.segmentName, state.codecId,
            IndexFileNames.COMPOUND_FILE_EXTENSION), state.context, true);
  }
  
  @Override
  protected Directory getDirectory() {
    return directory;
  }

  @Override
  public void close() throws IOException {
    this.directory.close();
  }

  @SuppressWarnings("fallthrough")
  public static void files(Directory dir, SegmentInfo segmentInfo, int codecId, Set<String> files) throws IOException {
    FieldInfos fieldInfos = segmentInfo.getFieldInfos();
    for (FieldInfo fieldInfo : fieldInfos) {
      if (fieldInfo.getCodecId() == codecId && fieldInfo.hasDocValues()) {
        files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecId, IndexFileNames.COMPOUND_FILE_EXTENSION));
        files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecId, IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION));
        assert dir.fileExists(IndexFileNames.segmentFileName(segmentInfo.name, codecId, IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION)); 
        assert dir.fileExists(IndexFileNames.segmentFileName(segmentInfo.name, codecId, IndexFileNames.COMPOUND_FILE_EXTENSION)); 
        return;
      }
    }
  }
  
  public static void getExtensions(Set<String> extensions) {
    extensions.add(IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION);
    extensions.add(IndexFileNames.COMPOUND_FILE_EXTENSION);
  }
}
