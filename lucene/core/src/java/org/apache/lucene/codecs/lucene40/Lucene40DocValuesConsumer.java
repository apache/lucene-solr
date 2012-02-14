package org.apache.lucene.codecs.lucene40;

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

import org.apache.lucene.codecs.lucene40.values.DocValuesWriterBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/**
 * Default PerDocConsumer implementation that uses compound file.
 * @lucene.experimental
 */
public class Lucene40DocValuesConsumer extends DocValuesWriterBase {
  private final Directory mainDirectory;
  private Directory directory;
  private final String segmentSuffix;
  public final static String DOC_VALUES_SEGMENT_SUFFIX = "dv";


  public Lucene40DocValuesConsumer(PerDocWriteState state, String segmentSuffix) throws IOException {
    super(state);
    this.segmentSuffix = segmentSuffix;
    mainDirectory = state.directory;
    //TODO maybe we should enable a global CFS that all codecs can pull on demand to further reduce the number of files?
  }
  
  @Override
  protected Directory getDirectory() throws IOException {
    // lazy init
    if (directory == null) {
      directory = new CompoundFileDirectory(mainDirectory,
                                            IndexFileNames.segmentFileName(segmentName, segmentSuffix,
                                                                           IndexFileNames.COMPOUND_FILE_EXTENSION), context, true);
    }
    return directory;
  }

  @Override
  public void close() throws IOException {
    if (directory != null) {
      directory.close();
    }
  }

  public static void files(SegmentInfo segmentInfo, Set<String> files) throws IOException {
    FieldInfos fieldInfos = segmentInfo.getFieldInfos();
    for (FieldInfo fieldInfo : fieldInfos) {
      if (fieldInfo.hasDocValues()) {
        files.add(IndexFileNames.segmentFileName(segmentInfo.name, DOC_VALUES_SEGMENT_SUFFIX, IndexFileNames.COMPOUND_FILE_EXTENSION));
        files.add(IndexFileNames.segmentFileName(segmentInfo.name, DOC_VALUES_SEGMENT_SUFFIX, IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION));
        assert segmentInfo.dir.fileExists(IndexFileNames.segmentFileName(segmentInfo.name, DOC_VALUES_SEGMENT_SUFFIX, IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION)); 
        assert segmentInfo.dir.fileExists(IndexFileNames.segmentFileName(segmentInfo.name, DOC_VALUES_SEGMENT_SUFFIX, IndexFileNames.COMPOUND_FILE_EXTENSION)); 
        break;
      }
    }
  }

  @Override
  public void abort() {
    try {
      close();
    } catch (IOException ignored) {}
    IOUtils.deleteFilesIgnoringExceptions(mainDirectory, IndexFileNames.segmentFileName(
        segmentName, segmentSuffix, IndexFileNames.COMPOUND_FILE_EXTENSION),
        IndexFileNames.segmentFileName(segmentName, segmentSuffix,
            IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION));
  }
}
