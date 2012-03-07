package org.apache.lucene.codecs.sep;

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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.codecs.PerDocProducerBase;
import org.apache.lucene.codecs.lucene40.values.DocValuesWriterBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/**
 * Implementation of PerDocConsumer that uses separate files.
 * @lucene.experimental
 */
public class SepDocValuesConsumer extends DocValuesWriterBase {
  private final Directory directory;
  private final FieldInfos fieldInfos;

  public SepDocValuesConsumer(PerDocWriteState state) throws IOException {
    super(state);
    this.directory = state.directory;
    fieldInfos = state.fieldInfos;
  }
  
  @Override
  protected Directory getDirectory() {
    return directory;
  }

  public static void files(SegmentInfo segmentInfo,
      Set<String> files) throws IOException {
    files(segmentInfo.dir, segmentInfo.getFieldInfos(), segmentInfo.name, files);
  }
  
  @SuppressWarnings("fallthrough")
  private static void files(Directory dir,FieldInfos fieldInfos, String segmentName, Set<String> files)  {
    for (FieldInfo fieldInfo : fieldInfos) {
      if (fieldInfo.hasDocValues()) {
        String filename = PerDocProducerBase.docValuesId(segmentName, fieldInfo.number);
        switch (fieldInfo.getDocValuesType()) {
          case BYTES_FIXED_DEREF:
          case BYTES_VAR_DEREF:
          case BYTES_VAR_STRAIGHT:
          case BYTES_FIXED_SORTED:
          case BYTES_VAR_SORTED:
            files.add(IndexFileNames.segmentFileName(filename, "",
                INDEX_EXTENSION));
            try {
            assert dir.fileExists(IndexFileNames.segmentFileName(filename, "",
                INDEX_EXTENSION));
            } catch (IOException e) {
              // don't throw checked exception - dir is only used in assert 
              throw new RuntimeException(e);
            }
            // until here all types use an index
          case BYTES_FIXED_STRAIGHT:
          case FLOAT_32:
          case FLOAT_64:
          case VAR_INTS:
          case FIXED_INTS_16:
          case FIXED_INTS_32:
          case FIXED_INTS_64:
          case FIXED_INTS_8:
            files.add(IndexFileNames.segmentFileName(filename, "",
                DATA_EXTENSION));
          try {
            assert dir.fileExists(IndexFileNames.segmentFileName(filename, "",
                DATA_EXTENSION));
          } catch (IOException e) {
            // don't throw checked exception - dir is only used in assert
            throw new RuntimeException(e);
          }
            break;
          default:
            assert false;
        }
      }
    }
  }

  @Override
  public void abort() {
    Set<String> files = new HashSet<String>();
    files(directory, fieldInfos, segmentName, files);
    IOUtils.deleteFilesIgnoringExceptions(directory, files.toArray(new String[0]));
  }
}
