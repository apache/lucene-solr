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
import java.util.Comparator;
import java.util.Set;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.values.Writer;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;

/**
 * 
 * @lucene.experimental
 */
public class DefaultDocValuesConsumer extends PerDocConsumer {
  private final String segmentName;
  private final int codecId;
  private final Directory directory;
  private final Counter bytesUsed;
  private final Comparator<BytesRef> comparator;
  private boolean useCompoundFile;
  private final IOContext context;
  
  public DefaultDocValuesConsumer(PerDocWriteState state, Comparator<BytesRef> comparator, boolean useCompoundFile) throws IOException {
    this.segmentName = state.segmentName;
    this.codecId = state.codecId;
    this.bytesUsed = state.bytesUsed;
    this.context = state.context;
    //TODO maybe we should enable a global CFS that all codecs can pull on demand to further reduce the number of files?
    this.directory = useCompoundFile ? new CompoundFileDirectory(state.directory,
        IndexFileNames.segmentFileName(segmentName, codecId,
            IndexFileNames.COMPOUND_FILE_EXTENSION), context, true) : state.directory;
    this.comparator = comparator;
    this.useCompoundFile = useCompoundFile;
  }

  public void close() throws IOException {
    if (useCompoundFile) {
      this.directory.close();
    }
  }

  @Override
  public DocValuesConsumer addValuesField(FieldInfo field) throws IOException {
    return Writer.create(field.getDocValues(),
        docValuesId(segmentName, codecId, field.number),
        directory, comparator, bytesUsed, context);
  }
  
  @SuppressWarnings("fallthrough")
  public static void files(Directory dir, SegmentInfo segmentInfo, int codecId,
      Set<String> files, boolean useCompoundFile) throws IOException {
    FieldInfos fieldInfos = segmentInfo.getFieldInfos();
    for (FieldInfo fieldInfo : fieldInfos) {
      if (fieldInfo.getCodecId() == codecId && fieldInfo.hasDocValues()) {
        String filename = docValuesId(segmentInfo.name, codecId,
            fieldInfo.number);
        if (useCompoundFile) {
          files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecId, IndexFileNames.COMPOUND_FILE_EXTENSION));
          files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecId, IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION));
          assert dir.fileExists(IndexFileNames.segmentFileName(segmentInfo.name, codecId, IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION)); 
          assert dir.fileExists(IndexFileNames.segmentFileName(segmentInfo.name, codecId, IndexFileNames.COMPOUND_FILE_EXTENSION)); 
          return;
        } else {
          switch (fieldInfo.getDocValues()) {
          case BYTES_FIXED_DEREF:
          case BYTES_VAR_DEREF:
          case BYTES_VAR_SORTED:
          case BYTES_FIXED_SORTED:
          case BYTES_VAR_STRAIGHT:
            files.add(IndexFileNames.segmentFileName(filename, "",
                Writer.INDEX_EXTENSION));
            assert dir.fileExists(IndexFileNames.segmentFileName(filename, "",
                Writer.INDEX_EXTENSION));
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
                Writer.DATA_EXTENSION));
            assert dir.fileExists(IndexFileNames.segmentFileName(filename, "",
                Writer.DATA_EXTENSION));
            break;
        
          default:
            assert false;
          }
        }
      }
    }
  }
  

  static String docValuesId(String segmentsName, int codecID, int fieldId) {
    return segmentsName + "_" + codecID + "-" + fieldId;
  }
  
  public static void getDocValuesExtensions(Set<String> extensions, boolean useCompoundFile) {
    if (useCompoundFile) {
      extensions.add(IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION);
      extensions.add(IndexFileNames.COMPOUND_FILE_EXTENSION);
    } else {
      extensions.add(Writer.DATA_EXTENSION);
      extensions.add(Writer.INDEX_EXTENSION);
    }
  }

}
