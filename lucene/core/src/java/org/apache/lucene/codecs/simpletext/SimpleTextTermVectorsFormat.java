package org.apache.lucene.codecs.simpletext;

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

import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * plain text term vectors format.
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * @lucene.experimental
 */
public class SimpleTextTermVectorsFormat extends TermVectorsFormat {

  @Override
  public TermVectorsReader vectorsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext context) throws IOException {
    return new SimpleTextTermVectorsReader(directory, segmentInfo, context);
  }

  @Override
  public TermVectorsWriter vectorsWriter(Directory directory, String segment, IOContext context) throws IOException {
    return new SimpleTextTermVectorsWriter(directory, segment, context);
  }

  @Override
  public void files(SegmentInfo info, Set<String> files) throws IOException {
    SimpleTextTermVectorsReader.files(info, files);
  }
}
