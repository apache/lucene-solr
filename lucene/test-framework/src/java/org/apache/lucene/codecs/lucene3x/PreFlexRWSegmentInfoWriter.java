package org.apache.lucene.codecs.lucene3x;

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

import java.io.IOException;

import org.apache.lucene.codecs.SegmentInfoWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * PreFlex implementation of {@link SegmentInfoWriter}.
 * @lucene.experimental
 */
class PreFlexRWSegmentInfoWriter extends SegmentInfoWriter {

  // NOTE: this is not "really" 3.x format, because we are
  // writing each SI to its own file, vs 3.x where the list
  // of segments and SI for each segment is written into a
  // single segments_N file

  /** Save a single segment's info. */
  @Override
  public void write(Directory dir, SegmentInfo si, FieldInfos fis, IOContext ioContext) throws IOException {
    SegmentInfos.write3xInfo(dir, si, ioContext);
  }
}
