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

import org.apache.lucene.codecs.PostingsBaseFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/** 
 * Provides a {@link PostingsReaderBase} and {@link
 * PostingsWriterBase}.
 *
 * @lucene.experimental */

// TODO: should these also be named / looked up via SPI?
public final class Lucene40PostingsBaseFormat extends PostingsBaseFormat {

  public Lucene40PostingsBaseFormat() {
    super("Lucene40");
  }

  @Override
  public PostingsReaderBase postingsReaderBase(SegmentReadState state) throws IOException {
    return new Lucene40PostingsReader(state.dir, state.segmentInfo, state.context, state.segmentSuffix);
  }

  @Override
  public PostingsWriterBase postingsWriterBase(SegmentWriteState state) throws IOException {
    return new Lucene40PostingsWriter(state);
  }
  
  @Override
  public void files(SegmentInfo segmentInfo, String segmentSuffix, Set<String> files) throws IOException {
    Lucene40PostingsReader.files(segmentInfo, segmentSuffix, files);
  }
}
