package org.apache.lucene.codecs.lucene3x;

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

import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.Directory;

/**
 * Read-Only Lucene 3.x Norms Format
 * 
 * @lucene.experimental
 */
public class Lucene3xNormsFormat extends NormsFormat {


  @Override
  public void files(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    Lucene3xNormsProducer.files(dir, info, files);
  }

  @Override
  public void separateFiles(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    Lucene3xNormsProducer.separateFiles(dir, info, files);
  }


  @Override
  public PerDocConsumer docsConsumer(PerDocWriteState state) throws IOException {
    throw new IllegalArgumentException("this codec can only be used for reading");
  }

  @Override
  public PerDocProducer docsProducer(SegmentReadState state) throws IOException {
    return docsProducer(state, null);
  }

  @Override
  public PerDocProducer docsProducer(SegmentReadState state,
      Directory separateNormsDir) throws IOException {
    return new Lucene3xNormsProducer(state.dir, state.segmentInfo, state.fieldInfos, state.context, separateNormsDir);
  }
}
