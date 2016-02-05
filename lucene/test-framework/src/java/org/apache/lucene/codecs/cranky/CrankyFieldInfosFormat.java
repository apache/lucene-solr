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
package org.apache.lucene.codecs.cranky;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

class CrankyFieldInfosFormat extends FieldInfosFormat {
  final FieldInfosFormat delegate;
  final Random random;
  
  CrankyFieldInfosFormat(FieldInfosFormat delegate, Random random) {
    this.delegate = delegate;
    this.random = random;
  }

  @Override
  public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext) throws IOException {
    return delegate.read(directory, segmentInfo, segmentSuffix, iocontext);
  }

  @Override
  public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context) throws IOException {
    if (random.nextInt(100) == 0) {
      throw new IOException("Fake IOException from FieldInfosFormat.getFieldInfosWriter()");
    }
    delegate.write(directory, segmentInfo, segmentSuffix, infos, context);
  }
}
