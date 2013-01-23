package org.apache.lucene.codecs.lucene40;

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

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene42.Lucene42DocValuesFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

// nocommit: still a lie, but allows javadocs @links to work
// nocommit: make read-only and move to impersonator
public class Lucene40LyingDocValuesFormat extends Lucene42DocValuesFormat {

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return super.fieldsProducer(state);
  }
}
