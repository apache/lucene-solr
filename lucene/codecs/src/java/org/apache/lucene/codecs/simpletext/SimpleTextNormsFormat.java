package org.apache.lucene.codecs.simpletext;

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
import java.util.Comparator;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.simpletext.SimpleTextDocValuesFormat.SimpleTextDocValuesReader;
import org.apache.lucene.codecs.simpletext.SimpleTextDocValuesFormat.SimpleTextDocValuesWriter;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;

/**
 * plain-text norms format.
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * 
 * @lucene.experimental
 */
public class SimpleTextNormsFormat extends NormsFormat {
  // nocommit put back to len once we replace current norms format:
  private static final String NORMS_SEG_EXTENSION = "slen";
  
  @Override
  public DocValuesConsumer normsConsumer(SegmentWriteState state) throws IOException {
    return new SimpleTextSimpleNormsConsumer(state);
  }
  
  @Override
  public DocValuesProducer normsProducer(SegmentReadState state) throws IOException {
    return new SimpleTextSimpleNormsProducer(state);
  }
  
  /**
   * Reads plain-text norms.
   * <p>
   * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
   * 
   * @lucene.experimental
   */
  public static class SimpleTextSimpleNormsProducer extends SimpleTextDocValuesReader {
    public SimpleTextSimpleNormsProducer(SegmentReadState state) throws IOException {
      // All we do is change the extension from .dat -> .len;
      // otherwise this is a normal simple doc values file:
      super(state, NORMS_SEG_EXTENSION);
    }
  }
  
  /**
   * Writes plain-text norms.
   * <p>
   * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
   * 
   * @lucene.experimental
   */
  public static class SimpleTextSimpleNormsConsumer extends SimpleTextDocValuesWriter {
    public SimpleTextSimpleNormsConsumer(SegmentWriteState state) throws IOException {
      // All we do is change the extension from .dat -> .len;
      // otherwise this is a normal simple doc values file:
      super(state, NORMS_SEG_EXTENSION);
    }
  }
}
