package org.apache.lucene.index.codecs.simpletext;

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

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.DefaultDocValuesFormat;
import org.apache.lucene.index.codecs.DefaultSegmentInfosFormat;
import org.apache.lucene.index.codecs.DocValuesFormat;
import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.SegmentInfosFormat;
import org.apache.lucene.index.codecs.StoredFieldsFormat;

/**
 * plain text index format.
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * @lucene.experimental
 */
public final class SimpleTextCodec extends Codec {
  private final PostingsFormat postings = new SimpleTextPostingsFormat();
  private final StoredFieldsFormat storedFields = new SimpleTextStoredFieldsFormat();
  
  // TODO: need a plain-text impl
  private final DocValuesFormat docValues = new DefaultDocValuesFormat();
  // TODO: need a plain-text impl
  private final SegmentInfosFormat segmentInfos = new DefaultSegmentInfosFormat();
  
  public SimpleTextCodec() {
    super("SimpleText");
  }
  
  @Override
  public PostingsFormat postingsFormat() {
    return postings;
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return docValues;
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return storedFields;
  }

  @Override
  public SegmentInfosFormat segmentInfosFormat() {
    return segmentInfos;
  }
}
