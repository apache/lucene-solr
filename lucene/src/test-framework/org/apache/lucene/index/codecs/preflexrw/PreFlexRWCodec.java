package org.apache.lucene.index.codecs.preflexrw;

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
import org.apache.lucene.index.codecs.DefaultFieldsFormat;
import org.apache.lucene.index.codecs.DocValuesFormat;
import org.apache.lucene.index.codecs.FieldsFormat;
import org.apache.lucene.index.codecs.PostingsFormat;

/**
 * Writes 3.x-like indexes (not perfect emulation yet) for testing only!
 * @lucene.experimental
 */
public class PreFlexRWCodec extends Codec {
  private final PostingsFormat postings = new PreFlexRWPostingsFormat();
  // TODO: we should emulate 3.x here as well
  private final FieldsFormat fields = new DefaultFieldsFormat();
  
  // TODO: really this should take a Version param, and emulate that version of lucene exactly...
  public PreFlexRWCodec() {
    super("Lucene3x"); // impersonate
  }

  @Override
  public PostingsFormat postingsFormat() {
    return postings;
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return null; // unsupported
  }

  @Override
  public FieldsFormat fieldsFormat() {
    return fields;
  }
}
