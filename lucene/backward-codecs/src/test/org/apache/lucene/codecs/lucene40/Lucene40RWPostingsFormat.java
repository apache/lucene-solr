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
package org.apache.lucene.codecs.lucene40;

import java.io.IOException;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.blocktree.Lucene40BlockTreeTermsWriter;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Read-write version of 4.0 postings format for testing
 * @deprecated for test purposes only
 */
@Deprecated
public final class Lucene40RWPostingsFormat extends Lucene40PostingsFormat {
  
  /** minimum items (terms or sub-blocks) per block for 4.0 BlockTree */
  final static int MIN_BLOCK_SIZE = 25;
  /** maximum items (terms or sub-blocks) per block for 4.0 BlockTree */
  final static int MAX_BLOCK_SIZE = 48;

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase docs = new Lucene40PostingsWriter(state);
    
    // TODO: should we make the terms index more easily
    // pluggable?  Ie so that this codec would record which
    // index impl was used, and switch on loading?
    // Or... you must make a new Codec for this?
    boolean success = false;
    try {
      FieldsConsumer ret = new Lucene40BlockTreeTermsWriter(state, docs, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
      success = true;
      return ret;
    } finally {
      if (!success) {
        docs.close();
      }
    }
  }
}
