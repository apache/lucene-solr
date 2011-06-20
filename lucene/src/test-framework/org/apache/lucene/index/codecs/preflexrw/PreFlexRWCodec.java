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

import java.io.IOException;

import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.codecs.preflex.PreFlexCodec;
import org.apache.lucene.index.codecs.preflex.PreFlexFields;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.util.LuceneTestCase;

/** Codec, only for testing, that can write and read the
 *  pre-flex index format.
 *
 * @lucene.experimental
 */
public class PreFlexRWCodec extends PreFlexCodec {

  public PreFlexRWCodec() {
    // NOTE: we impersonate the PreFlex codec so that it can
    // read the segments we write!
    super();
  }
  
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new PreFlexFieldsWriter(state);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {

    // Whenever IW opens readers, eg for merging, we have to
    // keep terms order in UTF16:

    return new PreFlexFields(state.dir, state.fieldInfos, state.segmentInfo, state.context, state.termsIndexDivisor) {
      @Override
      protected boolean sortTermsByUnicode() {
        // We carefully peek into stack track above us: if
        // we are part of a "merge", we must sort by UTF16:
        boolean unicodeSortOrder = true;

        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          //System.out.println(trace[i].getClassName());
          if ("merge".equals(trace[i].getMethodName())) {
            unicodeSortOrder = false;
            if (LuceneTestCase.VERBOSE) {
              System.out.println("NOTE: PreFlexRW codec: forcing legacy UTF16 term sort order");
            }
            break;
          }
        }

        return unicodeSortOrder;
      }
    };
  }
}
