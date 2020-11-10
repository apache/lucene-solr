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
package org.apache.lucene.codecs;


import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import java.io.Closeable;
import java.io.IOException;

/**
 * Class that plugs into term dictionaries, such as {@link
 * BlockTreeTermsWriter}, and handles writing postings.
 * 
 * @see PostingsReaderBase
 * @lucene.experimental
 */
// TODO: find a better name; this defines the API that the
// terms dict impls use to talk to a postings impl.
// TermsDict + PostingsReader/WriterBase == FieldsProducer/Consumer
public abstract class PostingsWriterBase implements Closeable {

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected PostingsWriterBase() {
  }

  /** Called once after startup, before any terms have been
   *  added.  Implementations typically write a header to
   *  the provided {@code termsOut}. */
  public abstract void init(IndexOutput termsOut, SegmentWriteState state) throws IOException;

  /** Write all postings for one term; use the provided
   *  {@link TermsEnum} to pull a {@link org.apache.lucene.index.PostingsEnum}.
   *  This method should not
   *  re-position the {@code TermsEnum}!  It is already
   *  positioned on the term that should be written.  This
   *  method must set the bit in the provided {@link
   *  FixedBitSet} for every docID written.  If no docs
   *  were written, this method should return null, and the
   *  terms dict will skip the term. */
  public abstract BlockTermState writeTerm(BytesRef term, TermsEnum termsEnum, FixedBitSet docsSeen, NormsProducer norms) throws IOException;

  /**
   * Encode metadata as long[] and byte[]. {@code absolute} controls whether 
   * current term is delta encoded according to latest term. 
   * Usually elements in {@code longs} are file pointers, so each one always 
   * increases when a new term is consumed. {@code out} is used to write generic
   * bytes, which are not monotonic.
   */
  public abstract void encodeTerm(DataOutput out, FieldInfo fieldInfo, BlockTermState state, boolean absolute) throws IOException;

  /** 
   * Sets the current field for writing. */
  public abstract void setField(FieldInfo fieldInfo);

  @Override
  public abstract void close() throws IOException;
}
