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
package org.apache.lucene.codecs.blockterms;

import java.io.IOException;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.blockterms.BlockTermsReader;
import org.apache.lucene.codecs.blockterms.BlockTermsWriter;
import org.apache.lucene.codecs.blockterms.FixedGapTermsIndexWriter;
import org.apache.lucene.codecs.blockterms.TermsIndexReaderBase;
import org.apache.lucene.codecs.blockterms.TermsIndexWriterBase;
import org.apache.lucene.codecs.blockterms.VariableGapTermsIndexReader;
import org.apache.lucene.codecs.blockterms.VariableGapTermsIndexWriter;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat; // javadocs
import org.apache.lucene.codecs.lucene50.Lucene50PostingsReader;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

// TODO: we could make separate base class that can wrap
// any PostingsFormat and make it ord-able...

/**
 * Customized version of {@link Lucene50PostingsFormat} that uses
 * {@link VariableGapTermsIndexWriter} with a fixed interval.
 */
public final class LuceneVarGapFixedInterval extends PostingsFormat {
  final int termIndexInterval;
  
  public LuceneVarGapFixedInterval() {
    this(FixedGapTermsIndexWriter.DEFAULT_TERM_INDEX_INTERVAL);
  }
  
  public LuceneVarGapFixedInterval(int termIndexInterval) {
    super("LuceneVarGapFixedInterval");
    this.termIndexInterval = termIndexInterval;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase docs = new Lucene50PostingsWriter(state);

    // TODO: should we make the terms index more easily
    // pluggable?  Ie so that this codec would record which
    // index impl was used, and switch on loading?
    // Or... you must make a new Codec for this?
    TermsIndexWriterBase indexWriter;
    boolean success = false;
    try {
      indexWriter = new VariableGapTermsIndexWriter(state, new VariableGapTermsIndexWriter.EveryNTermSelector(termIndexInterval));
      success = true;
    } finally {
      if (!success) {
        docs.close();
      }
    }

    success = false;
    try {
      // Must use BlockTermsWriter (not BlockTree) because
      // BlockTree doens't support ords (yet)...
      FieldsConsumer ret = new BlockTermsWriter(indexWriter, state, docs);
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          docs.close();
        } finally {
          indexWriter.close();
        }
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    PostingsReaderBase postings = new Lucene50PostingsReader(state);
    TermsIndexReaderBase indexReader;

    boolean success = false;
    try {
      indexReader = new VariableGapTermsIndexReader(state);
      success = true;
    } finally {
      if (!success) {
        postings.close();
      }
    }

    success = false;
    try {
      FieldsProducer ret = new BlockTermsReader(indexReader, postings, state);
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          postings.close();
        } finally {
          indexReader.close();
        }
      }
    }
  }

  /** Extension of freq postings file */
  static final String FREQ_EXTENSION = "frq";

  /** Extension of prox postings file */
  static final String PROX_EXTENSION = "prx";
}
