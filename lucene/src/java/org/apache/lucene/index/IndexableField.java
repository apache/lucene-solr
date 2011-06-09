package org.apache.lucene.index;

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

import java.io.Reader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.util.BytesRef;

// nocommit jdocs

// nocommit maybe abstract class instead...?  or maybe we
// have versioned interfaces over time, so indexer can
// consume JARs w/ older doc/field impls?  IndexableField1,
// IndexableField2, ...?

// nocommit maybe take this further and push analysis into Document

// nocommit what to do about multi-valued fields?  really,
// indexer should not know?

// nocommit make test case showing how you can index
// Iterable<IndexableField> that is not a Document
// instance...

// nocommit -- rename all this stuff -- remove get/is?  --
// this is all read only APIs

/** @lucene.experimental */
public interface IndexableField {
  // nocommit: attrs?
  // nocommit: doc values?
  // nocommit: sorted?

  public String name();

  // NOTE: if doc/field impl has the notion of "doc level boost"
  // it must be multiplied in w/ this field's boost
  public float boost();
  
  public boolean stored();

  // nocommit -- isBinary?
  public BytesRef binaryValue(BytesRef reuse);
  public String stringValue();
  public Reader readerValue();

  // nocommit -- decouple analyzers here: field impl should
  // go and ask analyzer for the token stream, so indexer
  // doesn't have to ask for string/reader value and then consult
  // analyzer 
  public TokenStream tokenStreamValue();

  // Numeric field:
  public boolean numeric();
  public NumericField.DataType numericDataType();
  public Number numericValue();

  // If this returns non-null then we index this field:
  public boolean indexed();

  // nocommit maybe remove?  only needed because stored
  // fields records this!  (well, and because analysis isn't
  // yet decoupled)
  public boolean tokenized();
  public boolean omitNorms();
  public boolean omitTermFreqAndPositions();

  public boolean storeTermVectors();
  public boolean storeTermVectorOffsets();
  public boolean storeTermVectorPositions();
}