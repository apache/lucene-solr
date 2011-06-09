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

// nocommit maybe take this further and push analysis into Document

// nocommit what to do about multi-valued fields?  really,
// indexer should not know?

/** @lucene.experimental */
public interface IndexableField {
  // nocommit: attrs?
  // nocommit: doc values?
  // nocommit: sorted?

  public String name();

  // nocommit -- make sure doc multplies its own boost in here:
  public float getBoost();
  
  public boolean isStored();

  // nocommit -- isBinary?
  public BytesRef binaryValue(BytesRef reuse);
  public String stringValue();
  public Reader readerValue();
  public TokenStream tokenStreamValue();

  // Numeric field:
  public boolean isNumeric();
  public NumericField.DataType getDataType();
  public Number getNumericValue();

  // If this returns non-null then we index this field:
  public boolean isIndexed();
  // nocommit maybe remove?  only needed because stored
  // fields records this!
  public boolean isTokenized();
  public boolean getOmitNorms();
  public boolean getOmitTermFreqAndPositions();

  public boolean isTermVectorStored();
  public boolean isStoreOffsetWithTermVector();
  public boolean isStorePositionWithTermVector();
}