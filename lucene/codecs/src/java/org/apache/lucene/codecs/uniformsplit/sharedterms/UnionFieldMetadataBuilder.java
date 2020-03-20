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

package org.apache.lucene.codecs.uniformsplit.sharedterms;

import org.apache.lucene.codecs.uniformsplit.FieldMetadata;
import org.apache.lucene.util.BytesRef;

/**
 * Builds a {@link FieldMetadata} that is the union of multiple {@link FieldMetadata}.
 *
 * @lucene.experimental
 */
public class UnionFieldMetadataBuilder {

  private long minStartBlockFP;
  private long maxEndBlockFP;
  private BytesRef maxLastTerm;

  public UnionFieldMetadataBuilder() {
    reset();
  }

  public UnionFieldMetadataBuilder reset() {
    maxEndBlockFP = Long.MIN_VALUE;
    minStartBlockFP = Long.MAX_VALUE;
    maxLastTerm = null;
    return this;
  }

  public UnionFieldMetadataBuilder addFieldMetadata(FieldMetadata fieldMetadata) {
    minStartBlockFP = Math.min(minStartBlockFP, fieldMetadata.getFirstBlockStartFP());
    maxEndBlockFP = Math.max(maxEndBlockFP, fieldMetadata.getLastBlockStartFP());
    if (maxLastTerm == null || maxLastTerm.compareTo(fieldMetadata.getLastTerm()) < 0) {
      maxLastTerm = fieldMetadata.getLastTerm();
    }
    return this;
  }

  public FieldMetadata build() {
    if (maxLastTerm == null) {
      throw new IllegalStateException("no field metadata was provided");
    }
    return new FieldMetadata(null, 0, false, minStartBlockFP, maxEndBlockFP, maxLastTerm);
  }
}
