package org.apache.lucene.index;

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
import java.util.List;

import org.apache.lucene.util.BytesRef;

public class MultiSimpleDocValues {

  public static NumericDocValues simpleNormValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    boolean anyReal = false;
    for(AtomicReaderContext ctx : leaves) {
      NumericDocValues norms = ctx.reader().simpleNormValues(field);

      if (norms == null) {
        norms = NumericDocValues.EMPTY;
      } else {
        anyReal = true;
      }
    }

    if (!anyReal) {
      return null;
    } else {
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          int subIndex = ReaderUtil.subIndex(docID, leaves);
          NumericDocValues norms;
          try {
            norms = leaves.get(subIndex).reader().simpleNormValues(field);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
          if (norms == null) {
            return 0;
          } else {
            return norms.get(docID - leaves.get(subIndex).docBase);
          }
        }
      };
    }
  }

  public static NumericDocValues simpleNumericValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    boolean anyReal = false;
    for(AtomicReaderContext ctx : leaves) {
      NumericDocValues values = ctx.reader().getNumericDocValues(field);

      if (values == null) {
        values = NumericDocValues.EMPTY;
      } else {
        anyReal = true;
      }
    }

    if (!anyReal) {
      return null;
    } else {
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          int subIndex = ReaderUtil.subIndex(docID, leaves);
          NumericDocValues values;
          try {
            values = leaves.get(subIndex).reader().getNumericDocValues(field);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
          if (values == null) {
            return 0;
          } else {
            return values.get(docID - leaves.get(subIndex).docBase);
          }
        }
      };
    }
  }

  public static BinaryDocValues simpleBinaryValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    boolean anyReal = false;

    for(AtomicReaderContext ctx : leaves) {
      BinaryDocValues values = ctx.reader().getBinaryDocValues(field);

      if (values == null) {
        values = BinaryDocValues.EMPTY;
      } else {
        anyReal = true;
      }
    }

    if (!anyReal) {
      return null;
    } else {

      return new BinaryDocValues() {
        @Override
        public void get(int docID, BytesRef result) {
          int subIndex = ReaderUtil.subIndex(docID, leaves);
          BinaryDocValues values;
          try {
            values = leaves.get(subIndex).reader().getBinaryDocValues(field);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
          if (values != null) {
            values.get(docID - leaves.get(subIndex).docBase, result);
          } else {
            result.length = 0;
            result.bytes = BinaryDocValues.MISSING;
          }
        }
      };
    }
  }

}
