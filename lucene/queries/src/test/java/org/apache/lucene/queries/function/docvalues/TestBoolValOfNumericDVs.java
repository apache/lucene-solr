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
package org.apache.lucene.queries.function.docvalues;

import java.io.IOException;

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.util.LuceneTestCase;

/**
 * <p>
 * Sanity check that {@link FunctionValues#boolVal} behaves as expected for trivial subclasses of the various
 * (Numeric) DocValue implementations.
 * </p>
 * <p>
 * Any "non-zero" value should result in "true"
 * </p>
 */
public class TestBoolValOfNumericDVs extends LuceneTestCase {

  public void test() throws IOException {
    check(true);
    check(false);
  }
  
  public void check(final boolean expected) throws IOException {

    // create "constant" based instances of each superclass that should returned the expected value based on
    // the constant used
    final FunctionValues[] values = new FunctionValues[] {
      new FloatDocValues(null) {
        @Override
        public float floatVal(int doc) throws IOException {
          return expected ? Float.MIN_VALUE : 0.0F;
        }
      },
      new DoubleDocValues(null) {
        @Override
        public double doubleVal(int doc) throws IOException {
          return expected ? Double.MIN_VALUE : 0.0D;
        }
      },
      new IntDocValues(null) {
        @Override
        public int intVal(int doc) throws IOException {
          return expected ? 1 : 0;
        }
      },
      new LongDocValues(null) {
        @Override
        public long longVal(int doc) throws IOException {
          return expected ? 1L : 0L;
        }
      },
    };
      
    for (FunctionValues fv : values) {
      // docId is irrelevant since all of our FunctionValues return a constant value.
      assertEquals(fv.getClass().getSuperclass().toString(), expected, fv.boolVal(123));
    }
  }
}
