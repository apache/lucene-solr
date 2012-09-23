package org.apache.lucene.util.fst;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.fst.UpToTwoPositiveIntOutputs.TwoLongs;

import static org.apache.lucene.util.fst.FSTTester.getRandomString;
import static org.apache.lucene.util.fst.FSTTester.toIntsRef;

public class TestFSTsMisc extends LuceneTestCase {

  private MockDirectoryWrapper dir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newMockDirectory();
    dir.setPreventDoubleWrite(false);
  }

  @Override
  public void tearDown() throws Exception {
    // can be null if we force simpletext (funky, some kind of bug in test runner maybe)
    if (dir != null) dir.close();
    super.tearDown();
  }

  public void testRandomWords() throws IOException {
    testRandomWords(1000, LuceneTestCase.atLeast(random(), 2));
    //testRandomWords(100, 1);
  }

  private void testRandomWords(int maxNumWords, int numIter) throws IOException {
    Random random = new Random(random().nextLong());
    for(int iter=0;iter<numIter;iter++) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter " + iter);
      }
      for(int inputMode=0;inputMode<2;inputMode++) {
        final int numWords = random.nextInt(maxNumWords+1);
        Set<IntsRef> termsSet = new HashSet<IntsRef>();
        IntsRef[] terms = new IntsRef[numWords];
        while(termsSet.size() < numWords) {
          final String term = getRandomString(random);
          termsSet.add(toIntsRef(term, inputMode));
        }
        doTest(inputMode, termsSet.toArray(new IntsRef[termsSet.size()]));
      }
    }
  }

  private void doTest(int inputMode, IntsRef[] terms) throws IOException {
    Arrays.sort(terms);

    // Up to two positive ints, shared, generally but not
    // monotonically increasing
    {
      if (VERBOSE) {
        System.out.println("TEST: now test UpToTwoPositiveIntOutputs");
      }
      final UpToTwoPositiveIntOutputs outputs = UpToTwoPositiveIntOutputs.getSingleton(true);
      final List<FSTTester.InputOutput<Object>> pairs = new ArrayList<FSTTester.InputOutput<Object>>(terms.length);
      long lastOutput = 0;
      for(int idx=0;idx<terms.length;idx++) {
        // Sometimes go backwards
        long value = lastOutput + _TestUtil.nextInt(random(), -100, 1000);
        while(value < 0) {
          value = lastOutput + _TestUtil.nextInt(random(), -100, 1000);
        }
        final Object output;
        if (random().nextInt(5) == 3) {
          long value2 = lastOutput + _TestUtil.nextInt(random(), -100, 1000);
          while(value2 < 0) {
            value2 = lastOutput + _TestUtil.nextInt(random(), -100, 1000);
          }
          List<Long> values = new ArrayList<Long>();
          values.add(value);
          values.add(value2);
          output = values;
        } else {
          output = outputs.get(value);
        }
        pairs.add(new FSTTester.InputOutput<Object>(terms[idx], output));
      }
      new FSTTester<Object>(random(), dir, inputMode, pairs, outputs, false) {
        @Override
        protected boolean outputsEqual(Object output1, Object output2) {
          if (output1 instanceof TwoLongs && output2 instanceof List) {
            TwoLongs twoLongs1 = (TwoLongs) output1;
            return Arrays.asList(new Long[] {twoLongs1.first, twoLongs1.second}).equals(output2);
          } else if (output2 instanceof TwoLongs && output1 instanceof List) {
            TwoLongs twoLongs2 = (TwoLongs) output2;
            return Arrays.asList(new Long[] {twoLongs2.first, twoLongs2.second}).equals(output1);
          }
          return output1.equals(output2);
        }
      }.doTest(false);
    }

    // ListOfOutputs(PositiveIntOutputs), generally but not
    // monotonically increasing
    {
      if (VERBOSE) {
        System.out.println("TEST: now test OneOrMoreOutputs");
      }
      final PositiveIntOutputs _outputs = PositiveIntOutputs.getSingleton();
      final ListOfOutputs<Long> outputs = new ListOfOutputs<Long>(_outputs);
      final List<FSTTester.InputOutput<Object>> pairs = new ArrayList<FSTTester.InputOutput<Object>>(terms.length);
      long lastOutput = 0;
      for(int idx=0;idx<terms.length;idx++) {
        
        int outputCount = _TestUtil.nextInt(random(), 1, 7);
        List<Long> values = new ArrayList<Long>();
        for(int i=0;i<outputCount;i++) {
          // Sometimes go backwards
          long value = lastOutput + _TestUtil.nextInt(random(), -100, 1000);
          while(value < 0) {
            value = lastOutput + _TestUtil.nextInt(random(), -100, 1000);
          }
          values.add(value);
          lastOutput = value;
        }

        final Object output;
        if (values.size() == 1) {
          output = values.get(0);
        } else {
          output = values;
        }

        pairs.add(new FSTTester.InputOutput<Object>(terms[idx], output));
      }
      new FSTTester<Object>(random(), dir, inputMode, pairs, outputs, false).doTest(false);
    }
  }

  public void testListOfOutputs() throws Exception {
    PositiveIntOutputs _outputs = PositiveIntOutputs.getSingleton();
    ListOfOutputs<Long> outputs = new ListOfOutputs<Long>(_outputs);
    final Builder<Object> builder = new Builder<Object>(FST.INPUT_TYPE.BYTE1, outputs);

    final IntsRef scratch = new IntsRef();
    // Add the same input more than once and the outputs
    // are merged:
    builder.add(Util.toIntsRef(new BytesRef("a"), scratch), 1L);
    builder.add(Util.toIntsRef(new BytesRef("a"), scratch), 3L);
    builder.add(Util.toIntsRef(new BytesRef("a"), scratch), 0L);
    builder.add(Util.toIntsRef(new BytesRef("b"), scratch), 17L);
    final FST<Object> fst = builder.finish();

    Object output = Util.get(fst, new BytesRef("a"));
    assertNotNull(output);
    List<Long> outputList = outputs.asList(output);
    assertEquals(3, outputList.size());
    assertEquals(1L, outputList.get(0).longValue());
    assertEquals(3L, outputList.get(1).longValue());
    assertEquals(0L, outputList.get(2).longValue());

    output = Util.get(fst, new BytesRef("b"));
    assertNotNull(output);
    outputList = outputs.asList(output);
    assertEquals(1, outputList.size());
    assertEquals(17L, outputList.get(0).longValue());
  }

  public void testListOfOutputsEmptyString() throws Exception {
    PositiveIntOutputs _outputs = PositiveIntOutputs.getSingleton();
    ListOfOutputs<Long> outputs = new ListOfOutputs<Long>(_outputs);
    final Builder<Object> builder = new Builder<Object>(FST.INPUT_TYPE.BYTE1, outputs);

    final IntsRef scratch = new IntsRef();
    builder.add(scratch, 0L);
    builder.add(scratch, 1L);
    builder.add(scratch, 17L);
    builder.add(scratch, 1L);

    builder.add(Util.toIntsRef(new BytesRef("a"), scratch), 1L);
    builder.add(Util.toIntsRef(new BytesRef("a"), scratch), 3L);
    builder.add(Util.toIntsRef(new BytesRef("a"), scratch), 0L);
    builder.add(Util.toIntsRef(new BytesRef("b"), scratch), 0L);
    
    final FST<Object> fst = builder.finish();

    Object output = Util.get(fst, new BytesRef(""));
    assertNotNull(output);
    List<Long> outputList = outputs.asList(output);
    assertEquals(4, outputList.size());
    assertEquals(0L, outputList.get(0).longValue());
    assertEquals(1L, outputList.get(1).longValue());
    assertEquals(17L, outputList.get(2).longValue());
    assertEquals(1L, outputList.get(3).longValue());

    output = Util.get(fst, new BytesRef("a"));
    assertNotNull(output);
    outputList = outputs.asList(output);
    assertEquals(3, outputList.size());
    assertEquals(1L, outputList.get(0).longValue());
    assertEquals(3L, outputList.get(1).longValue());
    assertEquals(0L, outputList.get(2).longValue());

    output = Util.get(fst, new BytesRef("b"));
    assertNotNull(output);
    outputList = outputs.asList(output);
    assertEquals(1, outputList.size());
    assertEquals(0L, outputList.get(0).longValue());
  }
}


