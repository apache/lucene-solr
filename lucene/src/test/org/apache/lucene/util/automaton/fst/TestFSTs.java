package org.apache.lucene.util.automaton.fst;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util._TestUtil;

public class TestFSTs extends LuceneTestCase {

  private MockDirectoryWrapper dir;

  public void setUp() throws IOException {
    dir = newDirectory();
    dir.setPreventDoubleWrite(false);
  }

  public void tearDown() throws IOException {
    dir.close();
  }

  private static BytesRef toBytesRef(IntsRef ir) {
    BytesRef br = new BytesRef(ir.length);
    for(int i=0;i<ir.length;i++) {
      int x = ir.ints[ir.offset+i];
      assert x >= 0 && x <= 255;
      br.bytes[i] = (byte) x;
    }
    br.length = ir.length;
    return br;
  }

  private static IntsRef toIntsRef(String s, int inputMode) {
    return toIntsRef(s, inputMode, new IntsRef(10));
  }

  private static IntsRef toIntsRef(String s, int inputMode, IntsRef ir) {
    if (inputMode == 0) {
      // utf8
      return toIntsRef(new BytesRef(s), ir);
    } else {
      // utf32
      return toIntsRefUTF32(s, ir);
    }
  }

  private static IntsRef toIntsRefUTF32(String s, IntsRef ir) {
    final int charLength = s.length();
    int charIdx = 0;
    int intIdx = 0;
    while(charIdx < charLength) {
      if (intIdx == ir.ints.length) {
        ir.grow(intIdx+1);
      }
      final int utf32 = s.codePointAt(charIdx);
      ir.ints[intIdx] = utf32;
      charIdx += Character.charCount(utf32);
      intIdx++;
    }
    ir.length = intIdx;
    return ir;
  }

  private static IntsRef toIntsRef(BytesRef br, IntsRef ir) {
    if (br.length > ir.ints.length) {
      ir.grow(br.length);
    }
    for(int i=0;i<br.length;i++) {
      ir.ints[i] = br.bytes[br.offset+i]&0xFF;
    }
    ir.length = br.length;
    return ir;
  }

  public void testBasicFSA() throws IOException {
    String[] strings = new String[] {"station", "commotion", "elation", "elastic", "plastic", "stop", "ftop", "ftation"};
    IntsRef[] terms = new IntsRef[strings.length];
    for(int inputMode=0;inputMode<2;inputMode++) {
      if (VERBOSE) {
        System.out.println("TEST: inputMode=" + inputModeToString(inputMode));
      }

      for(int idx=0;idx<strings.length;idx++) {
        terms[idx] = toIntsRef(strings[idx], inputMode);
      }

      doTest(inputMode, terms);
    
      // Test pre-determined FST sizes to make sure we haven't lost minimality (at least on this trivial set of terms):

      // FSA
      {
        final Outputs<Object> outputs = NoOutputs.getSingleton();
        final Object NO_OUTPUT = outputs.getNoOutput();      
        final List<FSTTester.InputOutput<Object>> pairs = new ArrayList<FSTTester.InputOutput<Object>>(terms.length);
        for(IntsRef term : terms) {
          pairs.add(new FSTTester.InputOutput<Object>(term, NO_OUTPUT));
        }
        FST<Object> fst = new FSTTester<Object>(random, dir, inputMode, pairs, outputs).doTest(0, 0);
        assertNotNull(fst);
        assertEquals(22, fst.getNodeCount());
        assertEquals(27, fst.getArcCount());
      }

      // FST ord pos int
      {
        final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
        final List<FSTTester.InputOutput<Long>> pairs = new ArrayList<FSTTester.InputOutput<Long>>(terms.length);
        for(int idx=0;idx<terms.length;idx++) {
          pairs.add(new FSTTester.InputOutput<Long>(terms[idx], outputs.get(idx)));
        }
        final FST<Long> fst = new FSTTester<Long>(random, dir, inputMode, pairs, outputs).doTest(0, 0);
        assertNotNull(fst);
        assertEquals(22, fst.getNodeCount());
        assertEquals(27, fst.getArcCount());
      }

      // FST byte sequence ord
      {
        final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
        final BytesRef NO_OUTPUT = outputs.getNoOutput();      
        final List<FSTTester.InputOutput<BytesRef>> pairs = new ArrayList<FSTTester.InputOutput<BytesRef>>(terms.length);
        for(int idx=0;idx<terms.length;idx++) {
          final BytesRef output = random.nextInt(30) == 17 ? NO_OUTPUT : new BytesRef(Integer.toString(idx));
          pairs.add(new FSTTester.InputOutput<BytesRef>(terms[idx], output));
        }
        final FST<BytesRef> fst = new FSTTester<BytesRef>(random, dir, inputMode, pairs, outputs).doTest(0, 0);
        assertNotNull(fst);
        assertEquals(24, fst.getNodeCount());
        assertEquals(30, fst.getArcCount());
      }
    }
  }

  private static String simpleRandomString(Random r) {
    final int end = r.nextInt(10);
    if (end == 0) {
      // allow 0 length
      return "";
    }
    final char[] buffer = new char[end];
    for (int i = 0; i < end; i++) {
      buffer[i] = (char) _TestUtil.nextInt(random, 97, 102);
    }
    return new String(buffer, 0, end);
  }

  // given set of terms, test the different outputs for them
  private void doTest(int inputMode, IntsRef[] terms) throws IOException {
    Arrays.sort(terms);

    // NoOutputs (simple FSA)
    {
      final Outputs<Object> outputs = NoOutputs.getSingleton();
      final Object NO_OUTPUT = outputs.getNoOutput();      
      final List<FSTTester.InputOutput<Object>> pairs = new ArrayList<FSTTester.InputOutput<Object>>(terms.length);
      for(IntsRef term : terms) {
        pairs.add(new FSTTester.InputOutput<Object>(term, NO_OUTPUT));
      }
      new FSTTester<Object>(random, dir, inputMode, pairs, outputs).doTest();
    }

    // PositiveIntOutput (ord)
    {
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
      final List<FSTTester.InputOutput<Long>> pairs = new ArrayList<FSTTester.InputOutput<Long>>(terms.length);
      for(int idx=0;idx<terms.length;idx++) {
        pairs.add(new FSTTester.InputOutput<Long>(terms[idx], outputs.get(idx)));
      }
      new FSTTester<Long>(random, dir, inputMode, pairs, outputs).doTest();
    }

    // PositiveIntOutput (random monotonically increasing positive number)
    {
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(random.nextBoolean());
      final List<FSTTester.InputOutput<Long>> pairs = new ArrayList<FSTTester.InputOutput<Long>>(terms.length);
      long lastOutput = 0;
      for(int idx=0;idx<terms.length;idx++) {
        final long value = lastOutput + _TestUtil.nextInt(random, 1, 1000);
        lastOutput = value;
        pairs.add(new FSTTester.InputOutput<Long>(terms[idx], outputs.get(value)));
      }
      new FSTTester<Long>(random, dir, inputMode, pairs, outputs).doTest();
    }

    // PositiveIntOutput (random positive number)
    {
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(random.nextBoolean());
      final List<FSTTester.InputOutput<Long>> pairs = new ArrayList<FSTTester.InputOutput<Long>>(terms.length);
      for(int idx=0;idx<terms.length;idx++) {
        pairs.add(new FSTTester.InputOutput<Long>(terms[idx], outputs.get(random.nextLong()) & Long.MAX_VALUE));
      }
      new FSTTester<Long>(random, dir, inputMode, pairs, outputs).doTest();
    }

    // Pair<ord, (random monotonically increasing positive number>
    {
      final PositiveIntOutputs o1 = PositiveIntOutputs.getSingleton(random.nextBoolean());
      final PositiveIntOutputs o2 = PositiveIntOutputs.getSingleton(random.nextBoolean());
      final PairOutputs<Long,Long> outputs = new PairOutputs<Long,Long>(o1, o2);
      final List<FSTTester.InputOutput<PairOutputs.Pair<Long,Long>>> pairs = new ArrayList<FSTTester.InputOutput<PairOutputs.Pair<Long,Long>>>(terms.length);
      long lastOutput = 0;
      for(int idx=0;idx<terms.length;idx++) {
        final long value = lastOutput + _TestUtil.nextInt(random, 1, 1000);
        lastOutput = value;
        pairs.add(new FSTTester.InputOutput<PairOutputs.Pair<Long,Long>>(terms[idx],
                                                                         outputs.get(o1.get(idx),
                                                                                     o2.get(value))));
      }
      new FSTTester<PairOutputs.Pair<Long,Long>>(random, dir, inputMode, pairs, outputs).doTest();
    }

    // Sequence-of-bytes
    {
      final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
      final BytesRef NO_OUTPUT = outputs.getNoOutput();      
      final List<FSTTester.InputOutput<BytesRef>> pairs = new ArrayList<FSTTester.InputOutput<BytesRef>>(terms.length);
      for(int idx=0;idx<terms.length;idx++) {
        final BytesRef output = random.nextInt(30) == 17 ? NO_OUTPUT : new BytesRef(Integer.toString(idx));
        pairs.add(new FSTTester.InputOutput<BytesRef>(terms[idx], output));
      }
      new FSTTester<BytesRef>(random, dir, inputMode, pairs, outputs).doTest();
    }

    // Sequence-of-ints
    {
      final IntSequenceOutputs outputs = IntSequenceOutputs.getSingleton();
      final List<FSTTester.InputOutput<IntsRef>> pairs = new ArrayList<FSTTester.InputOutput<IntsRef>>(terms.length);
      for(int idx=0;idx<terms.length;idx++) {
        final String s = Integer.toString(idx);
        final IntsRef output = new IntsRef(s.length());
        output.length = s.length();
        for(int idx2=0;idx2<output.length;idx2++) {
          output.ints[idx2] = s.charAt(idx2);
        }
        pairs.add(new FSTTester.InputOutput<IntsRef>(terms[idx], output));
      }
      new FSTTester<IntsRef>(random, dir, inputMode, pairs, outputs).doTest();
    }
  }

  private static class FSTTester<T> {

    final Random random;
    final List<InputOutput<T>> pairs;
    final int inputMode;
    final Outputs<T> outputs;
    final Directory dir;

    public FSTTester(Random random, Directory dir, int inputMode, List<InputOutput<T>> pairs, Outputs<T> outputs) {
      this.random = random;
      this.dir = dir;
      this.inputMode = inputMode;
      this.pairs = pairs;
      this.outputs = outputs;
    }

    private static class InputOutput<T> implements Comparable<InputOutput<T>> {
      public final IntsRef input;
      public final T output;

      public InputOutput(IntsRef input, T output) {
        this.input = input;
        this.output = output;
      }

      public int compareTo(InputOutput<T> other) {
        if (other instanceof InputOutput) {
          return input.compareTo((other).input);
        } else {
          throw new IllegalArgumentException();
        }
      }
    }

    private String getRandomString() {
      final String term;
      if (random.nextBoolean()) {
        term = _TestUtil.randomRealisticUnicodeString(random);
      } else {
        // we want to mix in limited-alphabet symbols so
        // we get more sharing of the nodes given how few
        // terms we are testing...
        term = simpleRandomString(random);
      }
      return term;
    }

    public void doTest() throws IOException {
      // no pruning
      doTest(0, 0);

      // simple pruning
      doTest(_TestUtil.nextInt(random, 1, 1+pairs.size()), 0);

      // leafy pruning
      doTest(0, _TestUtil.nextInt(random, 1, 1+pairs.size()));
    }

    // NOTE: only copies the stuff this test needs!!
    private FST.Arc<T> copyArc(FST.Arc<T> arc) {
      final FST.Arc<T> copy = new FST.Arc<T>();
      copy.label = arc.label;
      copy.target = arc.target;
      copy.output = arc.output;
      copy.nextFinalOutput = arc.nextFinalOutput;
      return arc;
    }

    // runs the term, returning the output, or null if term
    // isn't accepted.  if stopNode is non-null it must be
    // length 2 int array; stopNode[0] will be the last
    // matching node (-1 if the term is accepted)
    // and stopNode[1] will be the length of the
    // term prefix that matches
    private T run(FST<T> fst, IntsRef term, int[] stopNode) throws IOException {
      if (term.length == 0) {
        final T output = fst.getEmptyOutput();
        if (stopNode != null) {
          stopNode[1] = 0;
          if (output != null) {
            // accepted
            stopNode[0] = -1;
          } else {
            stopNode[0] = fst.getStartNode();
          }
        }
        return output;
      }

      final FST.Arc<T> arc = new FST.Arc<T>();
      int node = fst.getStartNode();
      int lastNode = -1;
      T output = fst.outputs.getNoOutput();
      //System.out.println("match?");
      for(int i=0;i<term.length;i++) {
        //System.out.println("  int=" + term.ints[i]);
        if (!fst.hasArcs(node)) {
          //System.out.println("    no arcs!");
          // hit end node before term's end
          if (stopNode != null) {
            stopNode[0] = lastNode;
            stopNode[1] = i-1;
            return output;
          } else {
            return null;
          }
        }

        if (fst.findArc(node, term.ints[term.offset + i], arc) != null) {
          node = arc.target;
          //System.out.println("    match final?=" + arc.isFinal());
          if (arc.output != fst.outputs.getNoOutput()) {
            output = fst.outputs.add(output, arc.output);
          }
        } else if (stopNode != null) {
          stopNode[0] = node;
          stopNode[1] = i;
          return output;
        } else {
          //System.out.println("    no match");
          return null;
        }

        lastNode = node;
      }

      if (!arc.isFinal()) {
        // hit term's end before end node
        if (stopNode != null) {
          stopNode[0] = node;
          stopNode[1] = term.length;
          return output;
        } else {
          return null;
        }
      }

      if (arc.nextFinalOutput != fst.outputs.getNoOutput()) {
        output = fst.outputs.add(output, arc.nextFinalOutput);
      }

      if (stopNode != null) {
        stopNode[0] = -1;
        stopNode[1] = term.length;
      }
      return output;
    }

    private T randomAcceptedWord(FST<T> fst, IntsRef in) throws IOException {
      int node = fst.getStartNode();

      if (fst.noNodes()) {
        // degenerate FST: only accepts the empty string
        assertTrue(fst.getEmptyOutput() != null);
        in.length = 0;
        return fst.getEmptyOutput();
      }
      final List<FST.Arc<T>> arcs = new ArrayList<FST.Arc<T>>();
      in.length = 0;
      in.offset = 0;
      T output = fst.outputs.getNoOutput();
      //System.out.println("get random");
      while(true) {
        // read all arcs:
        //System.out.println("  n=" + node);
        int arcAddress = node;
        FST.Arc<T> arc = new FST.Arc<T>();
        fst.readFirstArc(arcAddress, arc);
        arcs.add(copyArc(arc));
        while(!arc.isLast()) {
          fst.readNextArc(arc);
          arcs.add(copyArc(arc));
        }
      
        // pick one
        arc = arcs.get(random.nextInt(arcs.size()));

        arcs.clear();

        // append label
        if (in.ints.length == in.length) {
          in.grow(1+in.length);
        }
        in.ints[in.length++] = arc.label;

        output = fst.outputs.add(output, arc.output);

        // maybe stop
        if (arc.isFinal()) {
          if (fst.hasArcs(arc.target)) {
            // final state but it also has outgoing edges
            if (random.nextBoolean()) {
              output = fst.outputs.add(output, arc.nextFinalOutput);
              break;
            }
          } else {
            break;
          }
        }

        node = arc.target;
      }

      return output;
    }


    private FST<T> doTest(int prune1, int prune2) throws IOException {
      if (VERBOSE) {
        System.out.println("TEST: prune1=" + prune1 + " prune2=" + prune2);
      }

      final Builder<T> builder = new Builder<T>(inputMode == 0 ? FST.INPUT_TYPE.BYTE1 : FST.INPUT_TYPE.BYTE4,
                                                prune1, prune2,
                                                prune1==0 && prune2==0, outputs);

      for(InputOutput<T> pair : pairs) {
        builder.add(pair.input, pair.output);
      }
      FST<T> fst = builder.finish();

      if (random.nextBoolean() && fst != null) {
        IndexOutput out = dir.createOutput("fst.bin");
        fst.save(out);
        out.close();
        IndexInput in = dir.openInput("fst.bin");
        try {
          fst = new FST<T>(in, outputs);
        } finally {
          in.close();
          dir.deleteFile("fst.bin");
        }
      }

      if (VERBOSE && pairs.size() <= 20 && fst != null) {
        PrintStream ps = new PrintStream("out.dot");
        fst.toDot(ps);
        ps.close();
        System.out.println("SAVED out.dot");
      }

      if (VERBOSE) {
        if (fst == null) {
          System.out.println("  fst has 0 nodes (fully pruned)");
        } else {
          System.out.println("  fst has " + fst.getNodeCount() + " nodes and " + fst.getArcCount() + " arcs");
        }
      }

      if (prune1 == 0 && prune2 == 0) {
        verifyUnPruned(inputMode, fst);
      } else {
        verifyPruned(inputMode, fst, prune1, prune2);
      }

      return fst;
    }

    // FST is complete
    private void verifyUnPruned(int inputMode, FST<T> fst) throws IOException {

      if (pairs.size() == 0) {
        assertNull(fst);
        return;
      }

      if (VERBOSE) {
        System.out.println("TEST: now verify " + pairs.size() + " terms");
        for(InputOutput<T> pair : pairs) {
          assertNotNull(pair);
          assertNotNull(pair.input);
          assertNotNull(pair.output);
          System.out.println("  " + inputToString(inputMode, pair.input) + ": " + outputs.outputToString(pair.output));
        }
      }

      assertNotNull(fst);

      // make sure all words are accepted
      {
        IntsRefFSTEnum<T> fstEnum = new IntsRefFSTEnum<T>(fst);
        for(InputOutput<T> pair : pairs) {
          IntsRef term = pair.input;
          Object output = run(fst, term, null);

          assertNotNull("term " + inputToString(inputMode, term) + " is not accepted", output);
          assertEquals(output, pair.output);

          // verify enum's next
          IntsRefFSTEnum.InputOutput<T> t = fstEnum.next();

          assertEquals(term, t.input);
          assertEquals(pair.output, t.output);
        }
        assertNull(fstEnum.next());
      }

      final Map<IntsRef,T> termsMap = new HashMap<IntsRef,T>();
      for(InputOutput<T> pair : pairs) {
        termsMap.put(pair.input, pair.output);
      }

      // find random matching word and make sure it's valid
      final IntsRef scratch = new IntsRef(10);
      for(int iter=0;iter<500*RANDOM_MULTIPLIER;iter++) {
        T output = randomAcceptedWord(fst, scratch);
        assertTrue("accepted word " + inputToString(inputMode, scratch) + " is not valid", termsMap.containsKey(scratch));
        assertEquals(termsMap.get(scratch), output);
      }
    
      // test single IntsRefFSTEnum.advance:
      //System.out.println("TEST: verify advance");
      for(int iter=0;iter<100*RANDOM_MULTIPLIER;iter++) {
        final IntsRefFSTEnum<T> fstEnum = new IntsRefFSTEnum<T>(fst);
        if (random.nextBoolean()) {
          // seek to term that doesn't exist:
          while(true) {
            final IntsRef term = toIntsRef(getRandomString(), inputMode);
            int pos = Collections.binarySearch(pairs, new InputOutput<T>(term, null));
            if (pos < 0) {
              pos = -(pos+1);
              // ok doesn't exist
              //System.out.println("  seek " + inputToString(inputMode, term));
              final IntsRefFSTEnum.InputOutput<T> seekResult = fstEnum.advance(term);
              if (pos < pairs.size()) {
                //System.out.println("    got " + inputToString(inputMode,seekResult.input) + " output=" + fst.outputs.outputToString(seekResult.output));
                assertEquals(pairs.get(pos).input, seekResult.input);
                assertEquals(pairs.get(pos).output, seekResult.output);
              } else {
                // seeked beyond end
                //System.out.println("seek=" + seekTerm);
                assertNull("expected null but got " + (seekResult==null ? "null" : inputToString(inputMode, seekResult.input)), seekResult);
              }

              break;
            }
          }
        } else {
          // seek to term that does exist:
          InputOutput pair = pairs.get(random.nextInt(pairs.size()));
          //System.out.println("  seek " + inputToString(inputMode, pair.input));
          final IntsRefFSTEnum.InputOutput<T> seekResult = fstEnum.advance(pair.input);
          assertEquals(pair.input, seekResult.input);
          assertEquals(pair.output, seekResult.output);
        }
      }

      if (VERBOSE) {
        System.out.println("TEST: mixed next/advance");
      }

      // test mixed next/advance
      for(int iter=0;iter<100*RANDOM_MULTIPLIER;iter++) {
        if (VERBOSE) {
          System.out.println("TEST: iter " + iter);
        }
        final IntsRefFSTEnum<T> fstEnum = new IntsRefFSTEnum<T>(fst);
        int upto = -1;
        while(true) {
          boolean isDone = false;
          if (upto == pairs.size()-1 || random.nextBoolean()) {
            // next
            upto++;
            if (VERBOSE) {
              System.out.println("  do next");
            }
            isDone = fstEnum.next() == null;
          } else if (upto != -1 && upto < 0.75 * pairs.size() && random.nextBoolean()) {
            int attempt = 0;
            for(;attempt<10;attempt++) {
              IntsRef term = toIntsRef(getRandomString(), inputMode);
              if (!termsMap.containsKey(term) && term.compareTo(pairs.get(upto).input) > 0) {
                if (VERBOSE) {
                  System.out.println("  do non-exist advance(" + inputToString(inputMode, term) + "]");
                }
                int pos = Collections.binarySearch(pairs, new InputOutput<T>(term, null));
                assert pos < 0;
                upto = -(pos+1);
                isDone = fstEnum.advance(term) == null;
                break;
              }
            }
            if (attempt == 10) {
              continue;
            }
            
          } else {
            final int inc = random.nextInt(pairs.size() - upto - 1);
            upto += inc;
            if (upto == -1) {
              upto = 0;
            }

            if (VERBOSE) {
              System.out.println("  do advance(" + inputToString(inputMode, pairs.get(upto).input) + "]");
            }
            isDone = fstEnum.advance(pairs.get(upto).input) == null;
          }
          if (VERBOSE) {
            if (!isDone) {
              System.out.println("    got " + inputToString(inputMode, fstEnum.current().input));
            } else {
              System.out.println("    got null");
            }
          }

          if (upto == pairs.size()) {
            assertTrue(isDone);
            break;
          } else {
            assertFalse(isDone);
            assertEquals(pairs.get(upto).input, fstEnum.current().input);
            assertEquals(pairs.get(upto).output, fstEnum.current().output);
          }
        }
      }
    }

    private static class CountMinOutput<T> {
      int count;
      T output;
      T finalOutput;
      boolean isLeaf = true;
      boolean isFinal;
    }

    // FST is pruned
    private void verifyPruned(int inputMode, FST<T> fst, int prune1, int prune2) throws IOException {

      if (VERBOSE) {
        System.out.println("TEST: now verify pruned " + pairs.size() + " terms; outputs=" + outputs);
        for(InputOutput<T> pair : pairs) {
          System.out.println("  " + inputToString(inputMode, pair.input) + ": " + outputs.outputToString(pair.output));
        }
      }

      // To validate the FST, we brute-force compute all prefixes
      // in the terms, matched to their "common" outputs, prune that
      // set according to the prune thresholds, then assert the FST
      // matches that same set.

      // NOTE: Crazy RAM intensive!!

      //System.out.println("TEST: tally prefixes");

      // build all prefixes
      final Map<IntsRef,CountMinOutput<T>> prefixes = new HashMap<IntsRef,CountMinOutput<T>>();
      final IntsRef scratch = new IntsRef(10);
      for(InputOutput<T> pair: pairs) {
        scratch.copy(pair.input);
        for(int idx=0;idx<=pair.input.length;idx++) {
          scratch.length = idx;
          CountMinOutput<T> cmo = prefixes.get(scratch);
          if (cmo == null) {
            cmo = new CountMinOutput<T>();
            cmo.count = 1;
            cmo.output = pair.output;
            prefixes.put(new IntsRef(scratch), cmo);
          } else {
            cmo.count++;
            cmo.output = outputs.common(cmo.output, pair.output);
          }
          if (idx == pair.input.length) {
            cmo.isFinal = true;
            cmo.finalOutput = cmo.output;
          }
        }
      }

      //System.out.println("TEST: now prune");

      // prune 'em
      final Iterator<Map.Entry<IntsRef,CountMinOutput<T>>> it = prefixes.entrySet().iterator();
      while(it.hasNext()) {
        Map.Entry<IntsRef,CountMinOutput<T>> ent = it.next();
        final IntsRef prefix = ent.getKey();
        final CountMinOutput<T> cmo = ent.getValue();
        //System.out.println("  term=" + inputToString(inputMode, prefix) + " count=" + cmo.count + " isLeaf=" + cmo.isLeaf);
        final boolean keep;
        if (prune1 > 0) {
          keep = cmo.count >= prune1;
        } else {
          assert prune2 > 0;
          if (prune2 > 1 && cmo.count >= prune2) {
            keep = true;
          } else if (prefix.length > 0) {
            // consult our parent
            scratch.length = prefix.length-1;
            System.arraycopy(prefix.ints, prefix.offset, scratch.ints, 0, scratch.length);
            final CountMinOutput<T> cmo2 = prefixes.get(scratch);
            //System.out.println("    parent count = " + (cmo2 == null ? -1 : cmo2.count));
            keep = cmo2 != null && ((prune2 > 1 && cmo2.count >= prune2) || (prune2 == 1 && (cmo2.count >= 2 || prefix.length <= 1)));
          } else if (cmo.count >= prune2) {
            keep = true;
          } else {
            keep = false;
          }
        }

        if (!keep) {
          it.remove();
          //System.out.println("    remove");
        } else {
          // clear isLeaf for all ancestors
          //System.out.println("    keep");
          scratch.copy(prefix);
          scratch.length--;
          while(scratch.length >= 0) {
            final CountMinOutput<T> cmo2 = prefixes.get(scratch);
            if (cmo2 != null) {
              //System.out.println("    clear isLeaf " + inputToString(inputMode, scratch));
              cmo2.isLeaf = false;
            }
            scratch.length--;
          }
        }
      }

      //System.out.println("TEST: after prune");
      /*
        for(Map.Entry<BytesRef,CountMinOutput> ent : prefixes.entrySet()) {
        System.out.println("  " + inputToString(inputMode, ent.getKey()) + ": isLeaf=" + ent.getValue().isLeaf + " isFinal=" + ent.getValue().isFinal);
        if (ent.getValue().isFinal) {
        System.out.println("    finalOutput=" + outputs.outputToString(ent.getValue().finalOutput));
        }
        }
      */

      if (prefixes.size() <= 1) {
        assertNull(fst);
        return;
      }

      assertNotNull(fst);

      // make sure FST only enums valid prefixes
      IntsRefFSTEnum<T> fstEnum = new IntsRefFSTEnum<T>(fst);
      IntsRefFSTEnum.InputOutput current;
      while((current = fstEnum.next()) != null) {
        //System.out.println("  fst enum term=" + inputToString(inputMode, current.input) + " output=" + outputs.outputToString(current.output));
        final CountMinOutput cmo = prefixes.get(current.input);
        assertNotNull(cmo);
        assertTrue(cmo.isLeaf || cmo.isFinal);
        if (cmo.isFinal && !cmo.isLeaf) {
          assertEquals(cmo.finalOutput, current.output);
        } else {
          assertEquals(cmo.output, current.output);
        }
      }

      // make sure all non-pruned prefixes are present in the FST
      final int[] stopNode = new int[2];
      for(Map.Entry<IntsRef,CountMinOutput<T>> ent : prefixes.entrySet()) {
        if (ent.getKey().length > 0) {
          final CountMinOutput<T> cmo = ent.getValue();
          final T output = run(fst, ent.getKey(), stopNode);
          //System.out.println("  term=" + inputToString(inputMode, ent.getKey()) + " output=" + outputs.outputToString(cmo.output));
          // if (cmo.isFinal && !cmo.isLeaf) {
          if (cmo.isFinal) {
            assertEquals(cmo.finalOutput, output);
          } else {
            assertEquals(cmo.output, output);
          }
          assertEquals(ent.getKey().length, stopNode[1]);
        }
      }
    }
  }

  public void testRandomWords() throws IOException {
    testRandomWords(1000, 5 * RANDOM_MULTIPLIER);
    //testRandomWords(10, 100);
  }

  private String inputModeToString(int mode) {
    if (mode == 0) {
      return "utf8";
    } else {
      return "utf32";
    }
  }

  private void testRandomWords(int maxNumWords, int numIter) throws IOException {
    for(int iter=0;iter<numIter;iter++) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter " + iter);
      }
      for(int inputMode=0;inputMode<2;inputMode++) {
        final int numWords = random.nextInt(maxNumWords+1);
        Set<IntsRef> termsSet = new HashSet<IntsRef>();
        IntsRef[] terms = new IntsRef[numWords];
        while(termsSet.size() < numWords) {
          final String term = getRandomString();
          termsSet.add(toIntsRef(term, inputMode));
        }
        doTest(inputMode, termsSet.toArray(new IntsRef[termsSet.size()]));
      }
    }
  }

  private String getRandomString() {
    final String term;
    if (random.nextBoolean()) {
      term = _TestUtil.randomRealisticUnicodeString(random);
    } else {
      // we want to mix in limited-alphabet symbols so
      // we get more sharing of the nodes given how few
      // terms we are testing...
      term = simpleRandomString(random);
    }
    return term;
  }

  @Nightly
  public void testBigSet() throws IOException {
    testRandomWords(50000, RANDOM_MULTIPLIER);
  }

  private static String inputToString(int inputMode, IntsRef term) {
    if (inputMode == 0) {
      // utf8
      return toBytesRef(term).utf8ToString();
    } else {
      // utf32
      return UnicodeUtil.newString(term.ints, term.offset, term.length);
    }
  }

  // Build FST for all unique terms in the test line docs
  // file, up until a time limit
  public void testRealTerms() throws Exception {

    if (CodecProvider.getDefault().getDefaultFieldCodec().equals("SimpleText")) {
      // no
      CodecProvider.getDefault().setDefaultFieldCodec("Standard");
    }

    final LineFileDocs docs = new LineFileDocs(false);
    final int RUN_TIME_SEC = LuceneTestCase.TEST_NIGHTLY ? 300 : 1;
    final IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(-1).setRAMBufferSizeMB(64);
    final File tempDir = _TestUtil.getTempDir("fstlines");
    final MockDirectoryWrapper dir = new MockDirectoryWrapper(random, FSDirectory.open(tempDir));
    final IndexWriter writer = new IndexWriter(dir, conf);
    final long stopTime = System.currentTimeMillis() + RUN_TIME_SEC * 1000;
    Document doc;
    int docCount = 0;
    while((doc = docs.nextDoc()) != null && System.currentTimeMillis() < stopTime) {
      writer.addDocument(doc);
      docCount++;
    }
    IndexReader r = IndexReader.open(writer);
    writer.close();
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(random.nextBoolean());
    Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, 0, 0, true, outputs);

    boolean storeOrd = random.nextBoolean();
    if (VERBOSE) {
      if (storeOrd) {
        System.out.println("FST stores ord");
      } else {
        System.out.println("FST stores docFreq");
      }
    }
    Terms terms = MultiFields.getTerms(r, "body");
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator();
      BytesRef term;
      int ord = 0;
      while((term = termsEnum.next()) != null) {
        if (ord == 0) {
          try {
            termsEnum.ord();
          } catch (UnsupportedOperationException uoe) {
            storeOrd = false;
          }
        }
        final int output;
        if (storeOrd) {
          output = ord;
        } else {
          output = termsEnum.docFreq();
        }
        builder.add(term, outputs.get(output));
        ord++;
      }
      final FST<Long> fst = builder.finish();
      if (VERBOSE) {
        System.out.println("FST: " + docCount + " docs; " + ord + " terms; " + fst.getNodeCount() + " nodes; " + fst.getArcCount() + " arcs;" + " " + fst.sizeInBytes() + " bytes");
      }

      if (ord > 0) {
        // Now confirm BytesRefFSTEnum and TermsEnum act the
        // same:
        final BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<Long>(fst);
        for(int iter=0;iter<1000*RANDOM_MULTIPLIER;iter++) {
          fstEnum.reset();
          final BytesRef randomTerm = new BytesRef(getRandomString());
        
          final TermsEnum.SeekStatus seekResult = termsEnum.seek(randomTerm);
          final BytesRefFSTEnum.InputOutput fstSeekResult = fstEnum.advance(randomTerm);

          if (VERBOSE) {
            System.out.println("TEST: seek " + randomTerm.utf8ToString());
          }

          if (seekResult == TermsEnum.SeekStatus.END) {
            assertNull(fstSeekResult);
          } else {
            assertSame(termsEnum, fstEnum, storeOrd);
            for(int nextIter=0;nextIter<10;nextIter++) {
              if (VERBOSE) {
                System.out.println("TEST: next");
              }
              if (termsEnum.next() != null) {
                if (VERBOSE) {
                  System.out.println("  term=" + termsEnum.term().utf8ToString());
                }
                assertNotNull(fstEnum.next());
                assertSame(termsEnum, fstEnum, storeOrd);
              } else {
                BytesRefFSTEnum.InputOutput<Long> nextResult = fstEnum.next();
                if (nextResult != null) {
                  System.out.println("expected null but got: input=" + nextResult.input.utf8ToString() + " output=" + outputs.outputToString(nextResult.output));
                  fail();
                }
                break;
              }
            }
          }
        }
      }
    }

    r.close();
    dir.close();
  }

  private void assertSame(TermsEnum termsEnum, BytesRefFSTEnum fstEnum, boolean storeOrd) throws Exception {
    if (termsEnum.term() == null) {
      assertNull(fstEnum.current());
    } else {
      assertEquals(termsEnum.term(), fstEnum.current().input);
      if (storeOrd) {
        // fst stored the ord
        assertEquals(termsEnum.ord(), ((Long) fstEnum.current().output).longValue());
      } else {
        // fst stored the docFreq
        assertEquals(termsEnum.docFreq(), (int) (((Long) fstEnum.current().output).longValue()));
      }
    }
  }

  private static abstract class VisitTerms<T> {
    private final String dirOut;
    private final String wordsFileIn;
    private int inputMode;
    private final Outputs<T> outputs;
    private final Builder<T> builder;

    public VisitTerms(String dirOut, String wordsFileIn, int inputMode, int prune, Outputs<T> outputs) {
      this.dirOut = dirOut;
      this.wordsFileIn = wordsFileIn;
      this.inputMode = inputMode;
      this.outputs = outputs;
      
      builder = new Builder<T>(inputMode == 0 ? FST.INPUT_TYPE.BYTE1 : FST.INPUT_TYPE.BYTE4, 0, prune, prune == 0, outputs);
    }

    protected abstract T getOutput(IntsRef input, int ord) throws IOException;

    public void run(int limit) throws IOException {
      BufferedReader is = new BufferedReader(new InputStreamReader(new FileInputStream(wordsFileIn), "UTF-8"), 65536);
      try {
        final IntsRef intsRef = new IntsRef(10);
        long tStart = System.currentTimeMillis();
        int ord = 0;
        while(true) {
          String w = is.readLine();
          if (w == null) {
            break;
          }
          toIntsRef(w, inputMode, intsRef);
          builder.add(intsRef,
                      getOutput(intsRef, ord));

          ord++;
          if (ord % 500000 == 0) {
            System.out.println(((System.currentTimeMillis()-tStart)/1000.0) + "s: " + ord + "...");
          }
          if (ord >= limit) {
            break;
          }
        }

        assert builder.getTermCount() == ord;
        final FST<T> fst = builder.finish();
        if (fst == null) {
          System.out.println("FST was fully pruned!");
          System.exit(0);
        }

        System.out.println(ord + " terms; " + fst.getNodeCount() + " nodes; " + fst.getArcCount() + " arcs; " + fst.getArcWithOutputCount() + " arcs w/ output; tot size " + fst.sizeInBytes());
        if (fst.getNodeCount() < 100) {
          PrintStream ps = new PrintStream("out.dot");
          fst.toDot(ps);
          ps.close();
          System.out.println("Wrote FST to out.dot");
        }

        Directory dir = FSDirectory.open(new File(dirOut));
        IndexOutput out = dir.createOutput("fst.bin");
        fst.save(out);
        out.close();

        System.out.println("Saved FST to fst.bin.");

        System.out.println("\nNow verify...");

        is.close();
        is = new BufferedReader(new InputStreamReader(new FileInputStream(wordsFileIn), "UTF-8"), 65536);

        ord = 0;
        tStart = System.currentTimeMillis();
        while(true) {
          String w = is.readLine();
          if (w == null) {
            break;
          }
          toIntsRef(w, inputMode, intsRef);
          T expected = getOutput(intsRef, ord);
          T actual = fst.get(intsRef);
          if (actual == null) {
            throw new RuntimeException("unexpected null output on input=" + w);
          }
          if (!actual.equals(expected)) {
            throw new RuntimeException("wrong output (got " + outputs.outputToString(actual) + " but expected " + outputs.outputToString(expected) + ") on input=" + w);
          }

          ord++;
          if (ord % 500000 == 0) {
            System.out.println(((System.currentTimeMillis()-tStart)/1000.0) + "s: " + ord + "...");
          }
          if (ord >= limit) {
            break;
          }
        }

        double totSec = ((System.currentTimeMillis() - tStart)/1000.0);
        System.out.println("Verify took " + totSec + " sec + (" + (int) ((totSec*1000000000/ord)) + " nsec per lookup)");

      } finally {
        is.close();
      }
    }
  }

  // java -cp build/classes/test:build/classes/java:lib/junit-4.7.jar org.apache.lucene.util.automaton.fst.TestFSTs /x/tmp/allTerms3.txt out
  public static void main(String[] args) throws IOException {
    final String wordsFileIn = args[0];
    final String dirOut = args[1];
    int idx = 2;
    int prune = 0;
    int limit = Integer.MAX_VALUE;
    int inputMode = 0;                             // utf8
    boolean storeOrds = false;
    boolean storeDocFreqs = false;
    while(idx < args.length) {
      if (args[idx].equals("-prune")) {
        prune = Integer.valueOf(args[1+idx]);
        idx++;
      }
      if (args[idx].equals("-limit")) {
        limit = Integer.valueOf(args[1+idx]);
        idx++;
      }
      if (args[idx].equals("-utf8")) {
        inputMode = 0;
      }
      if (args[idx].equals("-utf32")) {
        inputMode = 1;
      }
      if (args[idx].equals("-docFreq")) {
        storeDocFreqs = true;
      }
      if (args[idx].equals("-ords")) {
        storeOrds = true;
      }
      idx++;
    }

    // ord benefits from share, docFreqs don't:

    if (storeOrds && storeDocFreqs) {
      // Store both ord & docFreq:
      final PositiveIntOutputs o1 = PositiveIntOutputs.getSingleton(true);
      final PositiveIntOutputs o2 = PositiveIntOutputs.getSingleton(false);
      final PairOutputs<Long,Long> outputs = new PairOutputs<Long,Long>(o1, o2);
      new VisitTerms<PairOutputs.Pair<Long,Long>>(dirOut, wordsFileIn, inputMode, prune, outputs) {
        Random rand;
        @Override
        public PairOutputs.Pair<Long,Long> getOutput(IntsRef input, int ord) {
          if (ord == 0) {
            rand = new Random(17);
          }
          return new PairOutputs.Pair<Long,Long>(o1.get(ord),
                                                 o2.get(_TestUtil.nextInt(rand, 1, 5000)));
        }
      }.run(limit);
    } else if (storeOrds) {
      // Store only ords
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
      new VisitTerms<Long>(dirOut, wordsFileIn, inputMode, prune, outputs) {
        @Override
        public Long getOutput(IntsRef input, int ord) {
          return outputs.get(ord);
        }
      }.run(limit);
    } else if (storeDocFreqs) {
      // Store only docFreq
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(false);
      new VisitTerms<Long>(dirOut, wordsFileIn, inputMode, prune, outputs) {
        Random rand;
        @Override
        public Long getOutput(IntsRef input, int ord) {
          if (ord == 0) {
            rand = new Random(17);
          }
          return outputs.get(_TestUtil.nextInt(rand, 1, 5000));
        }
      }.run(limit);
    } else {
      // Store nothing
      final NoOutputs outputs = NoOutputs.getSingleton();
      final Object NO_OUTPUT = outputs.getNoOutput();
      new VisitTerms<Object>(dirOut, wordsFileIn, inputMode, prune, outputs) {
        @Override
        public Object getOutput(IntsRef input, int ord) {
          return NO_OUTPUT;
        }
      }.run(limit);
    }
  }
}
