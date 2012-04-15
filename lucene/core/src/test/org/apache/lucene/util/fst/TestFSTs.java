package org.apache.lucene.util.fst;

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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.*;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.UseNoMemoryExpensiveCodec;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.fst.BytesRefFSTEnum.InputOutput;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.PairOutputs.Pair;

@UseNoMemoryExpensiveCodec
public class TestFSTs extends LuceneTestCase {

  private MockDirectoryWrapper dir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    dir.setPreventDoubleWrite(false);
  }

  @Override
  public void tearDown() throws Exception {
    // can be null if we force simpletext (funky, some kind of bug in test runner maybe)
    if (dir != null) dir.close();
    super.tearDown();
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

  static IntsRef toIntsRef(String s, int inputMode) {
    return toIntsRef(s, inputMode, new IntsRef(10));
  }

  static IntsRef toIntsRef(String s, int inputMode, IntsRef ir) {
    if (inputMode == 0) {
      // utf8
      return toIntsRef(new BytesRef(s), ir);
    } else {
      // utf32
      return toIntsRefUTF32(s, ir);
    }
  }

  static IntsRef toIntsRefUTF32(String s, IntsRef ir) {
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

  static IntsRef toIntsRef(BytesRef br, IntsRef ir) {
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
    String[] strings = new String[] {"station", "commotion", "elation", "elastic", "plastic", "stop", "ftop", "ftation", "stat"};
    String[] strings2 = new String[] {"station", "commotion", "elation", "elastic", "plastic", "stop", "ftop", "ftation"};
    IntsRef[] terms = new IntsRef[strings.length];
    IntsRef[] terms2 = new IntsRef[strings2.length];
    for(int inputMode=0;inputMode<2;inputMode++) {
      if (VERBOSE) {
        System.out.println("TEST: inputMode=" + inputModeToString(inputMode));
      }

      for(int idx=0;idx<strings.length;idx++) {
        terms[idx] = toIntsRef(strings[idx], inputMode);
      }
      for(int idx=0;idx<strings2.length;idx++) {
        terms2[idx] = toIntsRef(strings2[idx], inputMode);
      }
      Arrays.sort(terms2);

      doTest(inputMode, terms);
    
      // Test pre-determined FST sizes to make sure we haven't lost minimality (at least on this trivial set of terms):

      // FSA
      {
        final Outputs<Object> outputs = NoOutputs.getSingleton();
        final Object NO_OUTPUT = outputs.getNoOutput();      
        final List<FSTTester.InputOutput<Object>> pairs = new ArrayList<FSTTester.InputOutput<Object>>(terms2.length);
        for(IntsRef term : terms2) {
          pairs.add(new FSTTester.InputOutput<Object>(term, NO_OUTPUT));
        }
        FST<Object> fst = new FSTTester<Object>(random(), dir, inputMode, pairs, outputs, false).doTest(0, 0, false);
        assertNotNull(fst);
        assertEquals(22, fst.getNodeCount());
        assertEquals(27, fst.getArcCount());
      }

      // FST ord pos int
      {
        final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
        final List<FSTTester.InputOutput<Long>> pairs = new ArrayList<FSTTester.InputOutput<Long>>(terms2.length);
        for(int idx=0;idx<terms2.length;idx++) {
          pairs.add(new FSTTester.InputOutput<Long>(terms2[idx], (long) idx));
        }
        final FST<Long> fst = new FSTTester<Long>(random(), dir, inputMode, pairs, outputs, true).doTest(0, 0, false);
        assertNotNull(fst);
        assertEquals(22, fst.getNodeCount());
        assertEquals(27, fst.getArcCount());
      }

      // FST byte sequence ord
      {
        final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
        final BytesRef NO_OUTPUT = outputs.getNoOutput();      
        final List<FSTTester.InputOutput<BytesRef>> pairs = new ArrayList<FSTTester.InputOutput<BytesRef>>(terms2.length);
        for(int idx=0;idx<terms2.length;idx++) {
          final BytesRef output = random().nextInt(30) == 17 ? NO_OUTPUT : new BytesRef(Integer.toString(idx));
          pairs.add(new FSTTester.InputOutput<BytesRef>(terms2[idx], output));
        }
        final FST<BytesRef> fst = new FSTTester<BytesRef>(random(), dir, inputMode, pairs, outputs, false).doTest(0, 0, false);
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
      buffer[i] = (char) _TestUtil.nextInt(r, 97, 102);
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
      new FSTTester<Object>(random(), dir, inputMode, pairs, outputs, false).doTest();
    }

    // PositiveIntOutput (ord)
    {
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
      final List<FSTTester.InputOutput<Long>> pairs = new ArrayList<FSTTester.InputOutput<Long>>(terms.length);
      for(int idx=0;idx<terms.length;idx++) {
        pairs.add(new FSTTester.InputOutput<Long>(terms[idx], (long) idx));
      }
      new FSTTester<Long>(random(), dir, inputMode, pairs, outputs, true).doTest();
    }

    // PositiveIntOutput (random monotonically increasing positive number)
    {
      final boolean doShare = random().nextBoolean();
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(doShare);
      final List<FSTTester.InputOutput<Long>> pairs = new ArrayList<FSTTester.InputOutput<Long>>(terms.length);
      long lastOutput = 0;
      for(int idx=0;idx<terms.length;idx++) {
        final long value = lastOutput + _TestUtil.nextInt(random(), 1, 1000);
        lastOutput = value;
        pairs.add(new FSTTester.InputOutput<Long>(terms[idx], value));
      }
      new FSTTester<Long>(random(), dir, inputMode, pairs, outputs, doShare).doTest();
    }

    // PositiveIntOutput (random positive number)
    {
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(random().nextBoolean());
      final List<FSTTester.InputOutput<Long>> pairs = new ArrayList<FSTTester.InputOutput<Long>>(terms.length);
      for(int idx=0;idx<terms.length;idx++) {
        pairs.add(new FSTTester.InputOutput<Long>(terms[idx], random().nextLong() & Long.MAX_VALUE));
      }
      new FSTTester<Long>(random(), dir, inputMode, pairs, outputs, false).doTest();
    }

    // Pair<ord, (random monotonically increasing positive number>
    {
      final PositiveIntOutputs o1 = PositiveIntOutputs.getSingleton(random().nextBoolean());
      final PositiveIntOutputs o2 = PositiveIntOutputs.getSingleton(random().nextBoolean());
      final PairOutputs<Long,Long> outputs = new PairOutputs<Long,Long>(o1, o2);
      final List<FSTTester.InputOutput<PairOutputs.Pair<Long,Long>>> pairs = new ArrayList<FSTTester.InputOutput<PairOutputs.Pair<Long,Long>>>(terms.length);
      long lastOutput = 0;
      for(int idx=0;idx<terms.length;idx++) {
        final long value = lastOutput + _TestUtil.nextInt(random(), 1, 1000);
        lastOutput = value;
        pairs.add(new FSTTester.InputOutput<PairOutputs.Pair<Long,Long>>(terms[idx],
                                                                         outputs.newPair((long) idx, value)));
      }
      new FSTTester<PairOutputs.Pair<Long,Long>>(random(), dir, inputMode, pairs, outputs, false).doTest();
    }

    // Sequence-of-bytes
    {
      final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
      final BytesRef NO_OUTPUT = outputs.getNoOutput();      
      final List<FSTTester.InputOutput<BytesRef>> pairs = new ArrayList<FSTTester.InputOutput<BytesRef>>(terms.length);
      for(int idx=0;idx<terms.length;idx++) {
        final BytesRef output = random().nextInt(30) == 17 ? NO_OUTPUT : new BytesRef(Integer.toString(idx));
        pairs.add(new FSTTester.InputOutput<BytesRef>(terms[idx], output));
      }
      new FSTTester<BytesRef>(random(), dir, inputMode, pairs, outputs, false).doTest();
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
      new FSTTester<IntsRef>(random(), dir, inputMode, pairs, outputs, false).doTest();
    }

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
          output = outputs.get(value, value2);
        } else {
          output = outputs.get(value);
        }
        pairs.add(new FSTTester.InputOutput<Object>(terms[idx], output));
      }
      new FSTTester<Object>(random(), dir, inputMode, pairs, outputs, false).doTest();
    }
  }

  private static class FSTTester<T> {

    final Random random;
    final List<InputOutput<T>> pairs;
    final int inputMode;
    final Outputs<T> outputs;
    final Directory dir;
    final boolean doReverseLookup;

    public FSTTester(Random random, Directory dir, int inputMode, List<InputOutput<T>> pairs, Outputs<T> outputs, boolean doReverseLookup) {
      this.random = random;
      this.dir = dir;
      this.inputMode = inputMode;
      this.pairs = pairs;
      this.outputs = outputs;
      this.doReverseLookup = doReverseLookup;
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

    public void doTest() throws IOException {
      // no pruning
      doTest(0, 0, true);

      if (!(outputs instanceof UpToTwoPositiveIntOutputs)) {
        // simple pruning
        doTest(_TestUtil.nextInt(random, 1, 1+pairs.size()), 0, true);
        
        // leafy pruning
        doTest(0, _TestUtil.nextInt(random, 1, 1+pairs.size()), true);
      }
    }

    // runs the term, returning the output, or null if term
    // isn't accepted.  if prefixLength is non-null it must be
    // length 1 int array; prefixLength[0] is set to the length
    // of the term prefix that matches
    private T run(FST<T> fst, IntsRef term, int[] prefixLength) throws IOException {
      assert prefixLength == null || prefixLength.length == 1;
      final FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<T>());
      final T NO_OUTPUT = fst.outputs.getNoOutput();
      T output = NO_OUTPUT;
      final FST.BytesReader fstReader = fst.getBytesReader(0);

      for(int i=0;i<=term.length;i++) {
        final int label;
        if (i == term.length) {
          label = FST.END_LABEL;
        } else {
          label = term.ints[term.offset+i];
        }
        // System.out.println("   loop i=" + i + " label=" + label + " output=" + fst.outputs.outputToString(output) + " curArc: target=" + arc.target + " isFinal?=" + arc.isFinal());
        if (fst.findTargetArc(label, arc, arc, fstReader) == null) {
          // System.out.println("    not found");
          if (prefixLength != null) {
            prefixLength[0] = i;
            return output;
          } else {
            return null;
          }
        }
        output = fst.outputs.add(output, arc.output);
      }

      if (prefixLength != null) {
        prefixLength[0] = term.length;
      }

      return output;
    }

    private T randomAcceptedWord(FST<T> fst, IntsRef in) throws IOException {
      FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<T>());

      final List<FST.Arc<T>> arcs = new ArrayList<FST.Arc<T>>();
      in.length = 0;
      in.offset = 0;
      final T NO_OUTPUT = fst.outputs.getNoOutput();
      T output = NO_OUTPUT;

      while(true) {
        // read all arcs:
        fst.readFirstTargetArc(arc, arc);
        arcs.add(new FST.Arc<T>().copyFrom(arc));
        while(!arc.isLast()) {
          fst.readNextArc(arc);
          arcs.add(new FST.Arc<T>().copyFrom(arc));
        }
      
        // pick one
        arc = arcs.get(random.nextInt(arcs.size()));
        arcs.clear();

        // accumulate output
        output = fst.outputs.add(output, arc.output);

        // append label
        if (arc.label == FST.END_LABEL) {
          break;
        }

        if (in.ints.length == in.length) {
          in.grow(1+in.length);
        }
        in.ints[in.length++] = arc.label;
      }

      return output;
    }


    FST<T> doTest(int prune1, int prune2, boolean allowRandomSuffixSharing) throws IOException {
      if (VERBOSE) {
        System.out.println("\nTEST: prune1=" + prune1 + " prune2=" + prune2);
      }

      final boolean willRewrite = random.nextBoolean();

      final Builder<T> builder = new Builder<T>(inputMode == 0 ? FST.INPUT_TYPE.BYTE1 : FST.INPUT_TYPE.BYTE4,
                                                prune1, prune2,
                                                prune1==0 && prune2==0,
                                                allowRandomSuffixSharing ? random.nextBoolean() : true,
                                                allowRandomSuffixSharing ? _TestUtil.nextInt(random, 1, 10) : Integer.MAX_VALUE,
                                                outputs,
                                                null,
                                                willRewrite);

      for(InputOutput<T> pair : pairs) {
        if (pair.output instanceof UpToTwoPositiveIntOutputs.TwoLongs) {
          final UpToTwoPositiveIntOutputs _outputs = (UpToTwoPositiveIntOutputs) outputs;
          final UpToTwoPositiveIntOutputs.TwoLongs twoLongs = (UpToTwoPositiveIntOutputs.TwoLongs) pair.output;
          @SuppressWarnings("unchecked") final Builder<Object> builderObject = (Builder<Object>) builder;
          builderObject.add(pair.input, _outputs.get(twoLongs.first));
          builderObject.add(pair.input, _outputs.get(twoLongs.second));
        } else {
          builder.add(pair.input, pair.output);
        }
      }
      FST<T> fst = builder.finish();

      if (random.nextBoolean() && fst != null && !willRewrite) {
        TestFSTs t = new TestFSTs();
        IOContext context = LuceneTestCase.newIOContext(random);
        IndexOutput out = dir.createOutput("fst.bin", context);
        fst.save(out);
        out.close();
        IndexInput in = dir.openInput("fst.bin", context);
        try {
          fst = new FST<T>(in, outputs);
        } finally {
          in.close();
          dir.deleteFile("fst.bin");
        }
      }

      if (VERBOSE && pairs.size() <= 20 && fst != null) {
        Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"), "UTF-8");
        Util.toDot(fst, w, false, false);
        w.close();
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

      if (willRewrite && fst != null) {
        if (VERBOSE) {
          System.out.println("TEST: now rewrite");
        }
        final FST<T> packed = fst.pack(_TestUtil.nextInt(random, 1, 10), _TestUtil.nextInt(random, 0, 10000000));
        if (VERBOSE) {
          System.out.println("TEST: now verify packed FST");
        }
        if (prune1 == 0 && prune2 == 0) {
          verifyUnPruned(inputMode, packed);
        } else {
          verifyPruned(inputMode, packed, prune1, prune2);
        }
      }

      return fst;
    }

    // FST is complete
    private void verifyUnPruned(int inputMode, FST<T> fst) throws IOException {

      final FST<Long> fstLong;
      final Set<Long> validOutputs;
      long minLong = Long.MAX_VALUE;
      long maxLong = Long.MIN_VALUE;

      if (doReverseLookup) {
        @SuppressWarnings("unchecked") FST<Long> fstLong0 = (FST<Long>) fst;
        fstLong = fstLong0;
        validOutputs = new HashSet<Long>();
        for(InputOutput<T> pair: pairs) {
          Long output = (Long) pair.output;
          maxLong = Math.max(maxLong, output);
          minLong = Math.min(minLong, output);
          validOutputs.add(output);
        }
      } else {
        fstLong = null;
        validOutputs = null;
      }

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

      // visit valid pairs in order -- make sure all words
      // are accepted, and FSTEnum's next() steps through
      // them correctly
      if (VERBOSE) {
        System.out.println("TEST: check valid terms/next()");
      }
      {
        IntsRefFSTEnum<T> fstEnum = new IntsRefFSTEnum<T>(fst);
        for(InputOutput<T> pair : pairs) {
          IntsRef term = pair.input;
          if (VERBOSE) {
            System.out.println("TEST: check term=" + inputToString(inputMode, term) + " output=" + fst.outputs.outputToString(pair.output));
          }
          Object output = run(fst, term, null);
          assertNotNull("term " + inputToString(inputMode, term) + " is not accepted", output);
          assertEquals(pair.output, output);

          // verify enum's next
          IntsRefFSTEnum.InputOutput<T> t = fstEnum.next();
          assertNotNull(t);
          assertEquals("expected input=" + inputToString(inputMode, term) + " but fstEnum returned " + inputToString(inputMode, t.input), term, t.input);
          assertEquals(pair.output, t.output);
        }
        assertNull(fstEnum.next());
      }

      final Map<IntsRef,T> termsMap = new HashMap<IntsRef,T>();
      for(InputOutput<T> pair : pairs) {
        termsMap.put(pair.input, pair.output);
      }

      if (doReverseLookup && maxLong > minLong) {
        // Do random lookups so we test null (output doesn't
        // exist) case:
        assertNull(Util.getByOutput(fstLong, minLong-7));
        assertNull(Util.getByOutput(fstLong, maxLong+7));

        final int num = atLeast(100);
        for(int iter=0;iter<num;iter++) {
          Long v = minLong + random.nextLong() % (maxLong - minLong);
          IntsRef input = Util.getByOutput(fstLong, v);
          assertTrue(validOutputs.contains(v) || input == null);
        }
      }

      // find random matching word and make sure it's valid
      if (VERBOSE) {
        System.out.println("TEST: verify random accepted terms");
      }
      final IntsRef scratch = new IntsRef(10);
      int num = atLeast(500);
      for(int iter=0;iter<num;iter++) {
        T output = randomAcceptedWord(fst, scratch);
        assertTrue("accepted word " + inputToString(inputMode, scratch) + " is not valid", termsMap.containsKey(scratch));
        assertEquals(termsMap.get(scratch), output);

        if (doReverseLookup) {
          //System.out.println("lookup output=" + output + " outs=" + fst.outputs);
          IntsRef input = Util.getByOutput(fstLong, (Long) output);
          assertNotNull(input);
          //System.out.println("  got " + Util.toBytesRef(input, new BytesRef()).utf8ToString());
          assertEquals(scratch, input);
        }
      }
    
      // test IntsRefFSTEnum.seek:
      if (VERBOSE) {
        System.out.println("TEST: verify seek");
      }
      IntsRefFSTEnum<T> fstEnum = new IntsRefFSTEnum<T>(fst);
      num = atLeast(100);
      for(int iter=0;iter<num;iter++) {
        if (VERBOSE) {
          System.out.println("  iter=" + iter);
        }
        if (random.nextBoolean()) {
          // seek to term that doesn't exist:
          while(true) {
            final IntsRef term = toIntsRef(getRandomString(random), inputMode);
            int pos = Collections.binarySearch(pairs, new InputOutput<T>(term, null));
            if (pos < 0) {
              pos = -(pos+1);
              // ok doesn't exist
              //System.out.println("  seek " + inputToString(inputMode, term));
              final IntsRefFSTEnum.InputOutput<T> seekResult;
              if (random.nextInt(3) == 0) {
                if (VERBOSE) {
                  System.out.println("  do non-exist seekExact term=" + inputToString(inputMode, term));
                }
                seekResult = fstEnum.seekExact(term);
                pos = -1;
              } else if (random.nextBoolean()) {
                if (VERBOSE) {
                  System.out.println("  do non-exist seekFloor term=" + inputToString(inputMode, term));
                }
                seekResult = fstEnum.seekFloor(term);
                pos--;
              } else {
                if (VERBOSE) {
                  System.out.println("  do non-exist seekCeil term=" + inputToString(inputMode, term));
                }
                seekResult = fstEnum.seekCeil(term);
              }

              if (pos != -1 && pos < pairs.size()) {
                //System.out.println("    got " + inputToString(inputMode,seekResult.input) + " output=" + fst.outputs.outputToString(seekResult.output));
                assertNotNull("got null but expected term=" + inputToString(inputMode, pairs.get(pos).input), seekResult);
                if (VERBOSE) {
                  System.out.println("    got " + inputToString(inputMode, seekResult.input));
                }
                assertEquals("expected " + inputToString(inputMode, pairs.get(pos).input) + " but got " + inputToString(inputMode, seekResult.input), pairs.get(pos).input, seekResult.input);
                assertEquals(pairs.get(pos).output, seekResult.output);
              } else {
                // seeked before start or beyond end
                //System.out.println("seek=" + seekTerm);
                assertNull("expected null but got " + (seekResult==null ? "null" : inputToString(inputMode, seekResult.input)), seekResult);
                if (VERBOSE) {
                  System.out.println("    got null");
                }
              }

              break;
            }
          }
        } else {
          // seek to term that does exist:
          InputOutput<T> pair = pairs.get(random.nextInt(pairs.size()));
          final IntsRefFSTEnum.InputOutput<T> seekResult;
          if (random.nextInt(3) == 2) {
            if (VERBOSE) {
              System.out.println("  do exists seekExact term=" + inputToString(inputMode, pair.input));
            }
            seekResult = fstEnum.seekExact(pair.input);
          } else if (random.nextBoolean()) {
            if (VERBOSE) {
              System.out.println("  do exists seekFloor " + inputToString(inputMode, pair.input));
            }
            seekResult = fstEnum.seekFloor(pair.input);
          } else {
            if (VERBOSE) {
              System.out.println("  do exists seekCeil " + inputToString(inputMode, pair.input));
            }
            seekResult = fstEnum.seekCeil(pair.input);
          }
          assertNotNull(seekResult);
          assertEquals("got " + inputToString(inputMode, seekResult.input) + " but expected " + inputToString(inputMode, pair.input), pair.input, seekResult.input);
          assertEquals(pair.output, seekResult.output);
        }
      }

      if (VERBOSE) {
        System.out.println("TEST: mixed next/seek");
      }

      // test mixed next/seek
      num = atLeast(100);
      for(int iter=0;iter<num;iter++) {
        if (VERBOSE) {
          System.out.println("TEST: iter " + iter);
        }
        // reset:
        fstEnum = new IntsRefFSTEnum<T>(fst);
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
              IntsRef term = toIntsRef(getRandomString(random), inputMode);
              if (!termsMap.containsKey(term) && term.compareTo(pairs.get(upto).input) > 0) {
                int pos = Collections.binarySearch(pairs, new InputOutput<T>(term, null));
                assert pos < 0;
                upto = -(pos+1);

                if (random.nextBoolean()) {
                  upto--;
                  assertTrue(upto != -1);
                  if (VERBOSE) {
                    System.out.println("  do non-exist seekFloor(" + inputToString(inputMode, term) + ")");
                  }
                  isDone = fstEnum.seekFloor(term) == null;
                } else {
                  if (VERBOSE) {
                    System.out.println("  do non-exist seekCeil(" + inputToString(inputMode, term) + ")");
                  }
                  isDone = fstEnum.seekCeil(term) == null;
                }

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

            if (random.nextBoolean()) {
              if (VERBOSE) {
                System.out.println("  do seekCeil(" + inputToString(inputMode, pairs.get(upto).input) + ")");
              }
              isDone = fstEnum.seekCeil(pairs.get(upto).input) == null;
            } else {
              if (VERBOSE) {
                System.out.println("  do seekFloor(" + inputToString(inputMode, pairs.get(upto).input) + ")");
              }
              isDone = fstEnum.seekFloor(pairs.get(upto).input) == null;
            }
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

            /*
            if (upto < pairs.size()-1) {
              int tryCount = 0;
              while(tryCount < 10) {
                final IntsRef t = toIntsRef(getRandomString(), inputMode);
                if (pairs.get(upto).input.compareTo(t) < 0) {
                  final boolean expected = t.compareTo(pairs.get(upto+1).input) < 0;
                  if (VERBOSE) {
                    System.out.println("TEST: call beforeNext(" + inputToString(inputMode, t) + "); current=" + inputToString(inputMode, pairs.get(upto).input) + " next=" + inputToString(inputMode, pairs.get(upto+1).input) + " expected=" + expected);
                  }
                  assertEquals(expected, fstEnum.beforeNext(t));
                  break;
                }
                tryCount++;
              }
            }
            */
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
        scratch.copyInts(pair.input);
        for(int idx=0;idx<=pair.input.length;idx++) {
          scratch.length = idx;
          CountMinOutput<T> cmo = prefixes.get(scratch);
          if (cmo == null) {
            cmo = new CountMinOutput<T>();
            cmo.count = 1;
            cmo.output = pair.output;
            prefixes.put(IntsRef.deepCopyOf(scratch), cmo);
          } else {
            cmo.count++;
            T output1 = cmo.output;
            if (output1.equals(outputs.getNoOutput())) {
              output1 = outputs.getNoOutput();
            }
            T output2 = pair.output;
            if (output2.equals(outputs.getNoOutput())) {
              output2 = outputs.getNoOutput();
            }
            cmo.output = outputs.common(output1, output2);
          }
          if (idx == pair.input.length) {
            cmo.isFinal = true;
            cmo.finalOutput = cmo.output;
          }
        }
      }

      if (VERBOSE) {
        System.out.println("TEST: now prune");
      }

      // prune 'em
      final Iterator<Map.Entry<IntsRef,CountMinOutput<T>>> it = prefixes.entrySet().iterator();
      while(it.hasNext()) {
        Map.Entry<IntsRef,CountMinOutput<T>> ent = it.next();
        final IntsRef prefix = ent.getKey();
        final CountMinOutput<T> cmo = ent.getValue();
        if (VERBOSE) {
          System.out.println("  term prefix=" + inputToString(inputMode, prefix, false) + " count=" + cmo.count + " isLeaf=" + cmo.isLeaf + " output=" + outputs.outputToString(cmo.output) + " isFinal=" + cmo.isFinal);
        }
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
          scratch.copyInts(prefix);
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

      if (VERBOSE) {
        System.out.println("TEST: after prune");
        for(Map.Entry<IntsRef,CountMinOutput<T>> ent : prefixes.entrySet()) {
          System.out.println("  " + inputToString(inputMode, ent.getKey(), false) + ": isLeaf=" + ent.getValue().isLeaf + " isFinal=" + ent.getValue().isFinal);
          if (ent.getValue().isFinal) {
            System.out.println("    finalOutput=" + outputs.outputToString(ent.getValue().finalOutput));
          }
        }
      }

      if (prefixes.size() <= 1) {
        assertNull(fst);
        return;
      }

      assertNotNull(fst);

      // make sure FST only enums valid prefixes
      if (VERBOSE) {
        System.out.println("TEST: check pruned enum");
      }
      IntsRefFSTEnum<T> fstEnum = new IntsRefFSTEnum<T>(fst);
      IntsRefFSTEnum.InputOutput<T> current;
      while((current = fstEnum.next()) != null) {
        if (VERBOSE) {
          System.out.println("  fstEnum.next prefix=" + inputToString(inputMode, current.input, false) + " output=" + outputs.outputToString(current.output));
        }
        final CountMinOutput<T> cmo = prefixes.get(current.input);
        assertNotNull(cmo);
        assertTrue(cmo.isLeaf || cmo.isFinal);
        //if (cmo.isFinal && !cmo.isLeaf) {
        if (cmo.isFinal) {
          assertEquals(cmo.finalOutput, current.output);
        } else {
          assertEquals(cmo.output, current.output);
        }
      }

      // make sure all non-pruned prefixes are present in the FST
      if (VERBOSE) {
        System.out.println("TEST: verify all prefixes");
      }
      final int[] stopNode = new int[1];
      for(Map.Entry<IntsRef,CountMinOutput<T>> ent : prefixes.entrySet()) {
        if (ent.getKey().length > 0) {
          final CountMinOutput<T> cmo = ent.getValue();
          final T output = run(fst, ent.getKey(), stopNode);
          if (VERBOSE) {
            System.out.println("TEST: verify prefix=" + inputToString(inputMode, ent.getKey(), false) + " output=" + outputs.outputToString(cmo.output));
          }
          // if (cmo.isFinal && !cmo.isLeaf) {
          if (cmo.isFinal) {
            assertEquals(cmo.finalOutput, output);
          } else {
            assertEquals(cmo.output, output);
          }
          assertEquals(ent.getKey().length, stopNode[0]);
        }
      }
    }
  }

  public void testRandomWords() throws IOException {
    testRandomWords(1000, atLeast(2));
    //testRandomWords(100, 1);
  }

  String inputModeToString(int mode) {
    if (mode == 0) {
      return "utf8";
    } else {
      return "utf32";
    }
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

  static String getRandomString(Random random) {
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
    testRandomWords(_TestUtil.nextInt(random(), 50000, 60000), 1);
  }
  
  static String inputToString(int inputMode, IntsRef term) {
    return inputToString(inputMode, term, true);
  }

  private static String inputToString(int inputMode, IntsRef term, boolean isValidUnicode) {
    if (!isValidUnicode) {
      return term.toString();
    } else if (inputMode == 0) {
      // utf8
      return toBytesRef(term).utf8ToString() + " " + term;
    } else {
      // utf32
      return UnicodeUtil.newString(term.ints, term.offset, term.length) + " " + term;
    }
  }

  // Build FST for all unique terms in the test line docs
  // file, up until a time limit
  public void testRealTerms() throws Exception {

    // TODO: is this necessary? we use the annotation...
    final String defaultFormat = _TestUtil.getPostingsFormat("abracadabra");
    if (defaultFormat.equals("SimpleText") || defaultFormat.equals("Memory")) {
      // no
      Codec.setDefault(_TestUtil.alwaysPostingsFormat(new Lucene40PostingsFormat()));
    }

    final LineFileDocs docs = new LineFileDocs(random(), defaultCodecSupportsDocValues());
    final int RUN_TIME_MSEC = atLeast(500);
    final IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(-1).setRAMBufferSizeMB(64);
    final File tempDir = _TestUtil.getTempDir("fstlines");
    final MockDirectoryWrapper dir = newFSDirectory(tempDir);
    final IndexWriter writer = new IndexWriter(dir, conf);
    final long stopTime = System.currentTimeMillis() + RUN_TIME_MSEC;
    Document doc;
    int docCount = 0;
    while((doc = docs.nextDoc()) != null && System.currentTimeMillis() < stopTime) {
      writer.addDocument(doc);
      docCount++;
    }
    IndexReader r = IndexReader.open(writer, true);
    writer.close();
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(random().nextBoolean());

    final boolean doRewrite = random().nextBoolean();

    Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, outputs, null, doRewrite);

    boolean storeOrd = random().nextBoolean();
    if (VERBOSE) {
      if (storeOrd) {
        System.out.println("FST stores ord");
      } else {
        System.out.println("FST stores docFreq");
      }
    }
    Terms terms = MultiFields.getTerms(r, "body");
    if (terms != null) {
      final IntsRef scratchIntsRef = new IntsRef();
      final TermsEnum termsEnum = terms.iterator(null);
      if (VERBOSE) {
        System.out.println("TEST: got termsEnum=" + termsEnum);
      }
      BytesRef term;
      int ord = 0;
      while((term = termsEnum.next()) != null) {
        if (ord == 0) {
          try {
            termsEnum.ord();
          } catch (UnsupportedOperationException uoe) {
            if (VERBOSE) {
              System.out.println("TEST: codec doesn't support ord; FST stores docFreq");
            }
            storeOrd = false;
          }
        }
        final int output;
        if (storeOrd) {
          output = ord;
        } else {
          output = termsEnum.docFreq();
        }
        builder.add(Util.toIntsRef(term, scratchIntsRef), (long) output);
        ord++;
        if (VERBOSE && ord % 100000 == 0 && LuceneTestCase.TEST_NIGHTLY) {
          System.out.println(ord + " terms...");
        }
      }
      FST<Long> fst = builder.finish();
      if (VERBOSE) {
        System.out.println("FST: " + docCount + " docs; " + ord + " terms; " + fst.getNodeCount() + " nodes; " + fst.getArcCount() + " arcs;" + " " + fst.sizeInBytes() + " bytes");
      }

      if (ord > 0) {
        final Random random = new Random(random().nextLong());
        for(int rewriteIter=0;rewriteIter<2;rewriteIter++) {
          if (rewriteIter == 1) {
            if (doRewrite) {
              // Verify again, with packed FST:
              fst = fst.pack(_TestUtil.nextInt(random, 1, 10), _TestUtil.nextInt(random, 0, 10000000));
            } else {
              break;
            }
          }
          // Now confirm BytesRefFSTEnum and TermsEnum act the
          // same:
          final BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<Long>(fst);
          int num = atLeast(1000);
          for(int iter=0;iter<num;iter++) {
            final BytesRef randomTerm = new BytesRef(getRandomString(random));
        
            if (VERBOSE) {
              System.out.println("TEST: seek non-exist " + randomTerm.utf8ToString() + " " + randomTerm);
            }

            final TermsEnum.SeekStatus seekResult = termsEnum.seekCeil(randomTerm);
            final InputOutput<Long> fstSeekResult = fstEnum.seekCeil(randomTerm);

            if (seekResult == TermsEnum.SeekStatus.END) {
              assertNull("got " + (fstSeekResult == null ? "null" : fstSeekResult.input.utf8ToString()) + " but expected null", fstSeekResult);
            } else {
              assertSame(termsEnum, fstEnum, storeOrd);
              for(int nextIter=0;nextIter<10;nextIter++) {
                if (VERBOSE) {
                  System.out.println("TEST: next");
                  if (storeOrd) {
                    System.out.println("  ord=" + termsEnum.ord());
                  }
                }
                if (termsEnum.next() != null) {
                  if (VERBOSE) {
                    System.out.println("  term=" + termsEnum.term().utf8ToString());
                  }
                  assertNotNull(fstEnum.next());
                  assertSame(termsEnum, fstEnum, storeOrd);
                } else {
                  if (VERBOSE) {
                    System.out.println("  end!");
                  }
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
    }

    r.close();
    dir.close();
  }

  private void assertSame(TermsEnum termsEnum, BytesRefFSTEnum<?> fstEnum, boolean storeOrd) throws Exception {
    if (termsEnum.term() == null) {
      assertNull(fstEnum.current());
    } else {
      assertNotNull(fstEnum.current());
      assertEquals(termsEnum.term().utf8ToString() + " != " + fstEnum.current().input.utf8ToString(), termsEnum.term(), fstEnum.current().input);
      if (storeOrd) {
        // fst stored the ord
        assertEquals("term=" + termsEnum.term().utf8ToString() + " " + termsEnum.term(), termsEnum.ord(), ((Long) fstEnum.current().output).longValue());
      } else {
        // fst stored the docFreq
        assertEquals("term=" + termsEnum.term().utf8ToString() + " " + termsEnum.term(), termsEnum.docFreq(), (int) (((Long) fstEnum.current().output).longValue()));
      }
    }
  }

  private static abstract class VisitTerms<T> {
    private final String dirOut;
    private final String wordsFileIn;
    private int inputMode;
    private final Outputs<T> outputs;
    private final Builder<T> builder;
    private final boolean doPack;

    public VisitTerms(String dirOut, String wordsFileIn, int inputMode, int prune, Outputs<T> outputs, boolean doPack, boolean noArcArrays) {
      this.dirOut = dirOut;
      this.wordsFileIn = wordsFileIn;
      this.inputMode = inputMode;
      this.outputs = outputs;
      this.doPack = doPack;

      builder = new Builder<T>(inputMode == 0 ? FST.INPUT_TYPE.BYTE1 : FST.INPUT_TYPE.BYTE4, 0, prune, prune == 0, true, Integer.MAX_VALUE, outputs, null, doPack);
      builder.setAllowArrayArcs(!noArcArrays);
    }

    protected abstract T getOutput(IntsRef input, int ord) throws IOException;

    public void run(int limit, boolean verify, boolean verifyByOutput) throws IOException {
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
            System.out.println(
                String.format(Locale.ENGLISH, 
                    "%6.2fs: %9d...", ((System.currentTimeMillis() - tStart) / 1000.0), ord));
          }
          if (ord >= limit) {
            break;
          }
        }

        assert builder.getTermCount() == ord;
        FST<T> fst = builder.finish();
        if (fst == null) {
          System.out.println("FST was fully pruned!");
          System.exit(0);
        }

        if (dirOut == null) {
          return;
        }

        System.out.println(ord + " terms; " + fst.getNodeCount() + " nodes; " + fst.getArcCount() + " arcs; " + fst.getArcWithOutputCount() + " arcs w/ output; tot size " + fst.sizeInBytes());
        if (fst.getNodeCount() < 100) {
          Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"), "UTF-8");
          Util.toDot(fst, w, false, false);
          w.close();
          System.out.println("Wrote FST to out.dot");
        }

        if (doPack) {
          System.out.println("Pack...");
          fst = fst.pack(4, 100000000);
          System.out.println("New size " + fst.sizeInBytes() + " bytes");
        }
        
        Directory dir = FSDirectory.open(new File(dirOut));
        IndexOutput out = dir.createOutput("fst.bin", IOContext.DEFAULT);
        fst.save(out);
        out.close();
        System.out.println("Saved FST to fst.bin.");

        if (!verify) {
          return;
        }

        System.out.println("\nNow verify...");

        while(true) {
          for(int iter=0;iter<2;iter++) {
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
              if (iter == 0) {
                T expected = getOutput(intsRef, ord);
                T actual = Util.get(fst, intsRef);
                if (actual == null) {
                  throw new RuntimeException("unexpected null output on input=" + w);
                }
                if (!actual.equals(expected)) {
                  throw new RuntimeException("wrong output (got " + outputs.outputToString(actual) + " but expected " + outputs.outputToString(expected) + ") on input=" + w);
                }
              } else {
                // Get by output
                final Long output = (Long) getOutput(intsRef, ord);
                @SuppressWarnings("unchecked") final IntsRef actual = Util.getByOutput((FST<Long>) fst, output.longValue());
                if (actual == null) {
                  throw new RuntimeException("unexpected null input from output=" + output);
                }
                if (!actual.equals(intsRef)) {
                  throw new RuntimeException("wrong input (got " + actual + " but expected " + intsRef + " from output=" + output);
                }
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
            System.out.println("Verify " + (iter == 1 ? "(by output) " : "") + "took " + totSec + " sec + (" + (int) ((totSec*1000000000/ord)) + " nsec per lookup)");

            if (!verifyByOutput) {
              break;
            }
          }

          // NOTE: comment out to profile lookup...
          break;
        }

      } finally {
        is.close();
      }
    }
  }

  // java -cp build/classes/test:build/classes/test-framework:build/classes/java:lib/junit-4.10.jar org.apache.lucene.util.fst.TestFSTs /x/tmp/allTerms3.txt out
  public static void main(String[] args) throws IOException {
    int prune = 0;
    int limit = Integer.MAX_VALUE;
    int inputMode = 0;                             // utf8
    boolean storeOrds = false;
    boolean storeDocFreqs = false;
    boolean verify = true;
    boolean doPack = false;
    boolean noArcArrays = false;
    String wordsFileIn = null;
    String dirOut = null;

    int idx = 0;
    while (idx < args.length) {
      if (args[idx].equals("-prune")) {
        prune = Integer.valueOf(args[1 + idx]);
        idx++;
      } else if (args[idx].equals("-limit")) {
        limit = Integer.valueOf(args[1 + idx]);
        idx++;
      } else if (args[idx].equals("-utf8")) {
        inputMode = 0;
      } else if (args[idx].equals("-utf32")) {
        inputMode = 1;
      } else if (args[idx].equals("-docFreq")) {
        storeDocFreqs = true;
      } else if (args[idx].equals("-noArcArrays")) {
        noArcArrays = true;
      } else if (args[idx].equals("-ords")) {
        storeOrds = true;
      } else if (args[idx].equals("-noverify")) {
        verify = false;
      } else if (args[idx].equals("-pack")) {
        doPack = true;
      } else if (args[idx].startsWith("-")) {
        System.err.println("Unrecognized option: " + args[idx]);
        System.exit(-1);
      } else {
        if (wordsFileIn == null) {
          wordsFileIn = args[idx];
        } else if (dirOut == null) {
          dirOut = args[idx];
        } else {
          System.err.println("Too many arguments, expected: input [output]");
          System.exit(-1);
        }
      }
      idx++;
    }
    
    if (wordsFileIn == null) {
      System.err.println("No input file.");
      System.exit(-1);
    }

    // ord benefits from share, docFreqs don't:

    if (storeOrds && storeDocFreqs) {
      // Store both ord & docFreq:
      final PositiveIntOutputs o1 = PositiveIntOutputs.getSingleton(true);
      final PositiveIntOutputs o2 = PositiveIntOutputs.getSingleton(false);
      final PairOutputs<Long,Long> outputs = new PairOutputs<Long,Long>(o1, o2);
      new VisitTerms<PairOutputs.Pair<Long,Long>>(dirOut, wordsFileIn, inputMode, prune, outputs, doPack, noArcArrays) {
        Random rand;
        @Override
        public PairOutputs.Pair<Long,Long> getOutput(IntsRef input, int ord) {
          if (ord == 0) {
            rand = new Random(17);
          }
          return outputs.newPair((long) ord,
                                 (long) _TestUtil.nextInt(rand, 1, 5000));
        }
      }.run(limit, verify, false);
    } else if (storeOrds) {
      // Store only ords
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
      new VisitTerms<Long>(dirOut, wordsFileIn, inputMode, prune, outputs, doPack, noArcArrays) {
        @Override
        public Long getOutput(IntsRef input, int ord) {
          return (long) ord;
        }
      }.run(limit, verify, true);
    } else if (storeDocFreqs) {
      // Store only docFreq
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(false);
      new VisitTerms<Long>(dirOut, wordsFileIn, inputMode, prune, outputs, doPack, noArcArrays) {
        Random rand;
        @Override
        public Long getOutput(IntsRef input, int ord) {
          if (ord == 0) {
            rand = new Random(17);
          }
          return (long) _TestUtil.nextInt(rand, 1, 5000);
        }
      }.run(limit, verify, false);
    } else {
      // Store nothing
      final NoOutputs outputs = NoOutputs.getSingleton();
      final Object NO_OUTPUT = outputs.getNoOutput();
      new VisitTerms<Object>(dirOut, wordsFileIn, inputMode, prune, outputs, doPack, noArcArrays) {
        @Override
        public Object getOutput(IntsRef input, int ord) {
          return NO_OUTPUT;
        }
      }.run(limit, verify, false);
    }
  }

  public void testSingleString() throws Exception {
    final Outputs<Object> outputs = NoOutputs.getSingleton();
    final Builder<Object> b = new Builder<Object>(FST.INPUT_TYPE.BYTE1, outputs);
    b.add(Util.toIntsRef(new BytesRef("foobar"), new IntsRef()), outputs.getNoOutput());
    final BytesRefFSTEnum<Object> fstEnum = new BytesRefFSTEnum<Object>(b.finish());
    assertNull(fstEnum.seekFloor(new BytesRef("foo")));
    assertNull(fstEnum.seekCeil(new BytesRef("foobaz")));
  }

  /*
  public void testTrivial() throws Exception {

    // Get outputs -- passing true means FST will share
    // (delta code) the outputs.  This should result in
    // smaller FST if the outputs grow monotonically.  But
    // if numbers are "random", false should give smaller
    // final size:
    final NoOutputs outputs = NoOutputs.getSingleton();

    String[] strings = new String[] {"station", "commotion", "elation", "elastic", "plastic", "stop", "ftop", "ftation", "stat"};

    final Builder<Object> builder = new Builder<Object>(FST.INPUT_TYPE.BYTE1,
                                                        0, 0,
                                                        true,
                                                        true,
                                                        Integer.MAX_VALUE,
                                                        outputs,
                                                        null,
                                                        true);
    Arrays.sort(strings);
    final IntsRef scratch = new IntsRef();
    for(String s : strings) {
      builder.add(Util.toIntsRef(new BytesRef(s), scratch), outputs.getNoOutput());
    }
    final FST<Object> fst = builder.finish();
    System.out.println("DOT before rewrite");
    Writer w = new OutputStreamWriter(new FileOutputStream("/mnt/scratch/before.dot"));
    Util.toDot(fst, w, false, false);
    w.close();

    final FST<Object> rewrite = new FST<Object>(fst, 1, 100);

    System.out.println("DOT after rewrite");
    w = new OutputStreamWriter(new FileOutputStream("/mnt/scratch/after.dot"));
    Util.toDot(rewrite, w, false, false);
    w.close();
  }
  */

  public void testSimple() throws Exception {

    // Get outputs -- passing true means FST will share
    // (delta code) the outputs.  This should result in
    // smaller FST if the outputs grow monotonically.  But
    // if numbers are "random", false should give smaller
    // final size:
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);

    // Build an FST mapping BytesRef -> Long
    final Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, outputs);

    final BytesRef a = new BytesRef("a");
    final BytesRef b = new BytesRef("b");
    final BytesRef c = new BytesRef("c");

    builder.add(Util.toIntsRef(a, new IntsRef()), 17L);
    builder.add(Util.toIntsRef(b, new IntsRef()), 42L);
    builder.add(Util.toIntsRef(c, new IntsRef()), 13824324872317238L);

    final FST<Long> fst = builder.finish();

    assertEquals(13824324872317238L, (long) Util.get(fst, c));
    assertEquals(42, (long) Util.get(fst, b));
    assertEquals(17, (long) Util.get(fst, a));

    BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<Long>(fst);
    BytesRefFSTEnum.InputOutput<Long> seekResult;
    seekResult = fstEnum.seekFloor(a);
    assertNotNull(seekResult);
    assertEquals(17, (long) seekResult.output);

    // goes to a
    seekResult = fstEnum.seekFloor(new BytesRef("aa"));
    assertNotNull(seekResult);
    assertEquals(17, (long) seekResult.output);

    // goes to b
    seekResult = fstEnum.seekCeil(new BytesRef("aa"));
    assertNotNull(seekResult);
    assertEquals(b, seekResult.input);
    assertEquals(42, (long) seekResult.output);

    assertEquals(Util.toIntsRef(new BytesRef("c"), new IntsRef()),
                 Util.getByOutput(fst, 13824324872317238L));
    assertNull(Util.getByOutput(fst, 47));
    assertEquals(Util.toIntsRef(new BytesRef("b"), new IntsRef()),
                 Util.getByOutput(fst, 42));
    assertEquals(Util.toIntsRef(new BytesRef("a"), new IntsRef()),
                 Util.getByOutput(fst, 17));
  }

  public void testPrimaryKeys() throws Exception {
    Directory dir = newDirectory();

    for(int cycle=0;cycle<2;cycle++) {
      if (VERBOSE) {
        System.out.println("TEST: cycle=" + cycle);
      }
      RandomIndexWriter w = new RandomIndexWriter(random(), dir,
                                                  newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(IndexWriterConfig.OpenMode.CREATE));
      Document doc = new Document();
      Field idField = newField("id", "", StringField.TYPE_UNSTORED);
      doc.add(idField);
      
      final int NUM_IDS = atLeast(200);
      //final int NUM_IDS = (int) (377 * (1.0+random.nextDouble()));
      if (VERBOSE) {
        System.out.println("TEST: NUM_IDS=" + NUM_IDS);
      }
      final Set<String> allIDs = new HashSet<String>();
      for(int id=0;id<NUM_IDS;id++) {
        String idString;
        if (cycle == 0) {
          // PKs are assigned sequentially
          idString = String.format("%07d", id);
        } else {
          while(true) {
            final String s = Long.toString(random().nextLong());
            if (!allIDs.contains(s)) {
              idString = s;
              break;
            }
          }
        }
        allIDs.add(idString);
        idField.setStringValue(idString);
        w.addDocument(doc);
      }

      //w.forceMerge(1);

      // turn writer into reader:
      final IndexReader r = w.getReader();
      final IndexSearcher s = new IndexSearcher(r);
      w.close();

      final List<String> allIDsList = new ArrayList<String>(allIDs);
      final List<String> sortedAllIDsList = new ArrayList<String>(allIDsList);
      Collections.sort(sortedAllIDsList);

      // Sprinkle in some non-existent PKs:
      Set<String> outOfBounds = new HashSet<String>();
      for(int idx=0;idx<NUM_IDS/10;idx++) {
        String idString;
        if (cycle == 0) {
          idString = String.format("%07d", (NUM_IDS + idx));
        } else {
          while(true) {
            idString = Long.toString(random().nextLong());
            if (!allIDs.contains(idString)) {
              break;
            }
          }
        }
        outOfBounds.add(idString);
        allIDsList.add(idString);
      }

      // Verify w/ TermQuery
      for(int iter=0;iter<2*NUM_IDS;iter++) {
        final String id = allIDsList.get(random().nextInt(allIDsList.size()));
        final boolean exists = !outOfBounds.contains(id);
        if (VERBOSE) {
          System.out.println("TEST: TermQuery " + (exists ? "" : "non-exist ") + " id=" + id);
        }
        assertEquals((exists ? "" : "non-exist ") + "id=" + id, exists ? 1 : 0, s.search(new TermQuery(new Term("id", id)), 1).totalHits);
      }

      // Verify w/ MultiTermsEnum
      final TermsEnum termsEnum = MultiFields.getTerms(r, "id").iterator(null);
      for(int iter=0;iter<2*NUM_IDS;iter++) {
        final String id;
        final String nextID;
        final boolean exists;

        if (random().nextBoolean()) {
          id = allIDsList.get(random().nextInt(allIDsList.size()));
          exists = !outOfBounds.contains(id);
          nextID = null;
          if (VERBOSE) {
            System.out.println("TEST: exactOnly " + (exists ? "" : "non-exist ") + "id=" + id);
          }
        } else {
          // Pick ID between two IDs:
          exists = false;
          final int idv = random().nextInt(NUM_IDS-1);
          if (cycle == 0) {
            id = String.format("%07da", idv);
            nextID = String.format("%07d", idv+1);
          } else {
            id = sortedAllIDsList.get(idv) + "a";
            nextID = sortedAllIDsList.get(idv+1);
          }
          if (VERBOSE) {
            System.out.println("TEST: not exactOnly id=" + id + " nextID=" + nextID);
          }
        }

        final boolean useCache = random().nextBoolean();
        if (VERBOSE) {
          System.out.println("  useCache=" + useCache);
        }

        final TermsEnum.SeekStatus status;
        if (nextID == null) {
          if (termsEnum.seekExact(new BytesRef(id), useCache)) {
            status = TermsEnum.SeekStatus.FOUND;
          } else {
            status = TermsEnum.SeekStatus.NOT_FOUND;
          }
        } else {
          status = termsEnum.seekCeil(new BytesRef(id), useCache);
        }

        if (nextID != null) {
          assertEquals(TermsEnum.SeekStatus.NOT_FOUND, status);
          assertEquals("expected=" + nextID + " actual=" + termsEnum.term().utf8ToString(), new BytesRef(nextID), termsEnum.term());
        } else if (!exists) {
          assertTrue(status == TermsEnum.SeekStatus.NOT_FOUND ||
                     status == TermsEnum.SeekStatus.END);
        } else {
          assertEquals(TermsEnum.SeekStatus.FOUND, status);
        }
      }

      r.close();
    }
    dir.close();
  }

  public void testRandomTermLookup() throws Exception {
    Directory dir = newDirectory();

    RandomIndexWriter w = new RandomIndexWriter(random(), dir,
                                                newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(IndexWriterConfig.OpenMode.CREATE));
    Document doc = new Document();
    Field f = newField("field", "", StringField.TYPE_UNSTORED);
    doc.add(f);
      
    final int NUM_TERMS = (int) (1000*RANDOM_MULTIPLIER * (1+random().nextDouble()));
    if (VERBOSE) {
      System.out.println("TEST: NUM_TERMS=" + NUM_TERMS);
    }

    final Set<String> allTerms = new HashSet<String>();
    while(allTerms.size() < NUM_TERMS) {
      allTerms.add(simpleRandomString(random()));
    }

    for(String term : allTerms) {
      f.setStringValue(term);
      w.addDocument(doc);
    }

    // turn writer into reader:
    if (VERBOSE) {
      System.out.println("TEST: get reader");
    }
    IndexReader r = w.getReader();
    if (VERBOSE) {
      System.out.println("TEST: got reader=" + r);
    }
    IndexSearcher s = new IndexSearcher(r);
    w.close();

    final List<String> allTermsList = new ArrayList<String>(allTerms);
    Collections.shuffle(allTermsList, random());

    // verify exact lookup
    for(String term : allTermsList) {
      if (VERBOSE) {
        System.out.println("TEST: term=" + term);
      }
      assertEquals("term=" + term, 1, s.search(new TermQuery(new Term("field", term)), 1).totalHits);
    }

    r.close();
    dir.close();
  }


  /**
   * Test state expansion (array format) on close-to-root states. Creates
   * synthetic input that has one expanded state on each level.
   * 
   * @see "https://issues.apache.org/jira/browse/LUCENE-2933" 
   */
  public void testExpandedCloseToRoot() throws Exception {
    class SyntheticData {
      FST<Object> compile(String[] lines) throws IOException {
        final NoOutputs outputs = NoOutputs.getSingleton();
        final Object nothing = outputs.getNoOutput();
        final Builder<Object> b = new Builder<Object>(FST.INPUT_TYPE.BYTE1, outputs);

        int line = 0;
        final BytesRef term = new BytesRef();
        final IntsRef scratchIntsRef = new IntsRef();
        while (line < lines.length) {
          String w = lines[line++];
          if (w == null) {
            break;
          }
          term.copyChars(w);
          b.add(Util.toIntsRef(term, scratchIntsRef), nothing);
        }
        
        return b.finish();
      }
      
      void generate(ArrayList<String> out, StringBuilder b, char from, char to,
          int depth) {
        if (depth == 0 || from == to) {
          String seq = b.toString() + "_" + out.size() + "_end";
          out.add(seq);
        } else {
          for (char c = from; c <= to; c++) {
            b.append(c);
            generate(out, b, from, c == to ? to : from, depth - 1);
            b.deleteCharAt(b.length() - 1);
          }
        }
      }

      public int verifyStateAndBelow(FST<Object> fst, Arc<Object> arc, int depth) 
        throws IOException {
        if (FST.targetHasArcs(arc)) {
          int childCount = 0;
          for (arc = fst.readFirstTargetArc(arc, arc);; 
               arc = fst.readNextArc(arc), childCount++)
          {
            boolean expanded = fst.isExpandedTarget(arc);
            int children = verifyStateAndBelow(fst, new FST.Arc<Object>().copyFrom(arc), depth + 1);

            assertEquals(
                expanded,
                (depth <= FST.FIXED_ARRAY_SHALLOW_DISTANCE && 
                    children >= FST.FIXED_ARRAY_NUM_ARCS_SHALLOW) ||
                 children >= FST.FIXED_ARRAY_NUM_ARCS_DEEP);
            if (arc.isLast()) break;
          }

          return childCount;
        }
        return 0;
      }
    }

    // Sanity check.
    assertTrue(FST.FIXED_ARRAY_NUM_ARCS_SHALLOW < FST.FIXED_ARRAY_NUM_ARCS_DEEP);
    assertTrue(FST.FIXED_ARRAY_SHALLOW_DISTANCE >= 0);

    SyntheticData s = new SyntheticData();

    ArrayList<String> out = new ArrayList<String>();
    StringBuilder b = new StringBuilder();
    s.generate(out, b, 'a', 'i', 10);
    String[] input = out.toArray(new String[out.size()]);
    Arrays.sort(input);
    FST<Object> fst = s.compile(input);
    FST.Arc<Object> arc = fst.getFirstArc(new FST.Arc<Object>());
    s.verifyStateAndBelow(fst, arc, 1);
  }

  public void testFinalOutputOnEndState() throws Exception {
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);

    final Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE4, 2, 0, true, true, Integer.MAX_VALUE, outputs, null, random().nextBoolean());
    builder.add(Util.toUTF32("stat", new IntsRef()), 17L);
    builder.add(Util.toUTF32("station", new IntsRef()), 10L);
    final FST<Long> fst = builder.finish();
    //Writer w = new OutputStreamWriter(new FileOutputStream("/x/tmp3/out.dot"));
    StringWriter w = new StringWriter();
    Util.toDot(fst, w, false, false);
    w.close();
    //System.out.println(w.toString());
    assertTrue(w.toString().indexOf("label=\"t/[7]\"") != -1);
  }

  public void testInternalFinalState() throws Exception {
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
    final boolean willRewrite = random().nextBoolean();
    final Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, outputs, null, willRewrite);
    builder.add(Util.toIntsRef(new BytesRef("stat"), new IntsRef()), outputs.getNoOutput());
    builder.add(Util.toIntsRef(new BytesRef("station"), new IntsRef()), outputs.getNoOutput());
    final FST<Long> fst = builder.finish();
    StringWriter w = new StringWriter();
    //Writer w = new OutputStreamWriter(new FileOutputStream("/x/tmp/out.dot"));
    Util.toDot(fst, w, false, false);
    w.close();
    //System.out.println(w.toString());
    final String expected;
    if (willRewrite) {
      expected = "4 -> 3 [label=\"t\" style=\"bold\"";
    } else {
      expected = "8 -> 6 [label=\"t\" style=\"bold\"";
    }
    assertTrue(w.toString().indexOf(expected) != -1);
  }

  // Make sure raw FST can differentiate between final vs
  // non-final end nodes
  public void testNonFinalStopNode() throws Exception {
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
    final Long nothing = outputs.getNoOutput();
    final Builder<Long> b = new Builder<Long>(FST.INPUT_TYPE.BYTE1, outputs);

    final FST<Long> fst = new FST<Long>(FST.INPUT_TYPE.BYTE1, outputs, false);

    final Builder.UnCompiledNode<Long> rootNode = new Builder.UnCompiledNode<Long>(b, 0);

    // Add final stop node
    {
      final Builder.UnCompiledNode<Long> node = new Builder.UnCompiledNode<Long>(b, 0);
      node.isFinal = true;
      rootNode.addArc('a', node);
      final Builder.CompiledNode frozen = new Builder.CompiledNode();
      frozen.node = fst.addNode(node);
      rootNode.arcs[0].nextFinalOutput = 17L;
      rootNode.arcs[0].isFinal = true;
      rootNode.arcs[0].output = nothing;
      rootNode.arcs[0].target = frozen;
    }

    // Add non-final stop node
    {
      final Builder.UnCompiledNode<Long> node = new Builder.UnCompiledNode<Long>(b, 0);
      rootNode.addArc('b', node);
      final Builder.CompiledNode frozen = new Builder.CompiledNode();
      frozen.node = fst.addNode(node);
      rootNode.arcs[1].nextFinalOutput = nothing;
      rootNode.arcs[1].output = 42L;
      rootNode.arcs[1].target = frozen;
    }

    fst.finish(fst.addNode(rootNode));

    StringWriter w = new StringWriter();
    //Writer w = new OutputStreamWriter(new FileOutputStream("/x/tmp3/out.dot"));
    Util.toDot(fst, w, false, false);
    w.close();
    
    checkStopNodes(fst, outputs);

    // Make sure it still works after save/load:
    Directory dir = newDirectory();
    IndexOutput out = dir.createOutput("fst", IOContext.DEFAULT);
    fst.save(out);
    out.close();

    IndexInput in = dir.openInput("fst", IOContext.DEFAULT);
    final FST<Long> fst2 = new FST<Long>(in, outputs);
    checkStopNodes(fst2, outputs);
    in.close();
    dir.close();
  }

  private void checkStopNodes(FST<Long> fst, PositiveIntOutputs outputs) throws Exception {
    final Long nothing = outputs.getNoOutput();
    FST.Arc<Long> startArc = fst.getFirstArc(new FST.Arc<Long>());
    assertEquals(nothing, startArc.output);
    assertEquals(nothing, startArc.nextFinalOutput);

    FST.Arc<Long> arc = fst.readFirstTargetArc(startArc, new FST.Arc<Long>());
    assertEquals('a', arc.label);
    assertEquals(17, arc.nextFinalOutput.longValue());
    assertTrue(arc.isFinal());

    arc = fst.readNextArc(arc);
    assertEquals('b', arc.label);
    assertFalse(arc.isFinal());
    assertEquals(42, arc.output.longValue());
  }
  
  static final Comparator<Long> minLongComparator = new Comparator<Long> () {
    public int compare(Long left, Long right) {
      return left.compareTo(right);
    }  
  };

  public void testShortestPaths() throws Exception {
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
    final Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, outputs);

    final IntsRef scratch = new IntsRef();
    builder.add(Util.toIntsRef(new BytesRef("aab"), scratch), 22L);
    builder.add(Util.toIntsRef(new BytesRef("aac"), scratch), 7L);
    builder.add(Util.toIntsRef(new BytesRef("ax"), scratch), 17L);
    final FST<Long> fst = builder.finish();
    //Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"));
    //Util.toDot(fst, w, false, false);
    //w.close();

    Util.MinResult<Long>[] r = Util.shortestPaths(fst,
                                           fst.getFirstArc(new FST.Arc<Long>()),
                                           minLongComparator,
                                           3);
    assertEquals(3, r.length);

    assertEquals(Util.toIntsRef(new BytesRef("aac"), scratch), r[0].input);
    assertEquals(7L, r[0].output.longValue());

    assertEquals(Util.toIntsRef(new BytesRef("ax"), scratch), r[1].input);
    assertEquals(17L, r[1].output.longValue());

    assertEquals(Util.toIntsRef(new BytesRef("aab"), scratch), r[2].input);
    assertEquals(22L, r[2].output.longValue());
  }
  
  // compares just the weight side of the pair
  static final Comparator<Pair<Long,Long>> minPairWeightComparator = new Comparator<Pair<Long,Long>> () {
    public int compare(Pair<Long,Long> left, Pair<Long,Long> right) {
      return left.output1.compareTo(right.output1);
    }  
  };
  
  /** like testShortestPaths, but uses pairoutputs so we have both a weight and an output */
  public void testShortestPathsWFST() throws Exception {

    PairOutputs<Long,Long> outputs = new PairOutputs<Long,Long>(
        PositiveIntOutputs.getSingleton(true), // weight
        PositiveIntOutputs.getSingleton(true)  // output
    );
    
    final Builder<Pair<Long,Long>> builder = new Builder<Pair<Long,Long>>(FST.INPUT_TYPE.BYTE1, outputs);

    final IntsRef scratch = new IntsRef();
    builder.add(Util.toIntsRef(new BytesRef("aab"), scratch), outputs.newPair(22L, 57L));
    builder.add(Util.toIntsRef(new BytesRef("aac"), scratch), outputs.newPair(7L, 36L));
    builder.add(Util.toIntsRef(new BytesRef("ax"), scratch), outputs.newPair(17L, 85L));
    final FST<Pair<Long,Long>> fst = builder.finish();
    //Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"));
    //Util.toDot(fst, w, false, false);
    //w.close();

    Util.MinResult<Pair<Long,Long>>[] r = Util.shortestPaths(fst,
                                           fst.getFirstArc(new FST.Arc<Pair<Long,Long>>()),
                                           minPairWeightComparator,
                                           3);
    assertEquals(3, r.length);

    assertEquals(Util.toIntsRef(new BytesRef("aac"), scratch), r[0].input);
    assertEquals(7L, r[0].output.output1.longValue()); // weight
    assertEquals(36L, r[0].output.output2.longValue()); // output

    assertEquals(Util.toIntsRef(new BytesRef("ax"), scratch), r[1].input);
    assertEquals(17L, r[1].output.output1.longValue()); // weight
    assertEquals(85L, r[1].output.output2.longValue()); // output

    assertEquals(Util.toIntsRef(new BytesRef("aab"), scratch), r[2].input);
    assertEquals(22L, r[2].output.output1.longValue()); // weight
    assertEquals(57L, r[2].output.output2.longValue()); // output
  }
  
  public void testShortestPathsRandom() throws Exception {
    final Random random = random();
    int numWords = atLeast(1000);
    
    final TreeMap<String,Long> slowCompletor = new TreeMap<String,Long>();
    final TreeSet<String> allPrefixes = new TreeSet<String>();
    
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
    final Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, outputs);
    final IntsRef scratch = new IntsRef();
    
    for (int i = 0; i < numWords; i++) {
      String s;
      while (true) {
        s = _TestUtil.randomSimpleString(random);
        if (!slowCompletor.containsKey(s)) {
          break;
        }
      }
      
      for (int j = 1; j < s.length(); j++) {
        allPrefixes.add(s.substring(0, j));
      }
      int weight = _TestUtil.nextInt(random, 1, 100); // weights 1..100
      slowCompletor.put(s, (long)weight);
    }
    
    for (Map.Entry<String,Long> e : slowCompletor.entrySet()) {
      //System.out.println("add: " + e);
      builder.add(Util.toIntsRef(new BytesRef(e.getKey()), scratch), e.getValue());
    }
    
    final FST<Long> fst = builder.finish();
    //System.out.println("SAVE out.dot");
    //Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"));
    //Util.toDot(fst, w, false, false);
    //w.close();
    
    BytesReader reader = fst.getBytesReader(0);
    
    //System.out.println("testing: " + allPrefixes.size() + " prefixes");
    for (String prefix : allPrefixes) {
      // 1. run prefix against fst, then complete by value
      //System.out.println("TEST: " + prefix);
    
      long prefixOutput = 0;
      FST.Arc<Long> arc = fst.getFirstArc(new FST.Arc<Long>());
      for(int idx=0;idx<prefix.length();idx++) {
        if (fst.findTargetArc((int) prefix.charAt(idx), arc, arc, reader) == null) {
          fail();
        }
        prefixOutput += arc.output;
      }

      final int topN = _TestUtil.nextInt(random, 1, 10);

      Util.MinResult<Long>[] r = Util.shortestPaths(fst, arc, minLongComparator, topN);

      // 2. go thru whole treemap (slowCompletor) and check its actually the best suggestion
      final List<Util.MinResult<Long>> matches = new ArrayList<Util.MinResult<Long>>();

      // TODO: could be faster... but its slowCompletor for a reason
      for (Map.Entry<String,Long> e : slowCompletor.entrySet()) {
        if (e.getKey().startsWith(prefix)) {
          //System.out.println("  consider " + e.getKey());
          matches.add(new Util.MinResult<Long>(Util.toIntsRef(new BytesRef(e.getKey().substring(prefix.length())), new IntsRef()),
                                         e.getValue() - prefixOutput, minLongComparator));
        }
      }

      assertTrue(matches.size() > 0);
      Collections.sort(matches);
      if (matches.size() > topN) {
        matches.subList(topN, matches.size()).clear();
      }

      assertEquals(matches.size(), r.length);

      for(int hit=0;hit<r.length;hit++) {
        //System.out.println("  check hit " + hit);
        assertEquals(matches.get(hit).input, r[hit].input);
        assertEquals(matches.get(hit).output, r[hit].output);
      }
    }
  }
  
  // used by slowcompletor
  class TwoLongs {
    long a;
    long b;

    TwoLongs(long a, long b) {
      this.a = a;
      this.b = b;
    }
  }
  
  /** like testShortestPathsRandom, but uses pairoutputs so we have both a weight and an output */
  public void testShortestPathsWFSTRandom() throws Exception {
    int numWords = atLeast(1000);
    
    final TreeMap<String,TwoLongs> slowCompletor = new TreeMap<String,TwoLongs>();
    final TreeSet<String> allPrefixes = new TreeSet<String>();
    
    PairOutputs<Long,Long> outputs = new PairOutputs<Long,Long>(
        PositiveIntOutputs.getSingleton(true), // weight
        PositiveIntOutputs.getSingleton(true)  // output
    );
    final Builder<Pair<Long,Long>> builder = new Builder<Pair<Long,Long>>(FST.INPUT_TYPE.BYTE1, outputs);
    final IntsRef scratch = new IntsRef();
    
    Random random = random();
    for (int i = 0; i < numWords; i++) {
      String s;
      while (true) {
        s = _TestUtil.randomSimpleString(random);
        if (!slowCompletor.containsKey(s)) {
          break;
        }
      }
      
      for (int j = 1; j < s.length(); j++) {
        allPrefixes.add(s.substring(0, j));
      }
      int weight = _TestUtil.nextInt(random, 1, 100); // weights 1..100
      int output = _TestUtil.nextInt(random, 0, 500); // outputs 0..500 
      slowCompletor.put(s, new TwoLongs(weight, output));
    }
    
    for (Map.Entry<String,TwoLongs> e : slowCompletor.entrySet()) {
      //System.out.println("add: " + e);
      long weight = e.getValue().a;
      long output = e.getValue().b;
      builder.add(Util.toIntsRef(new BytesRef(e.getKey()), scratch), outputs.newPair(weight, output));
    }
    
    final FST<Pair<Long,Long>> fst = builder.finish();
    //System.out.println("SAVE out.dot");
    //Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"));
    //Util.toDot(fst, w, false, false);
    //w.close();
    
    BytesReader reader = fst.getBytesReader(0);
    
    //System.out.println("testing: " + allPrefixes.size() + " prefixes");
    for (String prefix : allPrefixes) {
      // 1. run prefix against fst, then complete by value
      //System.out.println("TEST: " + prefix);
    
      Pair<Long,Long> prefixOutput = outputs.getNoOutput();
      FST.Arc<Pair<Long,Long>> arc = fst.getFirstArc(new FST.Arc<Pair<Long,Long>>());
      for(int idx=0;idx<prefix.length();idx++) {
        if (fst.findTargetArc((int) prefix.charAt(idx), arc, arc, reader) == null) {
          fail();
        }
        prefixOutput = outputs.add(prefixOutput, arc.output);
      }

      final int topN = _TestUtil.nextInt(random, 1, 10);

      Util.MinResult<Pair<Long,Long>>[] r = Util.shortestPaths(fst, arc, minPairWeightComparator, topN);

      // 2. go thru whole treemap (slowCompletor) and check its actually the best suggestion
      final List<Util.MinResult<Pair<Long,Long>>> matches = new ArrayList<Util.MinResult<Pair<Long,Long>>>();

      // TODO: could be faster... but its slowCompletor for a reason
      for (Map.Entry<String,TwoLongs> e : slowCompletor.entrySet()) {
        if (e.getKey().startsWith(prefix)) {
          //System.out.println("  consider " + e.getKey());
          matches.add(new Util.MinResult<Pair<Long,Long>>(Util.toIntsRef(new BytesRef(e.getKey().substring(prefix.length())), new IntsRef()),
                                         outputs.newPair(e.getValue().a - prefixOutput.output1, e.getValue().b - prefixOutput.output2), 
                                         minPairWeightComparator));
        }
      }

      assertTrue(matches.size() > 0);
      Collections.sort(matches);
      if (matches.size() > topN) {
        matches.subList(topN, matches.size()).clear();
      }

      assertEquals(matches.size(), r.length);

      for(int hit=0;hit<r.length;hit++) {
        //System.out.println("  check hit " + hit);
        assertEquals(matches.get(hit).input, r[hit].input);
        assertEquals(matches.get(hit).output, r[hit].output);
      }
    }
  }

  public void testLargeOutputsOnArrayArcs() throws Exception {
    final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
    final Builder<BytesRef> builder = new Builder<BytesRef>(FST.INPUT_TYPE.BYTE1, outputs);

    final byte[] bytes = new byte[300];
    final IntsRef input = new IntsRef();
    input.grow(1);
    input.length = 1;
    final BytesRef output = new BytesRef(bytes);
    for(int arc=0;arc<6;arc++) {
      input.ints[0] = arc;
      output.bytes[0] = (byte) arc;
      builder.add(input, BytesRef.deepCopyOf(output));
    }

    final FST<BytesRef> fst = builder.finish();
    for(int arc=0;arc<6;arc++) {
      input.ints[0] = arc;
      final BytesRef result = Util.get(fst, input);
      assertNotNull(result);
      assertEquals(300, result.length);
      assertEquals(result.bytes[result.offset], arc);
      for(int byteIDX=1;byteIDX<result.length;byteIDX++) {
        assertEquals(0, result.bytes[result.offset+byteIDX]);
      }
    }
  }
}
