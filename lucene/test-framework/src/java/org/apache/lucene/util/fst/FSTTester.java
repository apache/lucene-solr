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
package org.apache.lucene.util.fst;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.UnicodeUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Helper class to test FSTs. */
public class FSTTester<T> {

  final Random random;
  final List<InputOutput<T>> pairs;
  final int inputMode;
  final Outputs<T> outputs;
  final Directory dir;
  final boolean doReverseLookup;
  long nodeCount;
  long arcCount;

  public FSTTester(Random random, Directory dir, int inputMode, List<InputOutput<T>> pairs, Outputs<T> outputs, boolean doReverseLookup) {
    this.random = random;
    this.dir = dir;
    this.inputMode = inputMode;
    this.pairs = pairs;
    this.outputs = outputs;
    this.doReverseLookup = doReverseLookup;
  }

  static String inputToString(int inputMode, IntsRef term) {
    return inputToString(inputMode, term, true);
  }

  static String inputToString(int inputMode, IntsRef term, boolean isValidUnicode) {
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

  static String getRandomString(Random random) {
    final String term;
    if (random.nextBoolean()) {
      term = TestUtil.randomRealisticUnicodeString(random);
    } else {
      // we want to mix in limited-alphabet symbols so
      // we get more sharing of the nodes given how few
      // terms we are testing...
      term = simpleRandomString(random);
    }
    return term;
  }

  static String simpleRandomString(Random r) {
    final int end = r.nextInt(10);
    if (end == 0) {
      // allow 0 length
      return "";
    }
    final char[] buffer = new char[end];
    for (int i = 0; i < end; i++) {
      buffer[i] = (char) TestUtil.nextInt(r, 97, 102);
    }
    return new String(buffer, 0, end);
  }

  static IntsRef toIntsRef(String s, int inputMode) {
    return toIntsRef(s, inputMode, new IntsRefBuilder());
  }

  static IntsRef toIntsRef(String s, int inputMode, IntsRefBuilder ir) {
    if (inputMode == 0) {
      // utf8
      return toIntsRef(new BytesRef(s), ir);
    } else {
      // utf32
      return toIntsRefUTF32(s, ir);
    }
  }

  static IntsRef toIntsRefUTF32(String s, IntsRefBuilder ir) {
    final int charLength = s.length();
    int charIdx = 0;
    int intIdx = 0;
    ir.clear();
    while(charIdx < charLength) {
      ir.grow(intIdx+1);
      final int utf32 = s.codePointAt(charIdx);
      ir.append(utf32);
      charIdx += Character.charCount(utf32);
      intIdx++;
    }
    return ir.get();
  }

  static IntsRef toIntsRef(BytesRef br, IntsRefBuilder ir) {
    ir.grow(br.length);
    ir.clear();
    for(int i=0;i<br.length;i++) {
      ir.append(br.bytes[br.offset+i]&0xFF);
    }
    return ir.get();
  }

  /** Holds one input/output pair. */
  public static class InputOutput<T> implements Comparable<InputOutput<T>> {
    public final IntsRef input;
    public final T output;

    public InputOutput(IntsRef input, T output) {
      this.input = input;
      this.output = output;
    }

    @Override
    public int compareTo(InputOutput<T> other) {
      if (other instanceof InputOutput) {
        return input.compareTo((other).input);
      } else {
        throw new IllegalArgumentException();
      }
    }
  }

  public void doTest(boolean testPruning) throws IOException {
    // no pruning
    doTest(0, 0, true);

    if (testPruning) {
      // simple pruning
      doTest(TestUtil.nextInt(random, 1, 1 + pairs.size()), 0, true);
        
      // leafy pruning
      doTest(0, TestUtil.nextInt(random, 1, 1 + pairs.size()), true);
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
    final FST.BytesReader fstReader = fst.getBytesReader();

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
      output = fst.outputs.add(output, arc.output());
    }

    if (prefixLength != null) {
      prefixLength[0] = term.length;
    }

    return output;
  }

  private T randomAcceptedWord(FST<T> fst, IntsRefBuilder in) throws IOException {
    FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<T>());

    final List<FST.Arc<T>> arcs = new ArrayList<>();
    in.clear();
    final T NO_OUTPUT = fst.outputs.getNoOutput();
    T output = NO_OUTPUT;
    final FST.BytesReader fstReader = fst.getBytesReader();

    while(true) {
      // read all arcs:
      fst.readFirstTargetArc(arc, arc, fstReader);
      arcs.add(new FST.Arc<T>().copyFrom(arc));
      while(!arc.isLast()) {
        fst.readNextArc(arc, fstReader);
        arcs.add(new FST.Arc<T>().copyFrom(arc));
      }
      
      // pick one
      arc = arcs.get(random.nextInt(arcs.size()));
      arcs.clear();

      // accumulate output
      output = fst.outputs.add(output, arc.output());

      // append label
      if (arc.label() == FST.END_LABEL) {
        break;
      }

      in.append(arc.label());
    }

    return output;
  }


  FST<T> doTest(int prune1, int prune2, boolean allowRandomSuffixSharing) throws IOException {
    if (LuceneTestCase.VERBOSE) {
      System.out.println("\nTEST: prune1=" + prune1 + " prune2=" + prune2);
    }

    final Builder<T> builder = new Builder<>(inputMode == 0 ? FST.INPUT_TYPE.BYTE1 : FST.INPUT_TYPE.BYTE4,
                                              prune1, prune2,
                                              prune1==0 && prune2==0,
                                              allowRandomSuffixSharing ? random.nextBoolean() : true,
                                              allowRandomSuffixSharing ? TestUtil.nextInt(random, 1, 10) : Integer.MAX_VALUE,
                                              outputs,
                                              true,
                                              15);

    for(InputOutput<T> pair : pairs) {
      if (pair.output instanceof List) {
        @SuppressWarnings("unchecked") List<Long> longValues = (List<Long>) pair.output;
        @SuppressWarnings("unchecked") final Builder<Object> builderObject = (Builder<Object>) builder;
        for(Long value : longValues) {
          builderObject.add(pair.input, value);
        }
      } else {
        builder.add(pair.input, pair.output);
      }
    }
    FST<T> fst = builder.finish();

    if (random.nextBoolean() && fst != null) {
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

    if (LuceneTestCase.VERBOSE && pairs.size() <= 20 && fst != null) {
      System.out.println("Printing FST as dot file to stdout:");
      final Writer w = new OutputStreamWriter(System.out, Charset.defaultCharset());
      Util.toDot(fst, w, false, false);
      w.flush();
      System.out.println("END dot file");
    }

    if (LuceneTestCase.VERBOSE) {
      if (fst == null) {
        System.out.println("  fst has 0 nodes (fully pruned)");
      } else {
        System.out.println("  fst has " + builder.getNodeCount() + " nodes and " + builder.getArcCount() + " arcs");
      }
    }

    if (prune1 == 0 && prune2 == 0) {
      verifyUnPruned(inputMode, fst);
    } else {
      verifyPruned(inputMode, fst, prune1, prune2);
    }

    nodeCount = builder.getNodeCount();
    arcCount = builder.getArcCount();

    return fst;
  }

  protected boolean outputsEqual(T a, T b) {
    return a.equals(b);
  }

  // FST is complete
  @SuppressWarnings("deprecation")
  private void verifyUnPruned(int inputMode, FST<T> fst) throws IOException {

    final FST<Long> fstLong;
    final Set<Long> validOutputs;
    long minLong = Long.MAX_VALUE;
    long maxLong = Long.MIN_VALUE;

    if (doReverseLookup) {
      @SuppressWarnings("unchecked") FST<Long> fstLong0 = (FST<Long>) fst;
      fstLong = fstLong0;
      validOutputs = new HashSet<>();
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

    if (LuceneTestCase.VERBOSE) {
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
    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: check valid terms/next()");
    }
    {
      IntsRefFSTEnum<T> fstEnum = new IntsRefFSTEnum<>(fst);
      for(InputOutput<T> pair : pairs) {
        IntsRef term = pair.input;
        if (LuceneTestCase.VERBOSE) {
          System.out.println("TEST: check term=" + inputToString(inputMode, term) + " output=" + fst.outputs.outputToString(pair.output));
        }
        T output = run(fst, term, null);
        assertNotNull("term " + inputToString(inputMode, term) + " is not accepted", output);
        assertTrue(outputsEqual(pair.output, output));

        // verify enum's next
        IntsRefFSTEnum.InputOutput<T> t = fstEnum.next();
        assertNotNull(t);
        assertEquals("expected input=" + inputToString(inputMode, term) + " but fstEnum returned " + inputToString(inputMode, t.input), term, t.input);
        assertTrue(outputsEqual(pair.output, t.output));
      }
      assertNull(fstEnum.next());
    }

    final Map<IntsRef,T> termsMap = new HashMap<>();
    for(InputOutput<T> pair : pairs) {
      termsMap.put(pair.input, pair.output);
    }

    if (doReverseLookup && maxLong > minLong) {
      // Do random lookups so we test null (output doesn't
      // exist) case:
      assertNull(Util.getByOutput(fstLong, minLong-7));
      assertNull(Util.getByOutput(fstLong, maxLong+7));

      final int num = LuceneTestCase.atLeast(random, 100);
      for(int iter=0;iter<num;iter++) {
        Long v = TestUtil.nextLong(random, minLong, maxLong);
        IntsRef input = Util.getByOutput(fstLong, v);
        assertTrue(validOutputs.contains(v) || input == null);
      }
    }

    // find random matching word and make sure it's valid
    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: verify random accepted terms");
    }
    final IntsRefBuilder scratch = new IntsRefBuilder();
    int num = LuceneTestCase.atLeast(random, 500);
    for(int iter=0;iter<num;iter++) {
      T output = randomAcceptedWord(fst, scratch);
      assertTrue("accepted word " + inputToString(inputMode, scratch.get()) + " is not valid", termsMap.containsKey(scratch.get()));
      assertTrue(outputsEqual(termsMap.get(scratch.get()), output));

      if (doReverseLookup) {
        //System.out.println("lookup output=" + output + " outs=" + fst.outputs);
        IntsRef input = Util.getByOutput(fstLong, (Long) output);
        assertNotNull(input);
        //System.out.println("  got " + Util.toBytesRef(input, new BytesRef()).utf8ToString());
        assertEquals(scratch.get(), input);
      }
    }
    
    // test IntsRefFSTEnum.seek:
    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: verify seek");
    }
    IntsRefFSTEnum<T> fstEnum = new IntsRefFSTEnum<>(fst);
    num = LuceneTestCase.atLeast(random, 100);
    for(int iter=0;iter<num;iter++) {
      if (LuceneTestCase.VERBOSE) {
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
              if (LuceneTestCase.VERBOSE) {
                System.out.println("  do non-exist seekExact term=" + inputToString(inputMode, term));
              }
              seekResult = fstEnum.seekExact(term);
              pos = -1;
            } else if (random.nextBoolean()) {
              if (LuceneTestCase.VERBOSE) {
                System.out.println("  do non-exist seekFloor term=" + inputToString(inputMode, term));
              }
              seekResult = fstEnum.seekFloor(term);
              pos--;
            } else {
              if (LuceneTestCase.VERBOSE) {
                System.out.println("  do non-exist seekCeil term=" + inputToString(inputMode, term));
              }
              seekResult = fstEnum.seekCeil(term);
            }

            if (pos != -1 && pos < pairs.size()) {
              //System.out.println("    got " + inputToString(inputMode,seekResult.input) + " output=" + fst.outputs.outputToString(seekResult.output));
              assertNotNull("got null but expected term=" + inputToString(inputMode, pairs.get(pos).input), seekResult);
              if (LuceneTestCase.VERBOSE) {
                System.out.println("    got " + inputToString(inputMode, seekResult.input));
              }
              assertEquals("expected " + inputToString(inputMode, pairs.get(pos).input) + " but got " + inputToString(inputMode, seekResult.input), pairs.get(pos).input, seekResult.input);
              assertTrue(outputsEqual(pairs.get(pos).output, seekResult.output));
            } else {
              // seeked before start or beyond end
              //System.out.println("seek=" + seekTerm);
              assertNull("expected null but got " + (seekResult==null ? "null" : inputToString(inputMode, seekResult.input)), seekResult);
              if (LuceneTestCase.VERBOSE) {
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
          if (LuceneTestCase.VERBOSE) {
            System.out.println("  do exists seekExact term=" + inputToString(inputMode, pair.input));
          }
          seekResult = fstEnum.seekExact(pair.input);
        } else if (random.nextBoolean()) {
          if (LuceneTestCase.VERBOSE) {
            System.out.println("  do exists seekFloor " + inputToString(inputMode, pair.input));
          }
          seekResult = fstEnum.seekFloor(pair.input);
        } else {
          if (LuceneTestCase.VERBOSE) {
            System.out.println("  do exists seekCeil " + inputToString(inputMode, pair.input));
          }
          seekResult = fstEnum.seekCeil(pair.input);
        }
        assertNotNull(seekResult);
        assertEquals("got " + inputToString(inputMode, seekResult.input) + " but expected " + inputToString(inputMode, pair.input), pair.input, seekResult.input);
        assertTrue(outputsEqual(pair.output, seekResult.output));
      }
    }

    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: mixed next/seek");
    }

    // test mixed next/seek
    num = LuceneTestCase.atLeast(random, 100);
    for(int iter=0;iter<num;iter++) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("TEST: iter " + iter);
      }
      // reset:
      fstEnum = new IntsRefFSTEnum<>(fst);
      int upto = -1;
      while(true) {
        boolean isDone = false;
        if (upto == pairs.size()-1 || random.nextBoolean()) {
          // next
          upto++;
          if (LuceneTestCase.VERBOSE) {
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
                if (LuceneTestCase.VERBOSE) {
                  System.out.println("  do non-exist seekFloor(" + inputToString(inputMode, term) + ")");
                }
                isDone = fstEnum.seekFloor(term) == null;
              } else {
                if (LuceneTestCase.VERBOSE) {
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
            if (LuceneTestCase.VERBOSE) {
              System.out.println("  do seekCeil(" + inputToString(inputMode, pairs.get(upto).input) + ")");
            }
            isDone = fstEnum.seekCeil(pairs.get(upto).input) == null;
          } else {
            if (LuceneTestCase.VERBOSE) {
              System.out.println("  do seekFloor(" + inputToString(inputMode, pairs.get(upto).input) + ")");
            }
            isDone = fstEnum.seekFloor(pairs.get(upto).input) == null;
          }
        }
        if (LuceneTestCase.VERBOSE) {
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
          assertTrue(outputsEqual(pairs.get(upto).output, fstEnum.current().output));

          /*
            if (upto < pairs.size()-1) {
            int tryCount = 0;
            while(tryCount < 10) {
            final IntsRef t = toIntsRef(getRandomString(), inputMode);
            if (pairs.get(upto).input.compareTo(t) < 0) {
            final boolean expected = t.compareTo(pairs.get(upto+1).input) < 0;
            if (LuceneTestCase.VERBOSE) {
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

    if (LuceneTestCase.VERBOSE) {
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
    final Map<IntsRef,CountMinOutput<T>> prefixes = new HashMap<>();
    final IntsRefBuilder scratch = new IntsRefBuilder();
    for(InputOutput<T> pair: pairs) {
      scratch.copyInts(pair.input);
      for(int idx=0;idx<=pair.input.length;idx++) {
        scratch.setLength(idx);
        CountMinOutput<T> cmo = prefixes.get(scratch.get());
        if (cmo == null) {
          cmo = new CountMinOutput<>();
          cmo.count = 1;
          cmo.output = pair.output;
          prefixes.put(scratch.toIntsRef(), cmo);
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

    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: now prune");
    }

    // prune 'em
    final Iterator<Map.Entry<IntsRef,CountMinOutput<T>>> it = prefixes.entrySet().iterator();
    while(it.hasNext()) {
      Map.Entry<IntsRef,CountMinOutput<T>> ent = it.next();
      final IntsRef prefix = ent.getKey();
      final CountMinOutput<T> cmo = ent.getValue();
      if (LuceneTestCase.VERBOSE) {
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
          scratch.setLength(prefix.length-1);
          System.arraycopy(prefix.ints, prefix.offset, scratch.ints(), 0, scratch.length());
          final CountMinOutput<T> cmo2 = prefixes.get(scratch.get());
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
        scratch.setLength(scratch.length() - 1);
        while(scratch.length() >= 0) {
          final CountMinOutput<T> cmo2 = prefixes.get(scratch.get());
          if (cmo2 != null) {
            //System.out.println("    clear isLeaf " + inputToString(inputMode, scratch));
            cmo2.isLeaf = false;
          }
          scratch.setLength(scratch.length() - 1);
        }
      }
    }

    if (LuceneTestCase.VERBOSE) {
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
    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: check pruned enum");
    }
    IntsRefFSTEnum<T> fstEnum = new IntsRefFSTEnum<>(fst);
    IntsRefFSTEnum.InputOutput<T> current;
    while((current = fstEnum.next()) != null) {
      if (LuceneTestCase.VERBOSE) {
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
    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: verify all prefixes");
    }
    final int[] stopNode = new int[1];
    for(Map.Entry<IntsRef,CountMinOutput<T>> ent : prefixes.entrySet()) {
      if (ent.getKey().length > 0) {
        final CountMinOutput<T> cmo = ent.getValue();
        final T output = run(fst, ent.getKey(), stopNode);
        if (LuceneTestCase.VERBOSE) {
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

