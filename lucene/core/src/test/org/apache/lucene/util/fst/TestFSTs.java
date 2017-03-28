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


import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
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
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.fst.BytesRefFSTEnum.InputOutput;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.PairOutputs.Pair;
import org.apache.lucene.util.fst.Util.Result;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.util.fst.FSTTester.getRandomString;
import static org.apache.lucene.util.fst.FSTTester.simpleRandomString;
import static org.apache.lucene.util.fst.FSTTester.toIntsRef;

@SuppressCodecs({ "SimpleText", "Memory", "Direct" })
@Slow
public class TestFSTs extends LuceneTestCase {

  private MockDirectoryWrapper dir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newMockDirectory();
  }

  @Override
  public void tearDown() throws Exception {
    // can be null if we force simpletext (funky, some kind of bug in test runner maybe)
    if (dir != null) dir.close();
    super.tearDown();
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
        final List<FSTTester.InputOutput<Object>> pairs = new ArrayList<>(terms2.length);
        for(IntsRef term : terms2) {
          pairs.add(new FSTTester.InputOutput<>(term, NO_OUTPUT));
        }
        FSTTester<Object> tester = new FSTTester<>(random(), dir, inputMode, pairs, outputs, false);
        FST<Object> fst = tester.doTest(0, 0, false);
        assertNotNull(fst);
        assertEquals(22, tester.nodeCount);
        assertEquals(27, tester.arcCount);
      }

      // FST ord pos int
      {
        final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
        final List<FSTTester.InputOutput<Long>> pairs = new ArrayList<>(terms2.length);
        for(int idx=0;idx<terms2.length;idx++) {
          pairs.add(new FSTTester.InputOutput<>(terms2[idx], (long) idx));
        }
        FSTTester<Long> tester = new FSTTester<>(random(), dir, inputMode, pairs, outputs, true);
        final FST<Long> fst = tester.doTest(0, 0, false);
        assertNotNull(fst);
        assertEquals(22, tester.nodeCount);
        assertEquals(27, tester.arcCount);
      }

      // FST byte sequence ord
      {
        final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
        final BytesRef NO_OUTPUT = outputs.getNoOutput();      
        final List<FSTTester.InputOutput<BytesRef>> pairs = new ArrayList<>(terms2.length);
        for(int idx=0;idx<terms2.length;idx++) {
          final BytesRef output = random().nextInt(30) == 17 ? NO_OUTPUT : new BytesRef(Integer.toString(idx));
          pairs.add(new FSTTester.InputOutput<>(terms2[idx], output));
        }
        FSTTester<BytesRef> tester = new FSTTester<>(random(), dir, inputMode, pairs, outputs, false);
        final FST<BytesRef> fst = tester.doTest(0, 0, false);
        assertNotNull(fst);
        assertEquals(24, tester.nodeCount);
        assertEquals(30, tester.arcCount);
      }
    }
  }

  // given set of terms, test the different outputs for them
  private void doTest(int inputMode, IntsRef[] terms) throws IOException {
    Arrays.sort(terms);

    // NoOutputs (simple FSA)
    {
      final Outputs<Object> outputs = NoOutputs.getSingleton();
      final Object NO_OUTPUT = outputs.getNoOutput();      
      final List<FSTTester.InputOutput<Object>> pairs = new ArrayList<>(terms.length);
      for(IntsRef term : terms) {
        pairs.add(new FSTTester.InputOutput<>(term, NO_OUTPUT));
      }
      new FSTTester<>(random(), dir, inputMode, pairs, outputs, false).doTest(true);
    }

    // PositiveIntOutput (ord)
    {
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
      final List<FSTTester.InputOutput<Long>> pairs = new ArrayList<>(terms.length);
      for(int idx=0;idx<terms.length;idx++) {
        pairs.add(new FSTTester.InputOutput<>(terms[idx], (long) idx));
      }
      new FSTTester<>(random(), dir, inputMode, pairs, outputs, true).doTest(true);
    }

    // PositiveIntOutput (random monotonically increasing positive number)
    {
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
      final List<FSTTester.InputOutput<Long>> pairs = new ArrayList<>(terms.length);
      long lastOutput = 0;
      for(int idx=0;idx<terms.length;idx++) {
        final long value = lastOutput + TestUtil.nextInt(random(), 1, 1000);
        lastOutput = value;
        pairs.add(new FSTTester.InputOutput<>(terms[idx], value));
      }
      new FSTTester<>(random(), dir, inputMode, pairs, outputs, true).doTest(true);
    }

    // PositiveIntOutput (random positive number)
    {
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
      final List<FSTTester.InputOutput<Long>> pairs = new ArrayList<>(terms.length);
      for(int idx=0;idx<terms.length;idx++) {
        pairs.add(new FSTTester.InputOutput<>(terms[idx], TestUtil.nextLong(random(), 0, Long.MAX_VALUE)));
      }
      new FSTTester<>(random(), dir, inputMode, pairs, outputs, false).doTest(true);
    }

    // Pair<ord, (random monotonically increasing positive number>
    {
      final PositiveIntOutputs o1 = PositiveIntOutputs.getSingleton();
      final PositiveIntOutputs o2 = PositiveIntOutputs.getSingleton();
      final PairOutputs<Long,Long> outputs = new PairOutputs<>(o1, o2);
      final List<FSTTester.InputOutput<PairOutputs.Pair<Long,Long>>> pairs = new ArrayList<>(terms.length);
      long lastOutput = 0;
      for(int idx=0;idx<terms.length;idx++) {
        final long value = lastOutput + TestUtil.nextInt(random(), 1, 1000);
        lastOutput = value;
        pairs.add(new FSTTester.InputOutput<>(terms[idx],
                                              outputs.newPair((long) idx, value)));
      }
      new FSTTester<>(random(), dir, inputMode, pairs, outputs, false).doTest(true);
    }

    // Sequence-of-bytes
    {
      final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
      final BytesRef NO_OUTPUT = outputs.getNoOutput();      
      final List<FSTTester.InputOutput<BytesRef>> pairs = new ArrayList<>(terms.length);
      for(int idx=0;idx<terms.length;idx++) {
        final BytesRef output = random().nextInt(30) == 17 ? NO_OUTPUT : new BytesRef(Integer.toString(idx));
        pairs.add(new FSTTester.InputOutput<>(terms[idx], output));
      }
      new FSTTester<>(random(), dir, inputMode, pairs, outputs, false).doTest(true);
    }

    // Sequence-of-ints
    {
      final IntSequenceOutputs outputs = IntSequenceOutputs.getSingleton();
      final List<FSTTester.InputOutput<IntsRef>> pairs = new ArrayList<>(terms.length);
      for(int idx=0;idx<terms.length;idx++) {
        final String s = Integer.toString(idx);
        final IntsRef output = new IntsRef(s.length());
        output.length = s.length();
        for(int idx2=0;idx2<output.length;idx2++) {
          output.ints[idx2] = s.charAt(idx2);
        }
        pairs.add(new FSTTester.InputOutput<>(terms[idx], output));
      }
      new FSTTester<>(random(), dir, inputMode, pairs, outputs, false).doTest(true);
    }

  }


  public void testRandomWords() throws IOException {
    if (TEST_NIGHTLY) {
      testRandomWords(1000, atLeast(2));
    } else {
      testRandomWords(100, 1);
    }
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
        Set<IntsRef> termsSet = new HashSet<>();
        IntsRef[] terms = new IntsRef[numWords];
        while(termsSet.size() < numWords) {
          final String term = getRandomString(random);
          termsSet.add(toIntsRef(term, inputMode));
        }
        doTest(inputMode, termsSet.toArray(new IntsRef[termsSet.size()]));
      }
    }
  }

  @Nightly
  public void testBigSet() throws IOException {
    testRandomWords(TestUtil.nextInt(random(), 50000, 60000), 1);
  }
  
  // Build FST for all unique terms in the test line docs
  // file, up until a doc limit
  public void testRealTerms() throws Exception {

    final LineFileDocs docs = new LineFileDocs(random());
    final int numDocs = TEST_NIGHTLY ? atLeast(1000) : atLeast(100);
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));

    final IndexWriterConfig conf = newIndexWriterConfig(analyzer).setMaxBufferedDocs(-1).setRAMBufferSizeMB(64);
    final Path tempDir = createTempDir("fstlines");
    final Directory dir = newFSDirectory(tempDir);
    final IndexWriter writer = new IndexWriter(dir, conf);
    Document doc;
    int docCount = 0;
    while((doc = docs.nextDoc()) != null && docCount < numDocs) {
      writer.addDocument(doc);
      docCount++;
    }
    IndexReader r = DirectoryReader.open(writer);
    writer.close();
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();

    final boolean doRewrite = random().nextBoolean();

    Builder<Long> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, outputs, doRewrite, PackedInts.DEFAULT, true, 15);

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
      final IntsRefBuilder scratchIntsRef = new IntsRefBuilder();
      final TermsEnum termsEnum = terms.iterator();
      if (VERBOSE) {
        System.out.println("TEST: got termsEnum=" + termsEnum);
      }
      BytesRef term;
      int ord = 0;

      Automaton automaton = new RegExp(".*", RegExp.NONE).toAutomaton();
      final TermsEnum termsEnum2 = terms.intersect(new CompiledAutomaton(automaton, false, false), null);

      while((term = termsEnum.next()) != null) {
        BytesRef term2 = termsEnum2.next();
        assertNotNull(term2);
        assertEquals(term, term2);
        assertEquals(termsEnum.docFreq(), termsEnum2.docFreq());
        assertEquals(termsEnum.totalTermFreq(), termsEnum2.totalTermFreq());

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
        System.out.println("FST: " + docCount + " docs; " + ord + " terms; " + builder.getNodeCount() + " nodes; " + builder.getArcCount() + " arcs;" + " " + fst.ramBytesUsed() + " bytes");
      }

      if (ord > 0) {
        final Random random = new Random(random().nextLong());
        // Now confirm BytesRefFSTEnum and TermsEnum act the
        // same:
        final BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<>(fst);
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
    private final Path dirOut;
    private final Path wordsFileIn;
    private int inputMode;
    private final Outputs<T> outputs;
    private final Builder<T> builder;
    private final boolean doPack;

    public VisitTerms(Path dirOut, Path wordsFileIn, int inputMode, int prune, Outputs<T> outputs, boolean doPack, boolean noArcArrays) {
      this.dirOut = dirOut;
      this.wordsFileIn = wordsFileIn;
      this.inputMode = inputMode;
      this.outputs = outputs;
      this.doPack = doPack;

      builder = new Builder<>(inputMode == 0 ? FST.INPUT_TYPE.BYTE1 : FST.INPUT_TYPE.BYTE4, 0, prune, prune == 0, true, Integer.MAX_VALUE, outputs, doPack, PackedInts.DEFAULT, !noArcArrays, 15);
    }

    protected abstract T getOutput(IntsRef input, int ord) throws IOException;

    public void run(int limit, boolean verify, boolean verifyByOutput) throws IOException {
      
      BufferedReader is = Files.newBufferedReader(wordsFileIn, StandardCharsets.UTF_8);
      try {
        final IntsRefBuilder intsRef = new IntsRefBuilder();
        long tStart = System.currentTimeMillis();
        int ord = 0;
        while(true) {
          String w = is.readLine();
          if (w == null) {
            break;
          }
          toIntsRef(w, inputMode, intsRef);
          builder.add(intsRef.get(),
                      getOutput(intsRef.get(), ord));

          ord++;
          if (ord % 500000 == 0) {
            System.out.println(
                String.format(Locale.ROOT,
                    "%6.2fs: %9d...", ((System.currentTimeMillis() - tStart) / 1000.0), ord));
          }
          if (ord >= limit) {
            break;
          }
        }

        long tMid = System.currentTimeMillis();
        System.out.println(((tMid-tStart) / 1000.0) + " sec to add all terms");

        assert builder.getTermCount() == ord;
        FST<T> fst = builder.finish();
        long tEnd = System.currentTimeMillis();
        System.out.println(((tEnd-tMid) / 1000.0) + " sec to finish/pack");
        if (fst == null) {
          System.out.println("FST was fully pruned!");
          System.exit(0);
        }

        if (dirOut == null) {
          return;
        }

        System.out.println(ord + " terms; " + builder.getNodeCount() + " nodes; " + builder.getArcCount() + " arcs; tot size " + fst.ramBytesUsed());
        if (builder.getNodeCount() < 100) {
          Writer w = Files.newBufferedWriter(Paths.get("out.dot"), StandardCharsets.UTF_8);
          Util.toDot(fst, w, false, false);
          w.close();
          System.out.println("Wrote FST to out.dot");
        }

        Directory dir = FSDirectory.open(dirOut);
        IndexOutput out = dir.createOutput("fst.bin", IOContext.DEFAULT);
        fst.save(out);
        out.close();
        System.out.println("Saved FST to fst.bin.");

        if (!verify) {
          return;
        }

        /*
        IndexInput in = dir.openInput("fst.bin", IOContext.DEFAULT);
        fst = new FST<T>(in, outputs);
        in.close();
        */

        System.out.println("\nNow verify...");

        while(true) {
          for(int iter=0;iter<2;iter++) {
            is.close();
            is = Files.newBufferedReader(wordsFileIn, StandardCharsets.UTF_8);

            ord = 0;
            tStart = System.currentTimeMillis();
            while(true) {
              String w = is.readLine();
              if (w == null) {
                break;
              }
              toIntsRef(w, inputMode, intsRef);
              if (iter == 0) {
                T expected = getOutput(intsRef.get(), ord);
                T actual = Util.get(fst, intsRef.get());
                if (actual == null) {
                  throw new RuntimeException("unexpected null output on input=" + w);
                }
                if (!actual.equals(expected)) {
                  throw new RuntimeException("wrong output (got " + outputs.outputToString(actual) + " but expected " + outputs.outputToString(expected) + ") on input=" + w);
                }
              } else {
                // Get by output
                final Long output = (Long) getOutput(intsRef.get(), ord);
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

  // TODO: try experiment: reverse terms before
  // compressing -- how much smaller?

  // TODO: can FST be used to index all internal substrings,
  // mapping to term?

  // java -cp ../build/codecs/classes/java:../test-framework/lib/randomizedtesting-runner-*.jar:../build/core/classes/test:../build/core/classes/test-framework:../build/core/classes/java:../build/test-framework/classes/java:../test-framework/lib/junit-4.10.jar org.apache.lucene.util.fst.TestFSTs /xold/tmp/allTerms3.txt out
  public static void main(String[] args) throws IOException {
    int prune = 0;
    int limit = Integer.MAX_VALUE;
    int inputMode = 0;                             // utf8
    boolean storeOrds = false;
    boolean storeDocFreqs = false;
    boolean verify = true;
    boolean doPack = false;
    boolean noArcArrays = false;
    Path wordsFileIn = null;
    Path dirOut = null;

    int idx = 0;
    while (idx < args.length) {
      if (args[idx].equals("-prune")) {
        prune = Integer.parseInt(args[1 + idx]);
        idx++;
      } else if (args[idx].equals("-limit")) {
        limit = Integer.parseInt(args[1 + idx]);
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
          wordsFileIn = Paths.get(args[idx]);
        } else if (dirOut == null) {
          dirOut = Paths.get(args[idx]);
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
      final PositiveIntOutputs o1 = PositiveIntOutputs.getSingleton();
      final PositiveIntOutputs o2 = PositiveIntOutputs.getSingleton();
      final PairOutputs<Long,Long> outputs = new PairOutputs<>(o1, o2);
      new VisitTerms<PairOutputs.Pair<Long,Long>>(dirOut, wordsFileIn, inputMode, prune, outputs, doPack, noArcArrays) {
        Random rand;
        @Override
        public PairOutputs.Pair<Long,Long> getOutput(IntsRef input, int ord) {
          if (ord == 0) {
            rand = new Random(17);
          }
          return outputs.newPair((long) ord,
                                 (long) TestUtil.nextInt(rand, 1, 5000));
        }
      }.run(limit, verify, false);
    } else if (storeOrds) {
      // Store only ords
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
      new VisitTerms<Long>(dirOut, wordsFileIn, inputMode, prune, outputs, doPack, noArcArrays) {
        @Override
        public Long getOutput(IntsRef input, int ord) {
          return (long) ord;
        }
      }.run(limit, verify, true);
    } else if (storeDocFreqs) {
      // Store only docFreq
      final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
      new VisitTerms<Long>(dirOut, wordsFileIn, inputMode, prune, outputs, doPack, noArcArrays) {
        Random rand;
        @Override
        public Long getOutput(IntsRef input, int ord) {
          if (ord == 0) {
            rand = new Random(17);
          }
          return (long) TestUtil.nextInt(rand, 1, 5000);
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
    final Builder<Object> b = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
    b.add(Util.toIntsRef(new BytesRef("foobar"), new IntsRefBuilder()), outputs.getNoOutput());
    final BytesRefFSTEnum<Object> fstEnum = new BytesRefFSTEnum<>(b.finish());
    assertNull(fstEnum.seekFloor(new BytesRef("foo")));
    assertNull(fstEnum.seekCeil(new BytesRef("foobaz")));
  }


  public void testDuplicateFSAString() throws Exception {
    String str = "foobar";
    final Outputs<Object> outputs = NoOutputs.getSingleton();
    final Builder<Object> b = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
    IntsRefBuilder ints = new IntsRefBuilder();
    for(int i=0; i<10; i++) {
      b.add(Util.toIntsRef(new BytesRef(str), ints), outputs.getNoOutput());
    }
    FST<Object> fst = b.finish();

    // count the input paths
    int count = 0;
    final BytesRefFSTEnum<Object> fstEnum = new BytesRefFSTEnum<>(fst);
    while(fstEnum.next()!=null) {
      count++;
    }
    assertEquals(1, count);

    assertNotNull(Util.get(fst, new BytesRef(str)));
    assertNull(Util.get(fst, new BytesRef("foobaz")));
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
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();

    // Build an FST mapping BytesRef -> Long
    final Builder<Long> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);

    final BytesRef a = new BytesRef("a");
    final BytesRef b = new BytesRef("b");
    final BytesRef c = new BytesRef("c");

    builder.add(Util.toIntsRef(a, new IntsRefBuilder()), 17L);
    builder.add(Util.toIntsRef(b, new IntsRefBuilder()), 42L);
    builder.add(Util.toIntsRef(c, new IntsRefBuilder()), 13824324872317238L);

    final FST<Long> fst = builder.finish();

    assertEquals(13824324872317238L, (long) Util.get(fst, c));
    assertEquals(42, (long) Util.get(fst, b));
    assertEquals(17, (long) Util.get(fst, a));

    BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<>(fst);
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

    assertEquals(Util.toIntsRef(new BytesRef("c"), new IntsRefBuilder()),
                 Util.getByOutput(fst, 13824324872317238L));
    assertNull(Util.getByOutput(fst, 47));
    assertEquals(Util.toIntsRef(new BytesRef("b"), new IntsRefBuilder()),
                 Util.getByOutput(fst, 42));
    assertEquals(Util.toIntsRef(new BytesRef("a"), new IntsRefBuilder()),
                 Util.getByOutput(fst, 17));
  }

  public void testPrimaryKeys() throws Exception {
    Directory dir = newDirectory();

    for(int cycle=0;cycle<2;cycle++) {
      if (VERBOSE) {
        System.out.println("TEST: cycle=" + cycle);
      }
      RandomIndexWriter w = new RandomIndexWriter(random(), dir,
                                                  newIndexWriterConfig(new MockAnalyzer(random())).setOpenMode(IndexWriterConfig.OpenMode.CREATE));
      Document doc = new Document();
      Field idField = newStringField("id", "", Field.Store.NO);
      doc.add(idField);

      final int NUM_IDS = atLeast(200);
      //final int NUM_IDS = (int) (377 * (1.0+random.nextDouble()));
      if (VERBOSE) {
        System.out.println("TEST: NUM_IDS=" + NUM_IDS);
      }
      final Set<String> allIDs = new HashSet<>();
      for(int id=0;id<NUM_IDS;id++) {
        String idString;
        if (cycle == 0) {
          // PKs are assigned sequentially
          idString = String.format(Locale.ROOT, "%07d", id);
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
      final IndexSearcher s = newSearcher(r);
      w.close();

      final List<String> allIDsList = new ArrayList<>(allIDs);
      final List<String> sortedAllIDsList = new ArrayList<>(allIDsList);
      Collections.sort(sortedAllIDsList);

      // Sprinkle in some non-existent PKs:
      Set<String> outOfBounds = new HashSet<>();
      for(int idx=0;idx<NUM_IDS/10;idx++) {
        String idString;
        if (cycle == 0) {
          idString = String.format(Locale.ROOT, "%07d", (NUM_IDS + idx));
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
      final TermsEnum termsEnum = MultiFields.getTerms(r, "id").iterator();
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
            id = String.format(Locale.ROOT, "%07da", idv);
            nextID = String.format(Locale.ROOT, "%07d", idv+1);
          } else {
            id = sortedAllIDsList.get(idv) + "a";
            nextID = sortedAllIDsList.get(idv+1);
          }
          if (VERBOSE) {
            System.out.println("TEST: not exactOnly id=" + id + " nextID=" + nextID);
          }
        }

        final TermsEnum.SeekStatus status;
        if (nextID == null) {
          if (termsEnum.seekExact(new BytesRef(id))) {
            status = TermsEnum.SeekStatus.FOUND;
          } else {
            status = TermsEnum.SeekStatus.NOT_FOUND;
          }
        } else {
          status = termsEnum.seekCeil(new BytesRef(id));
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
                                                newIndexWriterConfig(new MockAnalyzer(random())).setOpenMode(IndexWriterConfig.OpenMode.CREATE));
    Document doc = new Document();
    Field f = newStringField("field", "", Field.Store.NO);
    doc.add(f);

    final int NUM_TERMS = (int) (1000*RANDOM_MULTIPLIER * (1+random().nextDouble()));
    if (VERBOSE) {
      System.out.println("TEST: NUM_TERMS=" + NUM_TERMS);
    }

    final Set<String> allTerms = new HashSet<>();
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
    IndexSearcher s = newSearcher(r);
    w.close();

    final List<String> allTermsList = new ArrayList<>(allTerms);
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
   * @see <a href="https://issues.apache.org/jira/browse/LUCENE-2933">LUCENE-2933</a>
   */
  public void testExpandedCloseToRoot() throws Exception {
    class SyntheticData {
      FST<Object> compile(String[] lines) throws IOException {
        final NoOutputs outputs = NoOutputs.getSingleton();
        final Object nothing = outputs.getNoOutput();
        final Builder<Object> b = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);

        int line = 0;
        final BytesRefBuilder term = new BytesRefBuilder();
        final IntsRefBuilder scratchIntsRef = new IntsRefBuilder();
        while (line < lines.length) {
          String w = lines[line++];
          if (w == null) {
            break;
          }
          term.copyChars(w);
          b.add(Util.toIntsRef(term.get(), scratchIntsRef), nothing);
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
          BytesReader fstReader = fst.getBytesReader();
          for (arc = fst.readFirstTargetArc(arc, arc, fstReader);;
               arc = fst.readNextArc(arc, fstReader), childCount++)
          {
            boolean expanded = fst.isExpandedTarget(arc, fstReader);
            int children = verifyStateAndBelow(fst, new FST.Arc<>().copyFrom(arc), depth + 1);

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

    ArrayList<String> out = new ArrayList<>();
    StringBuilder b = new StringBuilder();
    s.generate(out, b, 'a', 'i', 10);
    String[] input = out.toArray(new String[out.size()]);
    Arrays.sort(input);
    FST<Object> fst = s.compile(input);
    FST.Arc<Object> arc = fst.getFirstArc(new FST.Arc<>());
    s.verifyStateAndBelow(fst, arc, 1);
  }

  public void testFinalOutputOnEndState() throws Exception {
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();

    final Builder<Long> builder = new Builder<>(FST.INPUT_TYPE.BYTE4, 2, 0, true, true, Integer.MAX_VALUE, outputs, random().nextBoolean(), PackedInts.DEFAULT, true, 15);
    builder.add(Util.toUTF32("stat", new IntsRefBuilder()), 17L);
    builder.add(Util.toUTF32("station", new IntsRefBuilder()), 10L);
    final FST<Long> fst = builder.finish();
    //Writer w = new OutputStreamWriter(new FileOutputStream("/x/tmp3/out.dot"));
    StringWriter w = new StringWriter();
    Util.toDot(fst, w, false, false);
    w.close();
    //System.out.println(w.toString());
    assertTrue(w.toString().indexOf("label=\"t/[7]\"") != -1);
  }

  public void testInternalFinalState() throws Exception {
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    final boolean willRewrite = random().nextBoolean();
    final Builder<Long> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, outputs, willRewrite, PackedInts.DEFAULT, true, 15);
    builder.add(Util.toIntsRef(new BytesRef("stat"), new IntsRefBuilder()), outputs.getNoOutput());
    builder.add(Util.toIntsRef(new BytesRef("station"), new IntsRefBuilder()), outputs.getNoOutput());
    final FST<Long> fst = builder.finish();
    StringWriter w = new StringWriter();
    //Writer w = new OutputStreamWriter(new FileOutputStream("/x/tmp/out.dot"));
    Util.toDot(fst, w, false, false);
    w.close();
    //System.out.println(w.toString());

    // check for accept state at label t
    assertTrue(w.toString().indexOf("[label=\"t\" style=\"bold\"") != -1);
    // check for accept state at label n
    assertTrue(w.toString().indexOf("[label=\"n\" style=\"bold\"") != -1);
  }

  // Make sure raw FST can differentiate between final vs
  // non-final end nodes
  public void testNonFinalStopNode() throws Exception {
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    final Long nothing = outputs.getNoOutput();
    final Builder<Long> b = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);

    //final FST<Long> fst = new FST<>(FST.INPUT_TYPE.BYTE1, outputs, false, PackedInts.COMPACT, 15);
    final FST<Long> fst = b.fst;

    final Builder.UnCompiledNode<Long> rootNode = new Builder.UnCompiledNode<>(b, 0);

    // Add final stop node
    {
      final Builder.UnCompiledNode<Long> node = new Builder.UnCompiledNode<>(b, 0);
      node.isFinal = true;
      rootNode.addArc('a', node);
      final Builder.CompiledNode frozen = new Builder.CompiledNode();
      frozen.node = fst.addNode(b, node);
      rootNode.arcs[0].nextFinalOutput = 17L;
      rootNode.arcs[0].isFinal = true;
      rootNode.arcs[0].output = nothing;
      rootNode.arcs[0].target = frozen;
    }

    // Add non-final stop node
    {
      final Builder.UnCompiledNode<Long> node = new Builder.UnCompiledNode<>(b, 0);
      rootNode.addArc('b', node);
      final Builder.CompiledNode frozen = new Builder.CompiledNode();
      frozen.node = fst.addNode(b, node);
      rootNode.arcs[1].nextFinalOutput = nothing;
      rootNode.arcs[1].output = 42L;
      rootNode.arcs[1].target = frozen;
    }

    fst.finish(fst.addNode(b, rootNode));

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
    final FST<Long> fst2 = new FST<>(in, outputs);
    checkStopNodes(fst2, outputs);
    in.close();
    dir.close();
  }

  private void checkStopNodes(FST<Long> fst, PositiveIntOutputs outputs) throws Exception {
    final Long nothing = outputs.getNoOutput();
    FST.Arc<Long> startArc = fst.getFirstArc(new FST.Arc<Long>());
    assertEquals(nothing, startArc.output);
    assertEquals(nothing, startArc.nextFinalOutput);

    FST.Arc<Long> arc = fst.readFirstTargetArc(startArc, new FST.Arc<Long>(),
                                               fst.getBytesReader());
    assertEquals('a', arc.label);
    assertEquals(17, arc.nextFinalOutput.longValue());
    assertTrue(arc.isFinal());

    arc = fst.readNextArc(arc, fst.getBytesReader());
    assertEquals('b', arc.label);
    assertFalse(arc.isFinal());
    assertEquals(42, arc.output.longValue());
  }

  static final Comparator<Long> minLongComparator = new Comparator<Long> () {
    @Override
    public int compare(Long left, Long right) {
      return left.compareTo(right);
    }
  };

  public void testShortestPaths() throws Exception {
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    final Builder<Long> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);

    final IntsRefBuilder scratch = new IntsRefBuilder();
    builder.add(Util.toIntsRef(new BytesRef("aab"), scratch), 22L);
    builder.add(Util.toIntsRef(new BytesRef("aac"), scratch), 7L);
    builder.add(Util.toIntsRef(new BytesRef("ax"), scratch), 17L);
    final FST<Long> fst = builder.finish();
    //Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"));
    //Util.toDot(fst, w, false, false);
    //w.close();

    Util.TopResults<Long> res = Util.shortestPaths(fst,
                                                  fst.getFirstArc(new FST.Arc<Long>()),
                                                  outputs.getNoOutput(),
                                                  minLongComparator,
                                                  3,
                                                  true);
    assertTrue(res.isComplete);
    assertEquals(3, res.topN.size());
    assertEquals(Util.toIntsRef(new BytesRef("aac"), scratch), res.topN.get(0).input);
    assertEquals(7L, res.topN.get(0).output.longValue());

    assertEquals(Util.toIntsRef(new BytesRef("ax"), scratch), res.topN.get(1).input);
    assertEquals(17L,res.topN.get(1).output.longValue());

    assertEquals(Util.toIntsRef(new BytesRef("aab"), scratch), res.topN.get(2).input);
    assertEquals(22L, res.topN.get(2).output.longValue());
  }

  public void testRejectNoLimits() throws IOException {
    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    final Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, outputs);

    final IntsRefBuilder scratch = new IntsRefBuilder();
    builder.add(Util.toIntsRef(new BytesRef("aab"), scratch), 22L);
    builder.add(Util.toIntsRef(new BytesRef("aac"), scratch), 7L);
    builder.add(Util.toIntsRef(new BytesRef("adcd"), scratch), 17L);
    builder.add(Util.toIntsRef(new BytesRef("adcde"), scratch), 17L);

    builder.add(Util.toIntsRef(new BytesRef("ax"), scratch), 17L);
    final FST<Long> fst = builder.finish();
    final AtomicInteger rejectCount = new AtomicInteger();
    Util.TopNSearcher<Long> searcher = new Util.TopNSearcher<Long>(fst, 2, 6, minLongComparator) {
      @Override
      protected boolean acceptResult(IntsRef input, Long output) {
        boolean accept = output.intValue() == 7;
        if (!accept) {
          rejectCount.incrementAndGet();
        }
        return accept;
      }
    };

    searcher.addStartPaths(fst.getFirstArc(new FST.Arc<Long>()),  outputs.getNoOutput(), true, new IntsRefBuilder());
    Util.TopResults<Long> res = searcher.search();
    assertEquals(rejectCount.get(), 4);
    assertTrue(res.isComplete); // rejected(4) + topN(2) <= maxQueueSize(6)

    assertEquals(1, res.topN.size());
    assertEquals(Util.toIntsRef(new BytesRef("aac"), scratch), res.topN.get(0).input);
    assertEquals(7L, res.topN.get(0).output.longValue());
    rejectCount.set(0);
    searcher = new Util.TopNSearcher<Long>(fst, 2, 5, minLongComparator) {
      @Override
      protected boolean acceptResult(IntsRef input, Long output) {
        boolean accept = output.intValue() == 7;
        if (!accept) {
          rejectCount.incrementAndGet();
        }
        return accept;
      }
    };

    searcher.addStartPaths(fst.getFirstArc(new FST.Arc<Long>()),  outputs.getNoOutput(), true, new IntsRefBuilder());
    res = searcher.search();
    assertEquals(rejectCount.get(), 4);
    assertFalse(res.isComplete); // rejected(4) + topN(2) > maxQueueSize(5)
  }

  // compares just the weight side of the pair
  static final Comparator<Pair<Long,Long>> minPairWeightComparator = new Comparator<Pair<Long,Long>> () {
    @Override
    public int compare(Pair<Long,Long> left, Pair<Long,Long> right) {
      return left.output1.compareTo(right.output1);
    }
  };

  /** like testShortestPaths, but uses pairoutputs so we have both a weight and an output */
  public void testShortestPathsWFST() throws Exception {

    PairOutputs<Long,Long> outputs = new PairOutputs<>(
        PositiveIntOutputs.getSingleton(), // weight
        PositiveIntOutputs.getSingleton()  // output
    );

    final Builder<Pair<Long,Long>> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);

    final IntsRefBuilder scratch = new IntsRefBuilder();
    builder.add(Util.toIntsRef(new BytesRef("aab"), scratch), outputs.newPair(22L, 57L));
    builder.add(Util.toIntsRef(new BytesRef("aac"), scratch), outputs.newPair(7L, 36L));
    builder.add(Util.toIntsRef(new BytesRef("ax"), scratch), outputs.newPair(17L, 85L));
    final FST<Pair<Long,Long>> fst = builder.finish();
    //Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"));
    //Util.toDot(fst, w, false, false);
    //w.close();

    Util.TopResults<Pair<Long,Long>> res = Util.shortestPaths(fst,
                                                             fst.getFirstArc(new FST.Arc<Pair<Long,Long>>()),
                                                             outputs.getNoOutput(),
                                                             minPairWeightComparator,
                                                             3,
                                                             true);
    assertTrue(res.isComplete);
    assertEquals(3, res.topN.size());

    assertEquals(Util.toIntsRef(new BytesRef("aac"), scratch), res.topN.get(0).input);
    assertEquals(7L, res.topN.get(0).output.output1.longValue()); // weight
    assertEquals(36L, res.topN.get(0).output.output2.longValue()); // output

    assertEquals(Util.toIntsRef(new BytesRef("ax"), scratch), res.topN.get(1).input);
    assertEquals(17L, res.topN.get(1).output.output1.longValue()); // weight
    assertEquals(85L, res.topN.get(1).output.output2.longValue()); // output

    assertEquals(Util.toIntsRef(new BytesRef("aab"), scratch), res.topN.get(2).input);
    assertEquals(22L, res.topN.get(2).output.output1.longValue()); // weight
    assertEquals(57L, res.topN.get(2).output.output2.longValue()); // output
  }

  public void testShortestPathsRandom() throws Exception {
    final Random random = random();
    int numWords = atLeast(1000);

    final TreeMap<String,Long> slowCompletor = new TreeMap<>();
    final TreeSet<String> allPrefixes = new TreeSet<>();

    final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    final Builder<Long> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
    final IntsRefBuilder scratch = new IntsRefBuilder();

    for (int i = 0; i < numWords; i++) {
      String s;
      while (true) {
        s = TestUtil.randomSimpleString(random);
        if (!slowCompletor.containsKey(s)) {
          break;
        }
      }

      for (int j = 1; j < s.length(); j++) {
        allPrefixes.add(s.substring(0, j));
      }
      int weight = TestUtil.nextInt(random, 1, 100); // weights 1..100
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

    BytesReader reader = fst.getBytesReader();

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

      final int topN = TestUtil.nextInt(random, 1, 10);

      Util.TopResults<Long> r = Util.shortestPaths(fst, arc, fst.outputs.getNoOutput(), minLongComparator, topN, true);
      assertTrue(r.isComplete);

      // 2. go thru whole treemap (slowCompletor) and check it's actually the best suggestion
      final List<Result<Long>> matches = new ArrayList<>();

      // TODO: could be faster... but it's slowCompletor for a reason
      for (Map.Entry<String,Long> e : slowCompletor.entrySet()) {
        if (e.getKey().startsWith(prefix)) {
          //System.out.println("  consider " + e.getKey());
          matches.add(new Result<>(Util.toIntsRef(new BytesRef(e.getKey().substring(prefix.length())), new IntsRefBuilder()),
                                         e.getValue() - prefixOutput));
        }
      }

      assertTrue(matches.size() > 0);
      Collections.sort(matches, new TieBreakByInputComparator<>(minLongComparator));
      if (matches.size() > topN) {
        matches.subList(topN, matches.size()).clear();
      }

      assertEquals(matches.size(), r.topN.size());

      for(int hit=0;hit<r.topN.size();hit++) {
        //System.out.println("  check hit " + hit);
        assertEquals(matches.get(hit).input, r.topN.get(hit).input);
        assertEquals(matches.get(hit).output, r.topN.get(hit).output);
      }
    }
  }

  private static class TieBreakByInputComparator<T> implements Comparator<Result<T>> {
    private final Comparator<T> comparator;
    public TieBreakByInputComparator(Comparator<T> comparator) {
      this.comparator = comparator;
    }

    @Override
    public int compare(Result<T> a, Result<T> b) {
      int cmp = comparator.compare(a.output, b.output);
      if (cmp == 0) {
        return a.input.compareTo(b.input);
      } else {
        return cmp;
      }
    }
  }

  // used by slowcompletor
  static class TwoLongs {
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

    final TreeMap<String,TwoLongs> slowCompletor = new TreeMap<>();
    final TreeSet<String> allPrefixes = new TreeSet<>();

    PairOutputs<Long,Long> outputs = new PairOutputs<>(
        PositiveIntOutputs.getSingleton(), // weight
        PositiveIntOutputs.getSingleton()  // output
    );
    final Builder<Pair<Long,Long>> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
    final IntsRefBuilder scratch = new IntsRefBuilder();

    Random random = random();
    for (int i = 0; i < numWords; i++) {
      String s;
      while (true) {
        s = TestUtil.randomSimpleString(random);
        if (!slowCompletor.containsKey(s)) {
          break;
        }
      }

      for (int j = 1; j < s.length(); j++) {
        allPrefixes.add(s.substring(0, j));
      }
      int weight = TestUtil.nextInt(random, 1, 100); // weights 1..100
      int output = TestUtil.nextInt(random, 0, 500); // outputs 0..500
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

    BytesReader reader = fst.getBytesReader();

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

      final int topN = TestUtil.nextInt(random, 1, 10);

      Util.TopResults<Pair<Long,Long>> r = Util.shortestPaths(fst, arc, fst.outputs.getNoOutput(), minPairWeightComparator, topN, true);
      assertTrue(r.isComplete);
      // 2. go thru whole treemap (slowCompletor) and check it's actually the best suggestion
      final List<Result<Pair<Long,Long>>> matches = new ArrayList<>();

      // TODO: could be faster... but it's slowCompletor for a reason
      for (Map.Entry<String,TwoLongs> e : slowCompletor.entrySet()) {
        if (e.getKey().startsWith(prefix)) {
          //System.out.println("  consider " + e.getKey());
          matches.add(new Result<>(Util.toIntsRef(new BytesRef(e.getKey().substring(prefix.length())), new IntsRefBuilder()),
                                                  outputs.newPair(e.getValue().a - prefixOutput.output1, e.getValue().b - prefixOutput.output2)));
        }
      }

      assertTrue(matches.size() > 0);
      Collections.sort(matches, new TieBreakByInputComparator<>(minPairWeightComparator));
      if (matches.size() > topN) {
        matches.subList(topN, matches.size()).clear();
      }

      assertEquals(matches.size(), r.topN.size());

      for(int hit=0;hit<r.topN.size();hit++) {
        //System.out.println("  check hit " + hit);
        assertEquals(matches.get(hit).input, r.topN.get(hit).input);
        assertEquals(matches.get(hit).output, r.topN.get(hit).output);
      }
    }
  }

  public void testLargeOutputsOnArrayArcs() throws Exception {
    final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
    final Builder<BytesRef> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);

    final byte[] bytes = new byte[300];
    final IntsRefBuilder input = new IntsRefBuilder();
    input.append(0);
    final BytesRef output = new BytesRef(bytes);
    for(int arc=0;arc<6;arc++) {
      input.setIntAt(0, arc);
      output.bytes[0] = (byte) arc;
      builder.add(input.get(), BytesRef.deepCopyOf(output));
    }

    final FST<BytesRef> fst = builder.finish();
    for(int arc=0;arc<6;arc++) {
      input.setIntAt(0,  arc);
      final BytesRef result = Util.get(fst, input.get());
      assertNotNull(result);
      assertEquals(300, result.length);
      assertEquals(result.bytes[result.offset], arc);
      for(int byteIDX=1;byteIDX<result.length;byteIDX++) {
        assertEquals(0, result.bytes[result.offset+byteIDX]);
      }
    }
  }

  public void testIllegallyModifyRootArc() throws Exception {
    assumeTrue("test relies on assertions", assertsAreEnabled);

    Set<BytesRef> terms = new HashSet<>();
    for(int i=0;i<100;i++) {
      String prefix = Character.toString((char) ('a' + i));
      terms.add(new BytesRef(prefix));
      if (prefix.equals("m") == false) {
        for(int j=0;j<20;j++) {
          // Make a big enough FST that the root cache will be created:
          String suffix = TestUtil.randomRealisticUnicodeString(random(), 10, 20);
          terms.add(new BytesRef(prefix + suffix));
        }
      }
    }

    List<BytesRef> termsList = new ArrayList<>(terms);
    Collections.sort(termsList);

    ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
    Builder<BytesRef> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);

    IntsRefBuilder input = new IntsRefBuilder();
    for(BytesRef term : termsList) {
      Util.toIntsRef(term, input);
      builder.add(input.get(), term);
    }

    FST<BytesRef> fst = builder.finish();
    
    Arc<BytesRef> arc = new FST.Arc<>();
    fst.getFirstArc(arc);
    FST.BytesReader reader = fst.getBytesReader();
    arc = fst.findTargetArc((int) 'm', arc, arc, reader);
    assertNotNull(arc);
    assertEquals(new BytesRef("m"), arc.output);

    // NOTE: illegal:
    arc.output.length = 0;

    fst.getFirstArc(arc);
    try {
      arc = fst.findTargetArc((int) 'm', arc, arc, reader);
    } catch (AssertionError ae) {
      // expected
    }
  }
}
