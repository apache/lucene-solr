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
package org.apache.lucene.search.suggest.analyzing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenStreamToAutomaton;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.LimitedFiniteStringsIterator;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs.Pair;
import org.apache.lucene.util.fst.PairOutputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util.Result;
import org.apache.lucene.util.fst.Util.TopResults;
import org.apache.lucene.util.fst.Util;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_MAX_DETERMINIZED_STATES;

/**
 * Suggester that first analyzes the surface form, adds the
 * analyzed form to a weighted FST, and then does the same
 * thing at lookup time.  This means lookup is based on the
 * analyzed form while suggestions are still the surface
 * form(s).
 *
 * <p>
 * This can result in powerful suggester functionality.  For
 * example, if you use an analyzer removing stop words, 
 * then the partial text "ghost chr..." could see the
 * suggestion "The Ghost of Christmas Past". Note that
 * position increments MUST NOT be preserved for this example
 * to work, so you should call the constructor with 
 * <code>preservePositionIncrements</code> parameter set to 
 * false
 *
 * <p>
 * If SynonymFilter is used to map wifi and wireless network to
 * hotspot then the partial text "wirele..." could suggest
 * "wifi router".  Token normalization like stemmers, accent
 * removal, etc., would allow suggestions to ignore such
 * variations.
 *
 * <p>
 * When two matching suggestions have the same weight, they
 * are tie-broken by the analyzed form.  If their analyzed
 * form is the same then the order is undefined.
 *
 * <p>
 * There are some limitations:
 * <ul>
 *
 *   <li> A lookup from a query like "net" in English won't
 *        be any different than "net " (ie, user added a
 *        trailing space) because analyzers don't reflect
 *        when they've seen a token separator and when they
 *        haven't.
 *
 *   <li> If you're using {@code StopFilter}, and the user will
 *        type "fast apple", but so far all they've typed is
 *        "fast a", again because the analyzer doesn't convey whether
 *        it's seen a token separator after the "a",
 *        {@code StopFilter} will remove that "a" causing
 *        far more matches than you'd expect.
 *
 *   <li> Lookups with the empty string return no results
 *        instead of all results.
 * </ul>
 * 
 * @lucene.experimental
 */
// redundant 'implements Accountable' to workaround javadocs bugs
public class AnalyzingSuggester extends Lookup implements Accountable {
 
  /**
   * FST&lt;Weight,Surface&gt;: 
   *  input is the analyzed form, with a null byte between terms
   *  weights are encoded as costs: (Integer.MAX_VALUE-weight)
   *  surface is the original, unanalyzed form.
   */
  private FST<Pair<Long,BytesRef>> fst = null;
  
  /** 
   * Analyzer that will be used for analyzing suggestions at
   * index time.
   */
  private final Analyzer indexAnalyzer;

  /** 
   * Analyzer that will be used for analyzing suggestions at
   * query time.
   */
  private final Analyzer queryAnalyzer;
  
  /** 
   * True if exact match suggestions should always be returned first.
   */
  private final boolean exactFirst;
  
  /** 
   * True if separator between tokens should be preserved.
   */
  private final boolean preserveSep;

  /** Include this flag in the options parameter to {@link
   *  #AnalyzingSuggester(Directory,String,Analyzer,Analyzer,int,int,int,boolean)} to always
   *  return the exact match first, regardless of score.  This
   *  has no performance impact but could result in
   *  low-quality suggestions. */
  public static final int EXACT_FIRST = 1;

  /** Include this flag in the options parameter to {@link
   *  #AnalyzingSuggester(Directory,String,Analyzer,Analyzer,int,int,int,boolean)} to preserve
   *  token separators when matching. */
  public static final int PRESERVE_SEP = 2;

  /** Represents the separation between tokens, if
   *  PRESERVE_SEP was specified */
  private static final int SEP_LABEL = '\u001F';

  /** Marks end of the analyzed input and start of dedup
   *  byte. */
  private static final int END_BYTE = 0x0;

  /** Maximum number of dup surface forms (different surface
   *  forms for the same analyzed form). */
  private final int maxSurfaceFormsPerAnalyzedForm;

  /** Maximum graph paths to index for a single analyzed
   *  surface form.  This only matters if your analyzer
   *  makes lots of alternate paths (e.g. contains
   *  SynonymFilter). */
  private final int maxGraphExpansions;

  private final Directory tempDir;
  private final String tempFileNamePrefix;

  /** Highest number of analyzed paths we saw for any single
   *  input surface form.  For analyzers that never create
   *  graphs this will always be 1. */
  private int maxAnalyzedPathsForOneInput;

  private boolean hasPayloads;

  private static final int PAYLOAD_SEP = '\u001f';

  /** Whether position holes should appear in the automaton. */
  private boolean preservePositionIncrements;

  /** Number of entries the lookup was built with */
  private long count = 0;

  /**
   * Calls {@link #AnalyzingSuggester(Directory,String,Analyzer,Analyzer,int,int,int,boolean)
   * AnalyzingSuggester(analyzer, analyzer, EXACT_FIRST |
   * PRESERVE_SEP, 256, -1, true)}
   */
  public AnalyzingSuggester(Directory tempDir, String tempFileNamePrefix, Analyzer analyzer) {
    this(tempDir, tempFileNamePrefix, analyzer, analyzer, EXACT_FIRST | PRESERVE_SEP, 256, -1, true);
  }

  /**
   * Calls {@link #AnalyzingSuggester(Directory,String,Analyzer,Analyzer,int,int,int,boolean)
   * AnalyzingSuggester(indexAnalyzer, queryAnalyzer, EXACT_FIRST |
   * PRESERVE_SEP, 256, -1, true)}
   */
  public AnalyzingSuggester(Directory tempDir, String tempFileNamePrefix, Analyzer indexAnalyzer, Analyzer queryAnalyzer) {
    this(tempDir, tempFileNamePrefix, indexAnalyzer, queryAnalyzer, EXACT_FIRST | PRESERVE_SEP, 256, -1, true);
  }

  /**
   * Creates a new suggester.
   * 
   * @param indexAnalyzer Analyzer that will be used for
   *   analyzing suggestions while building the index.
   * @param queryAnalyzer Analyzer that will be used for
   *   analyzing query text during lookup
   * @param options see {@link #EXACT_FIRST}, {@link #PRESERVE_SEP}
   * @param maxSurfaceFormsPerAnalyzedForm Maximum number of
   *   surface forms to keep for a single analyzed form.
   *   When there are too many surface forms we discard the
   *   lowest weighted ones.
   * @param maxGraphExpansions Maximum number of graph paths
   *   to expand from the analyzed form.  Set this to -1 for
   *   no limit.
   * @param preservePositionIncrements Whether position holes
   *   should appear in the automata
   */
  public AnalyzingSuggester(Directory tempDir, String tempFileNamePrefix, Analyzer indexAnalyzer, Analyzer queryAnalyzer, int options, int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions,
      boolean preservePositionIncrements) {
    this.indexAnalyzer = indexAnalyzer;
    this.queryAnalyzer = queryAnalyzer;
    if ((options & ~(EXACT_FIRST | PRESERVE_SEP)) != 0) {
      throw new IllegalArgumentException("options should only contain EXACT_FIRST and PRESERVE_SEP; got " + options);
    }
    this.exactFirst = (options & EXACT_FIRST) != 0;
    this.preserveSep = (options & PRESERVE_SEP) != 0;

    // NOTE: this is just an implementation limitation; if
    // somehow this is a problem we could fix it by using
    // more than one byte to disambiguate ... but 256 seems
    // like it should be way more then enough.
    if (maxSurfaceFormsPerAnalyzedForm <= 0 || maxSurfaceFormsPerAnalyzedForm > 256) {
      throw new IllegalArgumentException("maxSurfaceFormsPerAnalyzedForm must be > 0 and < 256 (got: " + maxSurfaceFormsPerAnalyzedForm + ")");
    }
    this.maxSurfaceFormsPerAnalyzedForm = maxSurfaceFormsPerAnalyzedForm;

    if (maxGraphExpansions < 1 && maxGraphExpansions != -1) {
      throw new IllegalArgumentException("maxGraphExpansions must -1 (no limit) or > 0 (got: " + maxGraphExpansions + ")");
    }
    this.maxGraphExpansions = maxGraphExpansions;
    this.preservePositionIncrements = preservePositionIncrements;
    this.tempDir = tempDir;
    this.tempFileNamePrefix = tempFileNamePrefix;
  }

  /** Returns byte size of the underlying FST. */
  @Override
  public long ramBytesUsed() {
    return fst == null ? 0 : fst.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    if (fst == null) {
      return Collections.emptyList();
    } else {
      return Collections.singletonList(Accountables.namedAccountable("fst", fst));
    }
  }

  // Replaces SEP with epsilon or remaps them if
  // we were asked to preserve them:
  private Automaton replaceSep(Automaton a) {

    int numStates = a.getNumStates();
    Automaton.Builder result = new Automaton.Builder(numStates, a.getNumTransitions());
    // Copy all states over
    result.copyStates(a);

    // Go in reverse topo sort so we know we only have to
    // make one pass:
    Transition t = new Transition();
    int[] topoSortStates = Operations.topoSortStates(a);
    for(int i=0;i<topoSortStates.length;i++) {
      int state = topoSortStates[topoSortStates.length-1-i];
      int count = a.initTransition(state, t);
      for(int j=0;j<count;j++) {
        a.getNextTransition(t);
        if (t.min == TokenStreamToAutomaton.POS_SEP) {
          assert t.max == TokenStreamToAutomaton.POS_SEP;
          if (preserveSep) {
            // Remap to SEP_LABEL:
            result.addTransition(state, t.dest, SEP_LABEL);
          } else {
            result.addEpsilon(state, t.dest);
          }
        } else if (t.min == TokenStreamToAutomaton.HOLE) {
          assert t.max == TokenStreamToAutomaton.HOLE;

          // Just remove the hole: there will then be two
          // SEP tokens next to each other, which will only
          // match another hole at search time.  Note that
          // it will also match an empty-string token ... if
          // that's somehow a problem we can always map HOLE
          // to a dedicated byte (and escape it in the
          // input).
          result.addEpsilon(state, t.dest);
        } else {
          result.addTransition(state, t.dest, t.min, t.max);
        }
      }
    }

    return result.finish();
  }

  /** Used by subclass to change the lookup automaton, if
   *  necessary. */
  protected Automaton convertAutomaton(Automaton a) {
    return a;
  }
  
  TokenStreamToAutomaton getTokenStreamToAutomaton() {
    final TokenStreamToAutomaton tsta = new TokenStreamToAutomaton();
    tsta.setPreservePositionIncrements(preservePositionIncrements);
    tsta.setFinalOffsetGapAsHole(true);
    return tsta;
  }
  
  private static class AnalyzingComparator implements Comparator<BytesRef> {

    private final boolean hasPayloads;

    public AnalyzingComparator(boolean hasPayloads) {
      this.hasPayloads = hasPayloads;
    }

    private final ByteArrayDataInput readerA = new ByteArrayDataInput();
    private final ByteArrayDataInput readerB = new ByteArrayDataInput();
    private final BytesRef scratchA = new BytesRef();
    private final BytesRef scratchB = new BytesRef();

    @Override
    public int compare(BytesRef a, BytesRef b) {

      // First by analyzed form:
      readerA.reset(a.bytes, a.offset, a.length);
      scratchA.length = readerA.readShort();
      scratchA.bytes = a.bytes;
      scratchA.offset = readerA.getPosition();

      readerB.reset(b.bytes, b.offset, b.length);
      scratchB.bytes = b.bytes;
      scratchB.length = readerB.readShort();
      scratchB.offset = readerB.getPosition();

      int cmp = scratchA.compareTo(scratchB);
      if (cmp != 0) {
        return cmp;
      }
      readerA.skipBytes(scratchA.length);
      readerB.skipBytes(scratchB.length);

      // Next by cost:
      long aCost = readerA.readInt();
      long bCost = readerB.readInt();
      assert decodeWeight(aCost) >= 0;
      assert decodeWeight(bCost) >= 0;
      if (aCost < bCost) {
        return -1;
      } else if (aCost > bCost) {
        return 1;
      }

      // Finally by surface form:
      if (hasPayloads) {
        scratchA.length = readerA.readShort();
        scratchB.length = readerB.readShort();
        scratchA.offset = readerA.getPosition();
        scratchB.offset = readerB.getPosition();
      } else {
        scratchA.offset = readerA.getPosition();
        scratchB.offset = readerB.getPosition();
        scratchA.length = readerA.length() - readerA.getPosition();
        scratchB.length = readerB.length() - readerB.getPosition();
      }
      assert scratchA.isValid();
      assert scratchB.isValid();
   
      return scratchA.compareTo(scratchB);
    }
  }

  @Override
  public void build(InputIterator iterator) throws IOException {
    if (iterator.hasContexts()) {
      throw new IllegalArgumentException("this suggester doesn't support contexts");
    }

    hasPayloads = iterator.hasPayloads();

    OfflineSorter sorter = new OfflineSorter(tempDir, tempFileNamePrefix, new AnalyzingComparator(hasPayloads));

    IndexOutput tempInput = tempDir.createTempOutput(tempFileNamePrefix, "input", IOContext.DEFAULT);

    OfflineSorter.ByteSequencesWriter writer = new OfflineSorter.ByteSequencesWriter(tempInput);
    OfflineSorter.ByteSequencesReader reader = null;
    BytesRefBuilder scratch = new BytesRefBuilder();

    TokenStreamToAutomaton ts2a = getTokenStreamToAutomaton();

    String tempSortedFileName = null;

    count = 0;
    byte buffer[] = new byte[8];
    try {
      ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);

      for (BytesRef surfaceForm; (surfaceForm = iterator.next()) != null;) {
        LimitedFiniteStringsIterator finiteStrings =
            new LimitedFiniteStringsIterator(toAutomaton(surfaceForm, ts2a), maxGraphExpansions);

        for (IntsRef string; (string = finiteStrings.next()) != null; count++) {
          Util.toBytesRef(string, scratch);
          
          // length of the analyzed text (FST input)
          if (scratch.length() > Short.MAX_VALUE-2) {
            throw new IllegalArgumentException("cannot handle analyzed forms > " + (Short.MAX_VALUE-2) + " in length (got " + scratch.length() + ")");
          }
          short analyzedLength = (short) scratch.length();

          // compute the required length:
          // analyzed sequence + weight (4) + surface + analyzedLength (short)
          int requiredLength = analyzedLength + 4 + surfaceForm.length + 2;

          BytesRef payload;

          if (hasPayloads) {
            if (surfaceForm.length > (Short.MAX_VALUE-2)) {
              throw new IllegalArgumentException("cannot handle surface form > " + (Short.MAX_VALUE-2) + " in length (got " + surfaceForm.length + ")");
            }
            payload = iterator.payload();
            // payload + surfaceLength (short)
            requiredLength += payload.length + 2;
          } else {
            payload = null;
          }
          
          buffer = ArrayUtil.grow(buffer, requiredLength);
          
          output.reset(buffer);

          output.writeShort(analyzedLength);

          output.writeBytes(scratch.bytes(), 0, scratch.length());

          output.writeInt(encodeWeight(iterator.weight()));

          if (hasPayloads) {
            for(int i=0;i<surfaceForm.length;i++) {
              if (surfaceForm.bytes[i] == PAYLOAD_SEP) {
                throw new IllegalArgumentException("surface form cannot contain unit separator character U+001F; this character is reserved");
              }
            }
            output.writeShort((short) surfaceForm.length);
            output.writeBytes(surfaceForm.bytes, surfaceForm.offset, surfaceForm.length);
            output.writeBytes(payload.bytes, payload.offset, payload.length);
          } else {
            output.writeBytes(surfaceForm.bytes, surfaceForm.offset, surfaceForm.length);
          }

          assert output.getPosition() == requiredLength: output.getPosition() + " vs " + requiredLength;
          writer.write(buffer, 0, output.getPosition());
        }

        maxAnalyzedPathsForOneInput = Math.max(maxAnalyzedPathsForOneInput, finiteStrings.size());
      }
      CodecUtil.writeFooter(tempInput);
      writer.close();

      // Sort all input/output pairs (required by FST.Builder):
      tempSortedFileName = sorter.sort(tempInput.getName());

      // Free disk space:
      tempDir.deleteFile(tempInput.getName());

      reader = new OfflineSorter.ByteSequencesReader(tempDir.openChecksumInput(tempSortedFileName, IOContext.READONCE), tempSortedFileName);
     
      PairOutputs<Long,BytesRef> outputs = new PairOutputs<>(PositiveIntOutputs.getSingleton(), ByteSequenceOutputs.getSingleton());
      Builder<Pair<Long,BytesRef>> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);

      // Build FST:
      BytesRefBuilder previousAnalyzed = null;
      BytesRefBuilder analyzed = new BytesRefBuilder();
      BytesRef surface = new BytesRef();
      IntsRefBuilder scratchInts = new IntsRefBuilder();
      ByteArrayDataInput input = new ByteArrayDataInput();

      // Used to remove duplicate surface forms (but we
      // still index the hightest-weight one).  We clear
      // this when we see a new analyzed form, so it cannot
      // grow unbounded (at most 256 entries):
      Set<BytesRef> seenSurfaceForms = new HashSet<>();

      int dedup = 0;
      while (true) {
        BytesRef bytes = reader.next();
        if (bytes == null) {
          break;
        }
        input.reset(bytes.bytes, bytes.offset, bytes.length);
        short analyzedLength = input.readShort();
        analyzed.grow(analyzedLength+2);
        input.readBytes(analyzed.bytes(), 0, analyzedLength);
        analyzed.setLength(analyzedLength);

        long cost = input.readInt();

        surface.bytes = bytes.bytes;
        if (hasPayloads) {
          surface.length = input.readShort();
          surface.offset = input.getPosition();
        } else {
          surface.offset = input.getPosition();
          surface.length = bytes.length - surface.offset;
        }
        
        if (previousAnalyzed == null) {
          previousAnalyzed = new BytesRefBuilder();
          previousAnalyzed.copyBytes(analyzed.get());
          seenSurfaceForms.add(BytesRef.deepCopyOf(surface));
        } else if (analyzed.get().equals(previousAnalyzed.get())) {
          dedup++;
          if (dedup >= maxSurfaceFormsPerAnalyzedForm) {
            // More than maxSurfaceFormsPerAnalyzedForm
            // dups: skip the rest:
            continue;
          }
          if (seenSurfaceForms.contains(surface)) {
            continue;
          }
          seenSurfaceForms.add(BytesRef.deepCopyOf(surface));
        } else {
          dedup = 0;
          previousAnalyzed.copyBytes(analyzed);
          seenSurfaceForms.clear();
          seenSurfaceForms.add(BytesRef.deepCopyOf(surface));
        }

        // TODO: I think we can avoid the extra 2 bytes when
        // there is no dup (dedup==0), but we'd have to fix
        // the exactFirst logic ... which would be sort of
        // hairy because we'd need to special case the two
        // (dup/not dup)...

        // NOTE: must be byte 0 so we sort before whatever
        // is next
        analyzed.append((byte) 0);
        analyzed.append((byte) dedup);

        Util.toIntsRef(analyzed.get(), scratchInts);
        //System.out.println("ADD: " + scratchInts + " -> " + cost + ": " + surface.utf8ToString());
        if (!hasPayloads) {
          builder.add(scratchInts.get(), outputs.newPair(cost, BytesRef.deepCopyOf(surface)));
        } else {
          int payloadOffset = input.getPosition() + surface.length;
          int payloadLength = bytes.length - payloadOffset;
          BytesRef br = new BytesRef(surface.length + 1 + payloadLength);
          System.arraycopy(surface.bytes, surface.offset, br.bytes, 0, surface.length);
          br.bytes[surface.length] = PAYLOAD_SEP;
          System.arraycopy(bytes.bytes, payloadOffset, br.bytes, surface.length+1, payloadLength);
          br.length = br.bytes.length;
          builder.add(scratchInts.get(), outputs.newPair(cost, br));
        }
      }
      fst = builder.finish();

      //Util.dotToFile(fst, "/tmp/suggest.dot");
    } finally {
      IOUtils.closeWhileHandlingException(reader, writer);
      IOUtils.deleteFilesIgnoringExceptions(tempDir, tempInput.getName(), tempSortedFileName);
    }
  }

  @Override
  public boolean store(DataOutput output) throws IOException {
    output.writeVLong(count);
    if (fst == null) {
      return false;
    }

    fst.save(output);
    output.writeVInt(maxAnalyzedPathsForOneInput);
    output.writeByte((byte) (hasPayloads ? 1 : 0));
    return true;
  }

  @Override
  public boolean load(DataInput input) throws IOException {
    count = input.readVLong();
    this.fst = new FST<>(input, new PairOutputs<>(PositiveIntOutputs.getSingleton(), ByteSequenceOutputs.getSingleton()));
    maxAnalyzedPathsForOneInput = input.readVInt();
    hasPayloads = input.readByte() == 1;
    return true;
  }

  private LookupResult getLookupResult(Long output1, BytesRef output2, CharsRefBuilder spare) {
    LookupResult result;
    if (hasPayloads) {
      int sepIndex = -1;
      for(int i=0;i<output2.length;i++) {
        if (output2.bytes[output2.offset+i] == PAYLOAD_SEP) {
          sepIndex = i;
          break;
        }
      }
      assert sepIndex != -1;
      spare.grow(sepIndex);
      final int payloadLen = output2.length - sepIndex - 1;
      spare.copyUTF8Bytes(output2.bytes, output2.offset, sepIndex);
      BytesRef payload = new BytesRef(payloadLen);
      System.arraycopy(output2.bytes, sepIndex+1, payload.bytes, 0, payloadLen);
      payload.length = payloadLen;
      result = new LookupResult(spare.toString(), decodeWeight(output1), payload);
    } else {
      spare.grow(output2.length);
      spare.copyUTF8Bytes(output2);
      result = new LookupResult(spare.toString(), decodeWeight(output1));
    }

    return result;
  }

  private boolean sameSurfaceForm(BytesRef key, BytesRef output2) {
    if (hasPayloads) {
      // output2 has at least PAYLOAD_SEP byte:
      if (key.length >= output2.length) {
        return false;
      }
      for(int i=0;i<key.length;i++) {
        if (key.bytes[key.offset+i] != output2.bytes[output2.offset+i]) {
          return false;
        }
      }
      return output2.bytes[output2.offset + key.length] == PAYLOAD_SEP;
    } else {
      return key.bytesEquals(output2);
    }
  }

  @Override
  public List<LookupResult> lookup(final CharSequence key, Set<BytesRef> contexts, boolean onlyMorePopular, int num) {
    assert num > 0;

    if (onlyMorePopular) {
      throw new IllegalArgumentException("this suggester only works with onlyMorePopular=false");
    }
    if (contexts != null) {
      throw new IllegalArgumentException("this suggester doesn't support contexts");
    }
    if (fst == null) {
      return Collections.emptyList();
    }

    //System.out.println("lookup key=" + key + " num=" + num);
    for (int i = 0; i < key.length(); i++) {
      if (key.charAt(i) == 0x1E) {
        throw new IllegalArgumentException("lookup key cannot contain HOLE character U+001E; this character is reserved");
      }
      if (key.charAt(i) == 0x1F) {
        throw new IllegalArgumentException("lookup key cannot contain unit separator character U+001F; this character is reserved");
      }
    }
    final BytesRef utf8Key = new BytesRef(key);
    try {
      Automaton lookupAutomaton = toLookupAutomaton(key);

      final CharsRefBuilder spare = new CharsRefBuilder();

      //System.out.println("  now intersect exactFirst=" + exactFirst);
    
      // Intersect automaton w/ suggest wFST and get all
      // prefix starting nodes & their outputs:
      //final PathIntersector intersector = getPathIntersector(lookupAutomaton, fst);

      //System.out.println("  prefixPaths: " + prefixPaths.size());

      BytesReader bytesReader = fst.getBytesReader();

      FST.Arc<Pair<Long,BytesRef>> scratchArc = new FST.Arc<>();

      final List<LookupResult> results = new ArrayList<>();

      List<FSTUtil.Path<Pair<Long,BytesRef>>> prefixPaths = FSTUtil.intersectPrefixPaths(convertAutomaton(lookupAutomaton), fst);

      if (exactFirst) {

        int count = 0;
        for (FSTUtil.Path<Pair<Long,BytesRef>> path : prefixPaths) {
          if (fst.findTargetArc(END_BYTE, path.fstNode, scratchArc, bytesReader) != null) {
            // This node has END_BYTE arc leaving, meaning it's an
            // "exact" match:
            count++;
          }
        }

        // Searcher just to find the single exact only
        // match, if present:
        Util.TopNSearcher<Pair<Long,BytesRef>> searcher;
        searcher = new Util.TopNSearcher<>(fst, count * maxSurfaceFormsPerAnalyzedForm, count * maxSurfaceFormsPerAnalyzedForm, weightComparator);

        // NOTE: we could almost get away with only using
        // the first start node.  The only catch is if
        // maxSurfaceFormsPerAnalyzedForm had kicked in and
        // pruned our exact match from one of these nodes
        // ...:
        for (FSTUtil.Path<Pair<Long,BytesRef>> path : prefixPaths) {
          if (fst.findTargetArc(END_BYTE, path.fstNode, scratchArc, bytesReader) != null) {
            // This node has END_BYTE arc leaving, meaning it's an
            // "exact" match:
            searcher.addStartPaths(scratchArc, fst.outputs.add(path.output, scratchArc.output), false, path.input);
          }
        }

        TopResults<Pair<Long,BytesRef>> completions = searcher.search();
        assert completions.isComplete;

        // NOTE: this is rather inefficient: we enumerate
        // every matching "exactly the same analyzed form"
        // path, and then do linear scan to see if one of
        // these exactly matches the input.  It should be
        // possible (though hairy) to do something similar
        // to getByOutput, since the surface form is encoded
        // into the FST output, so we more efficiently hone
        // in on the exact surface-form match.  Still, I
        // suspect very little time is spent in this linear
        // seach: it's bounded by how many prefix start
        // nodes we have and the
        // maxSurfaceFormsPerAnalyzedForm:
        for(Result<Pair<Long,BytesRef>> completion : completions) {
          BytesRef output2 = completion.output.output2;
          if (sameSurfaceForm(utf8Key, output2)) {
            results.add(getLookupResult(completion.output.output1, output2, spare));
            break;
          }
        }

        if (results.size() == num) {
          // That was quick:
          return results;
        }
      }

      Util.TopNSearcher<Pair<Long,BytesRef>> searcher;
      searcher = new Util.TopNSearcher<Pair<Long,BytesRef>>(fst,
                                                            num - results.size(),
                                                            num * maxAnalyzedPathsForOneInput,
                                                            weightComparator) {
        private final Set<BytesRef> seen = new HashSet<>();

        @Override
        protected boolean acceptResult(IntsRef input, Pair<Long,BytesRef> output) {

          // Dedup: when the input analyzes to a graph we
          // can get duplicate surface forms:
          if (seen.contains(output.output2)) {
            return false;
          }
          seen.add(output.output2);
          
          if (!exactFirst) {
            return true;
          } else {
            // In exactFirst mode, don't accept any paths
            // matching the surface form since that will
            // create duplicate results:
            if (sameSurfaceForm(utf8Key, output.output2)) {
              // We found exact match, which means we should
              // have already found it in the first search:
              assert results.size() == 1;
              return false;
            } else {
              return true;
            }
          }
        }
      };

      prefixPaths = getFullPrefixPaths(prefixPaths, lookupAutomaton, fst);
      
      for (FSTUtil.Path<Pair<Long,BytesRef>> path : prefixPaths) {
        searcher.addStartPaths(path.fstNode, path.output, true, path.input);
      }

      TopResults<Pair<Long,BytesRef>> completions = searcher.search();
      assert completions.isComplete;

      for(Result<Pair<Long,BytesRef>> completion : completions) {

        LookupResult result = getLookupResult(completion.output.output1, completion.output.output2, spare);

        // TODO: for fuzzy case would be nice to return
        // how many edits were required

        //System.out.println("    result=" + result);
        results.add(result);

        if (results.size() == num) {
          // In the exactFirst=true case the search may
          // produce one extra path
          break;
        }
      }

      return results;
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
  }
  
  @Override
  public long getCount() {
    return count;
  }

  /** Returns all prefix paths to initialize the search. */
  protected List<FSTUtil.Path<Pair<Long,BytesRef>>> getFullPrefixPaths(List<FSTUtil.Path<Pair<Long,BytesRef>>> prefixPaths,
                                                                       Automaton lookupAutomaton,
                                                                       FST<Pair<Long,BytesRef>> fst)
    throws IOException {
    return prefixPaths;
  }
  
  final Automaton toAutomaton(final BytesRef surfaceForm, final TokenStreamToAutomaton ts2a) throws IOException {
    // Analyze surface form:
    Automaton automaton;
    try (TokenStream ts = indexAnalyzer.tokenStream("", surfaceForm.utf8ToString())) {

      // Create corresponding automaton: labels are bytes
      // from each analyzed token, with byte 0 used as
      // separator between tokens:
      automaton = ts2a.toAutomaton(ts);
    }

    automaton = replaceSep(automaton);
    automaton = convertAutomaton(automaton);

    // TODO: LUCENE-5660 re-enable this once we disallow massive suggestion strings
    // assert SpecialOperations.isFinite(automaton);

    // Get all paths from the automaton (there can be
    // more than one path, eg if the analyzer created a
    // graph using SynFilter or WDF):
    return automaton;
  }

  final Automaton toLookupAutomaton(final CharSequence key) throws IOException {
    // TODO: is there a Reader from a CharSequence?
    // Turn tokenstream into automaton:
    Automaton automaton = null;
    try (TokenStream ts = queryAnalyzer.tokenStream("", key.toString())) {
      automaton = getTokenStreamToAutomaton().toAutomaton(ts);
    }

    automaton = replaceSep(automaton);

    // TODO: we can optimize this somewhat by determinizing
    // while we convert
    automaton = Operations.determinize(automaton, DEFAULT_MAX_DETERMINIZED_STATES);
    return automaton;
  }

  /**
   * Returns the weight associated with an input string,
   * or null if it does not exist.
   */
  public Object get(CharSequence key) {
    throw new UnsupportedOperationException();
  }
  
  /** cost -&gt; weight */
  private static int decodeWeight(long encoded) {
    return (int)(Integer.MAX_VALUE - encoded);
  }
  
  /** weight -&gt; cost */
  private static int encodeWeight(long value) {
    if (value < 0 || value > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("cannot encode value: " + value);
    }
    return Integer.MAX_VALUE - (int)value;
  }
   
  static final Comparator<Pair<Long,BytesRef>> weightComparator = new Comparator<Pair<Long,BytesRef>> () {
    @Override
    public int compare(Pair<Long,BytesRef> left, Pair<Long,BytesRef> right) {
      return left.output1.compareTo(right.output1);
    }
  };
}
