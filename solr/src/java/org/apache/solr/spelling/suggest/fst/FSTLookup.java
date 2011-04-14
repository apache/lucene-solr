package org.apache.solr.spelling.suggest.fst;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.fst.Builder;
import org.apache.lucene.util.automaton.fst.FST;
import org.apache.lucene.util.automaton.fst.FST.Arc;
import org.apache.lucene.util.automaton.fst.NoOutputs;
import org.apache.lucene.util.automaton.fst.Outputs;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.spelling.suggest.Lookup;
import org.apache.solr.spelling.suggest.tst.TSTLookup;
import org.apache.solr.util.TermFreqIterator;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

/**
 * Finite state automata based implementation of {@link Lookup} query 
 * suggestion/ autocomplete interface.
 * 
 * <h2>Implementation details</h2> 
 * 
 * <p>The construction step in {@link #build(TermFreqIterator)} works as follows:
 * <ul>
 * <li>A set of input terms (String) and weights (float) is given.</li>
 * <li>The range of weights is determined and then all weights are discretized into a fixed set 
 * of values ({@link #buckets}).
 * Note that this means that minor changes in weights may be lost during automaton construction. 
 * In general, this is not a big problem because the "priorities" of completions can be split
 * into a fixed set of classes (even as rough as: very frequent, frequent, baseline, marginal).
 * If you need exact, fine-grained weights, use {@link TSTLookup} instead.<li>
 * <li>All terms in the input are preprended with a synthetic pseudo-character being the weight
 * of that term. For example a term <code>abc</code> with a discretized weight equal '1' would
 * become <code>1abc</code>.</li> 
 * <li>The terms are sorted by their raw value of utf16 character values (including the synthetic
 * term in front).</li>
 * <li>A finite state automaton ({@link FST}) is constructed from the input. The root node has
 * arcs labeled with all possible weights. We cache all these arcs, highest-weight first.</li>   
 * </ul>
 * 
 * <p>At runtime, in {@link #lookup(String, boolean, int)}, the automaton is utilized as follows:
 * <ul>
 * <li>For each possible term weight encoded in the automaton (cached arcs from the root above), 
 * starting with the highest one, we descend along the path of the input key. If the key is not
 * a prefix of a sequence in the automaton (path ends prematurely), we exit immediately. 
 * No completions.
 * <li>Otherwise, we have found an internal automaton node that ends the key. <b>The entire
 * subautomaton (all paths) starting from this node form the key's completions.</b> We start
 * the traversal of this subautomaton. Every time we reach a final state (arc), we add a single
 * suggestion to the list of results (the weight of this suggestion is constant and equal to the
 * root path we started from). The tricky part is that because automaton edges are sorted and
 * we scan depth-first, we can terminate the entire procedure as soon as we collect enough 
 * suggestions the user requested.
 * <li>In case the number of suggestions collected in the step above is still insufficient,
 * we proceed to the next (smaller) weight leaving the root node and repeat the same 
 * algorithm again. 
 * </li>
 * </ul>
 *  
 * <h2>Runtime behavior and performance characteristic</h2>
 * 
 * <p>The algorithm described above is optimized for finding suggestions to short prefixes
 * in a top-weights-first order. This is probably the most common use case: it allows 
 * presenting suggestions early and sorts them by the global frequency (and then alphabetically).
 * 
 * <p>If there is an exact match in the automaton, it is returned first on the results
 * list (even with by-weight sorting).
 * 
 * <p>Note that the maximum lookup time for <b>any prefix</b>
 * is the time of descending to the subtree, plus traversal of the subtree up to the number
 * of requested suggestions (because they are already presorted by weight on the root level
 * and alphabetically at any node level).
 * 
 * <p>To order alphabetically only (no ordering by priorities), use identical term weights 
 * for all terms. Alphabetical suggestions are returned even if non-constant weights are
 * used, but the algorithm for doing this is suboptimal.  
 * 
 * <p>"alphabetically" in any of the documentation above indicates utf16 codepoint order, 
 * nothing else.
 */
public class FSTLookup extends Lookup {
  /** A structure for a single entry (for sorting/ preprocessing). */
  private static class Entry {
    char [] term;
    float weight;

    public Entry(char [] term, float freq) {
      this.term = term;
      this.weight = freq;
    }
  }

  /**
   * The number of separate buckets for weights (discretization). The more buckets,
   * the more fine-grained term weights (priorities) can be assigned. The speed of lookup
   * will not decrease for prefixes which have highly-weighted completions (because these
   * are filled-in first), but will decrease significantly for low-weighted terms (but
   * these should be infrequent, so it is all right).
   * 
   * <p>The number of buckets must be within [1, 255] range.
   */
  public static final String WEIGHT_BUCKETS = "weightBuckets";

  /**
   * If <code>true</code>, exact suggestions are returned first, even if they are prefixes
   * of other strings in the automaton (possibly with larger weights). 
   */
  public static final String EXACT_MATCH_FIRST = "exactMatchFirst";

  /** Serialized automaton file name (storage). */
  public static final String FILENAME = "fst.dat";

  /** An empty result. */
  private static final List<LookupResult> EMPTY_RESULT = Lists.newArrayList();

  /**
   * @see #WEIGHT_BUCKETS
   */
  private int buckets = 10;

  /**
   * #see #EXACT_MATCH_FIRST
   */
  private boolean exactMatchFirst = true;

  /**
   * Finite state automaton encoding all the lookup terms. See class
   * notes for details.
   */
  private FST<Object> automaton;

  /**
   * An array of arcs leaving the root automaton state and encoding weights of all
   * completions in their sub-trees.
   */
  private Arc<Object> [] rootArcs;

  /* */
  @Override
  @SuppressWarnings("rawtypes")
  public void init(NamedList config, SolrCore core) {
    this.buckets = config.get(WEIGHT_BUCKETS) != null
      ? Integer.parseInt(config.get(WEIGHT_BUCKETS).toString())
      : 10;

    this.exactMatchFirst = config.get(EXACT_MATCH_FIRST) != null
      ? Boolean.valueOf(config.get(EXACT_MATCH_FIRST).toString())
      : true;
  }

  /* */
  @Override
  public void build(TermFreqIterator tfit) throws IOException {
    // Buffer the input because we will need it twice: for calculating
    // weights distribution and for the actual automata building.
    List<Entry> entries = Lists.newArrayList();
    while (tfit.hasNext()) {
      String term = tfit.next();
      char [] termChars = new char [term.length() + 1]; // add padding for weight.
      for (int i = 0; i < term.length(); i++)
        termChars[i + 1] = term.charAt(i);
      entries.add(new Entry(termChars, tfit.freq()));
    }

    // Distribute weights into at most N buckets. This is a form of discretization to
    // limit the number of possible weights so that they can be efficiently encoded in the
    // automaton.
    //
    // It is assumed the distribution of weights is _linear_ so proportional division 
    // of [min, max] range will be enough here. Other approaches could be to sort 
    // weights and divide into proportional ranges.
    if (entries.size() > 0) {
      redistributeWeightsProportionalMinMax(entries, buckets);
      encodeWeightPrefix(entries);
    }

    // Build the automaton (includes input sorting) and cache root arcs in order from the highest,
    // to the lowest weight.
    this.automaton = buildAutomaton(entries);
    cacheRootArcs();
  }

  /**
   * Cache the root node's output arcs starting with completions with the highest weights.
   */
  @SuppressWarnings("unchecked")
  private void cacheRootArcs() throws IOException {
    if (automaton != null) {
      List<Arc<Object>> rootArcs = Lists.newArrayList();
      Arc<Object> arc = automaton.getFirstArc(new Arc<Object>());
      automaton.readFirstTargetArc(arc, arc);
      while (true) {
        rootArcs.add(new Arc<Object>().copyFrom(arc));
        if (arc.isLast())
          break;
        automaton.readNextArc(arc);
      }

      Collections.reverse(rootArcs); // we want highest weights first.
      this.rootArcs = rootArcs.toArray(new Arc[rootArcs.size()]);
    }    
  }

  /**
   * Not implemented.
   */
  @Override
  public boolean add(String key, Object value) {
    // This implementation does not support ad-hoc additions (all input
    // must be sorted for the builder).
    return false;
  }

  /**
   * Get the (approximated) weight of a single key (if there is a perfect match
   * for it in the automaton). 
   * 
   * @return Returns the approximated weight of the input key or <code>null</code>
   * if not found.
   */
  @Override
  public Float get(String key) {
    return getExactMatchStartingFromRootArc(0, key);
  }

  /**
   * Returns the first exact match by traversing root arcs, starting from 
   * the arc <code>i</code>.
   * 
   * @param i The first root arc index in {@link #rootArcs} to consider when
   * matching. 
   */
  private Float getExactMatchStartingFromRootArc(int i, String key) {
    // Get the UTF-8 bytes representation of the input key. 
    try {
      final FST.Arc<Object> scratch = new FST.Arc<Object>();
      for (; i < rootArcs.length; i++) {
        final FST.Arc<Object> rootArc = rootArcs[i];
        final FST.Arc<Object> arc = scratch.copyFrom(rootArc);

        // Descend into the automaton using the key as prefix.
        if (descendWithPrefix(arc, key)) {
          automaton.readFirstTargetArc(arc, arc);
          if (arc.label == FST.END_LABEL) {
            // Prefix-encoded weight.
            return rootArc.label / (float) buckets;
          }
        }
      }
    } catch (IOException e) {
      // Should never happen, but anyway.
      throw new RuntimeException(e);
    }
    
    return null;
  }

  /**
   * Lookup autocomplete suggestions to <code>key</code>.
   *  
   * @param key The prefix to which suggestions should be sought. 
   * @param onlyMorePopular Return most popular suggestions first. This is the default
   * behavior for this implementation. Setting it to <code>false</code> has no effect (use
   * constant term weights to sort alphabetically only). 
   * @param num At most this number of suggestions will be returned.
   * @return Returns the suggestions, sorted by their approximated weight first (decreasing)
   * and then alphabetically (utf16 codepoint order).
   */
  @Override
  public List<LookupResult> lookup(String key, boolean onlyMorePopular, int num) {
    if (key.length() == 0 || automaton == null) {
      // Keep the result an ArrayList to keep calls monomorphic.
      return EMPTY_RESULT; 
    }
    
    try {
      if (!onlyMorePopular && rootArcs.length > 1) {
        // We could emit a warning here (?). An optimal strategy for alphabetically sorted
        // suggestions would be to add them with a constant weight -- this saves unnecessary
        // traversals and sorting.
        return lookupSortedAlphabetically(key, num);
      } else {
        return lookupSortedByWeight(key, num, true);
      }
    } catch (IOException e) {
      // Should never happen, but anyway.
      throw new RuntimeException(e);
    }
  }

  /**
   * Lookup suggestions sorted alphabetically <b>if weights are not constant</b>. This
   * is a workaround: in general, use constant weights for alphabetically sorted result.
   */
  private List<LookupResult> lookupSortedAlphabetically(String key, int num) throws IOException {
    // Greedily get num results from each weight branch.
    List<LookupResult> res = lookupSortedByWeight(key, num, false);
    
    // Sort and trim.
    Collections.sort(res, new Comparator<LookupResult>() {
      @Override
      public int compare(LookupResult o1, LookupResult o2) {
        return o1.key.compareTo(o2.key);
      }
    });
    if (res.size() > num) {
      res = res.subList(0, num);
    }
    return res;
  }

  /**
   * Lookup suggestions sorted by weight (descending order).
   * 
   * @param greedy If <code>true</code>, the routine terminates immediately when <code>num</code>
   * suggestions have been collected. If <code>false</code>, it will collect suggestions from
   * all weight arcs (needed for {@link #lookupSortedAlphabetically}.
   */
  private ArrayList<LookupResult> lookupSortedByWeight(String key, int num, boolean greedy) throws IOException {
    final ArrayList<LookupResult> res = new ArrayList<LookupResult>(Math.min(10, num));
    final StringBuilder output = new StringBuilder(key);
    final int matchLength = key.length() - 1;
    
    for (int i = 0; i < rootArcs.length; i++) {
      final FST.Arc<Object> rootArc = rootArcs[i];
      final FST.Arc<Object> arc = new FST.Arc<Object>().copyFrom(rootArc);

      // Descend into the automaton using the key as prefix.
      if (descendWithPrefix(arc, key)) {
        // Prefix-encoded weight.
        final float weight = rootArc.label / (float) buckets;

        // A subgraph starting from the current node has the completions 
        // of the key prefix. The arc we're at is the last key's byte,
        // so we will collect it too.
        output.setLength(matchLength);
        if (collect(res, num, weight, output, arc) && greedy) {
          // We have enough suggestion to return immediately. Keep on looking for an
          // exact match, if requested.
          if (exactMatchFirst) {
            Float exactMatchWeight = getExactMatchStartingFromRootArc(i, key);
            if (exactMatchWeight != null) {
              res.add(0, new LookupResult(key, exactMatchWeight));
              while (res.size() > num) {
                res.remove(res.size() - 1);
              }
            }
          }
          break;
        }
      }
    }
    return res;
  }

  /**
   * Descend along the path starting at <code>arc</code> and going through
   * bytes in <code>utf8</code> argument.
   *  
   * @param arc The starting arc. This argument is modified in-place.
   * @param term The term to descend with.
   * @return If <code>true</code>, <code>arc</code> will be set to the arc matching
   * last byte of <code>utf8</code>. <code>false</code> is returned if no such 
   * prefix <code>utf8</code> exists.
   */
  private boolean descendWithPrefix(Arc<Object> arc, String term) throws IOException {
    final int max = term.length();

    for (int i = 0; i < max; i++) {
      if (automaton.findTargetArc(term.charAt(i) & 0xffff, arc, arc) == null) {
        // No matching prefixes, return an empty result.
        return false;
      }
    }

    return true;
  }

  /**
   * Recursive collect lookup results from the automaton subgraph starting at <code>arc</code>.
   * 
   * @param num Maximum number of results needed (early termination).
   * @param weight Weight of all results found during this collection.
   */
  private boolean collect(List<LookupResult> res, int num, float weight, StringBuilder output, Arc<Object> arc) throws IOException {
    output.append((char) arc.label);

    automaton.readFirstTargetArc(arc, arc);
    while (true) {
      if (arc.label == FST.END_LABEL) {
        res.add(new LookupResult(output.toString(), weight));
        if (res.size() >= num)
          return true;
      } else {
        int save = output.length();
        if (collect(res, num, weight, output, new Arc<Object>().copyFrom(arc))) {
          return true;
        }
        output.setLength(save);
      }

      if (arc.isLast()) {
        break;
      }
      automaton.readNextArc(arc);        
    }
    return false;
  }

  /**
   * Builds the final automaton from a list of entries. 
   */
  private FST<Object> buildAutomaton(List<Entry> entries) throws IOException {
    if (entries.size() == 0)
      return null;
    
    // Sort by utf16 (raw char value)
    final Comparator<Entry> comp = new Comparator<Entry>() {
      public int compare(Entry o1, Entry o2) {
        char [] ch1 = o1.term;
        char [] ch2 = o2.term;
        int len1 = ch1.length;
        int len2 = ch2.length;

        int max = Math.min(len1, len2);
        for (int i = 0; i < max; i++) {
          int v = ch1[i] - ch2[i];
          if (v != 0) return v;
        }
        return len1 - len2;
      }
    };
    Collections.sort(entries, comp);

    // Avoid duplicated identical entries, if possible. This is required because
    // it breaks automaton construction otherwise.
    int len = entries.size();
    int j = 0;
    for (int i = 1; i < len; i++) {
      if (comp.compare(entries.get(j), entries.get(i)) != 0) {
        entries.set(++j, entries.get(i));
      }
    }
    entries = entries.subList(0, j + 1);

    // Build the automaton.
    final Outputs<Object> outputs = NoOutputs.getSingleton();
    final Object empty = outputs.getNoOutput();
    final Builder<Object> builder = 
      new Builder<Object>(FST.INPUT_TYPE.BYTE4, 0, 0, true, outputs);
    final IntsRef scratchIntsRef = new IntsRef(10);
    for (Entry e : entries) {
      final int termLength = scratchIntsRef.length = e.term.length;

      scratchIntsRef.grow(termLength);
      final int [] ints = scratchIntsRef.ints;
      final char [] chars = e.term;
      for (int i = termLength; --i >= 0;) {
        ints[i] = chars[i];
      }
      builder.add(scratchIntsRef, empty);
    }
    return builder.finish();
  }

  /**
   * Prepends the entry's weight to each entry, encoded as a single byte, so that the
   * root automaton node fans out to all possible priorities, starting with the arc that has
   * the highest weights.     
   */
  private void encodeWeightPrefix(List<Entry> entries) {
    for (Entry e : entries) {
      int weight = (int) e.weight;
      assert (weight >= 0 && weight <= buckets) : 
        "Weight out of range: " + weight + " [" + buckets + "]";
  
      // There should be a single empty char reserved in front for the weight.
      e.term[0] = (char) weight;
    }
  }

  /**
   *  Split [min, max] range into buckets, reassigning weights. Entries' weights are
   *  remapped to [0, buckets] range (so, buckets + 1 buckets, actually).
   */
  private void redistributeWeightsProportionalMinMax(List<Entry> entries, int buckets) {
    float min = entries.get(0).weight;
    float max = min;
    for (Entry e : entries) {
      min = Math.min(e.weight, min);
      max = Math.max(e.weight, max);
    }
  
    final float range = max - min;
    for (Entry e : entries) {
      e.weight = (int) (buckets * ((e.weight - min) / range)); // int cast equiv. to floor()
    }
  }

  /**
   * Deserialization from disk.
   */
  @Override
  public synchronized boolean load(File storeDir) throws IOException {
    File data = new File(storeDir, FILENAME);
    if (!data.exists() || !data.canRead()) {
      return false;
    }

    InputStream is = new BufferedInputStream(new FileInputStream(data));
    try {
      this.automaton = new FST<Object>(new InputStreamDataInput(is), NoOutputs.getSingleton());
      cacheRootArcs();
    } finally {
      Closeables.closeQuietly(is);
    }
    return true;
  }

  /**
   * Serialization to disk.
   */
  @Override
  public synchronized boolean store(File storeDir) throws IOException {
    if (!storeDir.exists() || !storeDir.isDirectory() || !storeDir.canWrite()) {
      return false;
    }

    if (this.automaton == null)
      return false;

    File data = new File(storeDir, FILENAME);
    OutputStream os = new BufferedOutputStream(new FileOutputStream(data));
    try {
      this.automaton.save(new OutputStreamDataOutput(os));
    } finally {
      Closeables.closeQuietly(os);
    }

    return true;
  }
}
