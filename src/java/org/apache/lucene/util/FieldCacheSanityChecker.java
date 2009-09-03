package org.apache.lucene.util;
/**
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldCache.CacheEntry;

/** 
 * Provides methods for sanity checking that entries in the FieldCache 
 * are not wasteful or inconsistent.
 * </p>
 * <p>
 * Lucene 2.9 Introduced numerous enhancements into how the FieldCache 
 * is used by the low levels of Lucene searching (for Sorting and 
 * ValueSourceQueries) to improve both the speed for Sorting, as well 
 * as reopening of IndexReaders.  But these changes have shifted the 
 * usage of FieldCache from "top level" IndexReaders (frequently a 
 * MultiReader or DirectoryReader) down to the leaf level SegmentReaders.  
 * As a result, existing applications that directly access the FieldCache 
 * may find RAM usage increase significantly when upgrading to 2.9 or 
 * Later.  This class provides an API for these applications (or their 
 * Unit tests) to check at run time if the FieldCache contains "insane" 
 * usages of the FieldCache.
 * </p>
 * <p>
 * <b>EXPERIMENTAL API:</b> This API is considered extremely advanced and 
 * experimental.  It may be removed or altered w/o warning in future releases 
 * of Lucene.
 * </p>
 * @see FieldCache
 * @see FieldCacheSanityChecker.Insanity
 * @see FieldCacheSanityChecker.InsanityType
 */
public final class FieldCacheSanityChecker {

  private RamUsageEstimator ramCalc = null;
  public FieldCacheSanityChecker() {
    /* NOOP */
  }
  /**
   * If set, will be used to estimate size for all CacheEntry objects 
   * dealt with.
   */
  public void setRamUsageEstimator(RamUsageEstimator r) {
    ramCalc = r;
  }


  /** 
   * Quick and dirty convenience method
   * @see #check
   */
  public static Insanity[] checkSanity(FieldCache cache) {
    return checkSanity(cache.getCacheEntries());
  }

  /** 
   * Quick and dirty convenience method that instantiates an instance with 
   * "good defaults" and uses it to test the CacheEntry[]
   * @see #check
   */
  public static Insanity[] checkSanity(CacheEntry[] cacheEntries) {
    FieldCacheSanityChecker sanityChecker = new FieldCacheSanityChecker();
    // doesn't check for interned
    sanityChecker.setRamUsageEstimator(new RamUsageEstimator(false));
    return sanityChecker.check(cacheEntries);
  }


  /**
   * Tests a CacheEntry[] for indication of "insane" cache usage.
   * <p>
   * <B>NOTE:</b>FieldCache CreationPlaceholder objects are ignored.
   * (:TODO: is this a bad idea? are we masking a real problem?)
   * </p>
   */
  public Insanity[] check(CacheEntry[] cacheEntries) {
    if (null == cacheEntries || 0 == cacheEntries.length) 
      return new Insanity[0];

    if (null != ramCalc) {
      for (int i = 0; i < cacheEntries.length; i++) {
        cacheEntries[i].estimateSize(ramCalc);
      }
    }

    // the indirect mapping lets MapOfSet dedup identical valIds for us
    //
    // maps the (valId) identityhashCode of cache values to 
    // sets of CacheEntry instances
    final MapOfSets valIdToItems = new MapOfSets(new HashMap(17));
    // maps ReaderField keys to Sets of ValueIds
    final MapOfSets readerFieldToValIds = new MapOfSets(new HashMap(17));
    //

    // any keys that we know result in more then one valId
    final Set valMismatchKeys = new HashSet();

    // iterate over all the cacheEntries to get the mappings we'll need
    for (int i = 0; i < cacheEntries.length; i++) {
      final CacheEntry item = cacheEntries[i];
      final Object val = item.getValue();

      if (val instanceof FieldCache.CreationPlaceholder)
        continue;

      final ReaderField rf = new ReaderField(item.getReaderKey(), 
                                            item.getFieldName());

      final Integer valId = new Integer(System.identityHashCode(val));

      // indirect mapping, so the MapOfSet will dedup identical valIds for us
      valIdToItems.put(valId, item);
      if (1 < readerFieldToValIds.put(rf, valId)) {
        valMismatchKeys.add(rf);
      }
    }

    final List insanity = new ArrayList(valMismatchKeys.size() * 3);

    insanity.addAll(checkValueMismatch(valIdToItems, 
                                       readerFieldToValIds, 
                                       valMismatchKeys));
    insanity.addAll(checkSubreaders(valIdToItems, 
                                    readerFieldToValIds));
                    
    return (Insanity[]) insanity.toArray(new Insanity[insanity.size()]);
  }

  /** 
   * Internal helper method used by check that iterates over 
   * valMismatchKeys and generates a Collection of Insanity 
   * instances accordingly.  The MapOfSets are used to populate 
   * the Insanity objects. 
   * @see InsanityType#VALUEMISMATCH
   */
  private Collection checkValueMismatch(MapOfSets valIdToItems,
                                        MapOfSets readerFieldToValIds,
                                        Set valMismatchKeys) {

    final List insanity = new ArrayList(valMismatchKeys.size() * 3);

    if (! valMismatchKeys.isEmpty() ) { 
      // we have multiple values for some ReaderFields

      final Map rfMap = readerFieldToValIds.getMap();
      final Map valMap = valIdToItems.getMap();
      final Iterator mismatchIter = valMismatchKeys.iterator();
      while (mismatchIter.hasNext()) {
        final ReaderField rf = (ReaderField)mismatchIter.next();
        final List badEntries = new ArrayList(valMismatchKeys.size() * 2);
        final Iterator valIter = ((Set)rfMap.get(rf)).iterator();
        while (valIter.hasNext()) {
          Iterator entriesIter = ((Set)valMap.get(valIter.next())).iterator();
          while (entriesIter.hasNext()) {
            badEntries.add(entriesIter.next());
          }
        }

        CacheEntry[] badness = new CacheEntry[badEntries.size()];
        badness = (CacheEntry[]) badEntries.toArray(badness);

        insanity.add(new Insanity(InsanityType.VALUEMISMATCH,
                                  "Multiple distinct value objects for " + 
                                  rf.toString(), badness));
      }
    }
    return insanity;
  }

  /** 
   * Internal helper method used by check that iterates over 
   * the keys of readerFieldToValIds and generates a Collection 
   * of Insanity instances whenever two (or more) ReaderField instances are 
   * found that have an ancestry relationships.  
   *
   * @see InsanityType#SUBREADER
   */
  private Collection checkSubreaders(MapOfSets valIdToItems,
                                     MapOfSets readerFieldToValIds) {

    final List insanity = new ArrayList(23);

    Map badChildren = new HashMap(17);
    MapOfSets badKids = new MapOfSets(badChildren); // wrapper

    Map viToItemSets = valIdToItems.getMap();
    Map rfToValIdSets = readerFieldToValIds.getMap();

    Set seen = new HashSet(17);

    Set readerFields = rfToValIdSets.keySet();
    Iterator rfIter = readerFields.iterator();
    while (rfIter.hasNext()) {
      ReaderField rf = (ReaderField) rfIter.next();
      
      if (seen.contains(rf)) continue;

      List kids = getAllDecendentReaderKeys(rf.readerKey);
      for (int i = 0; i < kids.size(); i++) {
        ReaderField kid = new ReaderField(kids.get(i), rf.fieldName);
        
        if (badChildren.containsKey(kid)) {
          // we've already process this kid as RF and found other problems
          // track those problems as our own
          badKids.put(rf, kid);
          badKids.putAll(rf, (Collection)badChildren.get(kid));
          badChildren.remove(kid);
          
        } else if (rfToValIdSets.containsKey(kid)) {
          // we have cache entries for the kid
          badKids.put(rf, kid);
        }
        seen.add(kid);
      }
      seen.add(rf);
    }

    // every mapping in badKids represents an Insanity
    Iterator parentsIter = badChildren.keySet().iterator();
    while (parentsIter.hasNext()) {
      ReaderField parent = (ReaderField) parentsIter.next();
      Set kids = (Set) badChildren.get(parent);

      List badEntries = new ArrayList(kids.size() * 2);

      // put parent entr(ies) in first
      {
        Iterator valIter =((Set)rfToValIdSets.get(parent)).iterator();
        while (valIter.hasNext()) {
          badEntries.addAll((Set)viToItemSets.get(valIter.next()));
        }
      }

      // now the entries for the descendants
      Iterator kidsIter = kids.iterator();
      while (kidsIter.hasNext()) {
        ReaderField kid = (ReaderField) kidsIter.next();
        Iterator valIter =((Set)rfToValIdSets.get(kid)).iterator();
        while (valIter.hasNext()) {
          badEntries.addAll((Set)viToItemSets.get(valIter.next()));
        }
      }

      CacheEntry[] badness = new CacheEntry[badEntries.size()];
      badness = (CacheEntry[]) badEntries.toArray(badness);

      insanity.add(new Insanity(InsanityType.SUBREADER,
                                "Found caches for decendents of " + 
                                parent.toString(),
                                badness));
    }

    return insanity;

  }

  /**
   * Checks if the seed is an IndexReader, and if so will walk
   * the hierarchy of subReaders building up a list of the objects 
   * returned by obj.getFieldCacheKey()
   */
  private List getAllDecendentReaderKeys(Object seed) {
    List all = new ArrayList(17); // will grow as we iter
    all.add(seed);
    for (int i = 0; i < all.size(); i++) {
      Object obj = all.get(i);
      if (obj instanceof IndexReader) {
        IndexReader[] subs = ((IndexReader)obj).getSequentialSubReaders();
        for (int j = 0; (null != subs) && (j < subs.length); j++) {
          all.add(subs[j].getFieldCacheKey());
        }
      }
      
    }
    // need to skip the first, because it was the seed
    return all.subList(1, all.size());
  }

  /**
   * Simple pair object for using "readerKey + fieldName" a Map key
   */
  private final static class ReaderField {
    public final Object readerKey;
    public final String fieldName;
    public ReaderField(Object readerKey, String fieldName) {
      this.readerKey = readerKey;
      this.fieldName = fieldName;
    }
    public int hashCode() {
      return System.identityHashCode(readerKey) * fieldName.hashCode();
    }
    public boolean equals(Object that) {
      if (! (that instanceof ReaderField)) return false;

      ReaderField other = (ReaderField) that;
      return (this.readerKey == other.readerKey &&
              this.fieldName.equals(other.fieldName));
    }
    public String toString() {
      return readerKey.toString() + "+" + fieldName;
    }
  }

  /**
   * Simple container for a collection of related CacheEntry objects that 
   * in conjunction with each other represent some "insane" usage of the 
   * FieldCache.
   */
  public final static class Insanity {
    private final InsanityType type;
    private final String msg;
    private final CacheEntry[] entries;
    public Insanity(InsanityType type, String msg, CacheEntry[] entries) {
      if (null == type) {
        throw new IllegalArgumentException
          ("Insanity requires non-null InsanityType");
      }
      if (null == entries || 0 == entries.length) {
        throw new IllegalArgumentException
          ("Insanity requires non-null/non-empty CacheEntry[]");
      }
      this.type = type;
      this.msg = msg;
      this.entries = entries;
      
    }
    /**
     * Type of insane behavior this object represents
     */
    public InsanityType getType() { return type; }
    /**
     * Description of hte insane behavior
     */
    public String getMsg() { return msg; }
    /**
     * CacheEntry objects which suggest a problem
     */
    public CacheEntry[] getCacheEntries() { return entries; }
    /**
     * Multi-Line representation of this Insanity object, starting with 
     * the Type and Msg, followed by each CacheEntry.toString() on it's 
     * own line prefaced by a tab character
     */
    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append(getType()).append(": ");

      String m = getMsg();
      if (null != m) buf.append(m);

      buf.append('\n');

      CacheEntry[] ce = getCacheEntries();
      for (int i = 0; i < ce.length; i++) {
        buf.append('\t').append(ce[i].toString()).append('\n');
      }

      return buf.toString();
    }
  }

  /**
   * An Enumeration of the different types of "insane" behavior that 
   * may be detected in a FieldCache.
   *
   * @see InsanityType#SUBREADER
   * @see InsanityType#VALUEMISMATCH
   * @see InsanityType#EXPECTED
   */
  public final static class InsanityType {
    private final String label;
    private InsanityType(final String label) {
      this.label = label;
    }
    public String toString() { return label; }

    /** 
     * Indicates an overlap in cache usage on a given field 
     * in sub/super readers.
     */
    public final static InsanityType SUBREADER 
      = new InsanityType("SUBREADER");

    /** 
     * <p>
     * Indicates entries have the same reader+fieldname but 
     * different cached values.  This can happen if different datatypes, 
     * or parsers are used -- and while it's not necessarily a bug 
     * it's typically an indication of a possible problem.
     * </p>
     * <p>
     * <bPNOTE:</b> Only the reader, fieldname, and cached value are actually 
     * tested -- if two cache entries have different parsers or datatypes but 
     * the cached values are the same Object (== not just equal()) this method 
     * does not consider that a red flag.  This allows for subtle variations 
     * in the way a Parser is specified (null vs DEFAULT_LONG_PARSER, etc...)
     * </p>
     */
    public final static InsanityType VALUEMISMATCH 
      = new InsanityType("VALUEMISMATCH");

    /** 
     * Indicates an expected bit of "insanity".  This may be useful for 
     * clients that wish to preserve/log information about insane usage 
     * but indicate that it was expected. 
     */
    public final static InsanityType EXPECTED
      = new InsanityType("EXPECTED");
  }
  
  
}
