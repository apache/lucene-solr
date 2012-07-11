package org.apache.lucene.facet.taxonomy;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

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

/**
 * TaxonomyReader is the read-only interface with which the faceted-search
 * library uses the taxonomy during search time.
 * <P>
 * A TaxonomyReader holds a list of categories. Each category has a serial
 * number which we call an "ordinal", and a hierarchical "path" name:
 * <UL>
 * <LI>
 * The ordinal is an integer that starts at 0 for the first category (which is
 * always the root category), and grows contiguously as more categories are
 * added; Note that once a category is added, it can never be deleted.
 * <LI>
 * The path is a CategoryPath object specifying the category's position in the
 * hierarchy.
 * </UL>
 * <B>Notes about concurrent access to the taxonomy:</B>
 * <P>
 * An implementation must allow multiple readers to be active concurrently
 * with a single writer. Readers follow so-called "point in time" semantics,
 * i.e., a TaxonomyReader object will only see taxonomy entries which were
 * available at the time it was created. What the writer writes is only
 * available to (new) readers after the writer's commit() is called.
 * <P>
 * In faceted search, two separate indices are used: the main Lucene index,
 * and the taxonomy. Because the main index refers to the categories listed
 * in the taxonomy, it is important to open the taxonomy *after* opening the
 * main index, and it is also necessary to reopen() the taxonomy after
 * reopen()ing the main index.
 * <P>
 * This order is important, otherwise it would be possible for the main index
 * to refer to a category which is not yet visible in the old snapshot of
 * the taxonomy. Note that it is indeed fine for the the taxonomy to be opened
 * after the main index - even a long time after. The reason is that once
 * a category is added to the taxonomy, it can never be changed or deleted,
 * so there is no danger that a "too new" taxonomy not being consistent with
 * an older index.
 * 
 * @lucene.experimental
 */
public interface TaxonomyReader extends Closeable {
  
  /**
   * The root category (the category with the empty path) always has the
   * ordinal 0, to which we give a name ROOT_ORDINAL.
   * getOrdinal() of an empty path will always return ROOT_ORDINAL, and
   * getCategory(ROOT_ORDINAL) will return the empty path.
   */
  public final static int ROOT_ORDINAL = 0;
  
  /**
   * Ordinals are always non-negative, so a negative ordinal can be used to
   * signify an error. Methods here return INVALID_ORDINAL (-1) in this case.
   */
  public final static int INVALID_ORDINAL = -1;
  
  /**
   * getOrdinal() returns the ordinal of the category given as a path.
   * The ordinal is the category's serial number, an integer which starts
   * with 0 and grows as more categories are added (note that once a category
   * is added, it can never be deleted).
   * <P>
   * If the given category wasn't found in the taxonomy, INVALID_ORDINAL is
   * returned.
   */
  public int getOrdinal(CategoryPath categoryPath) throws IOException;
  
  /**
   * getPath() returns the path name of the category with the given
   * ordinal. The path is returned as a new CategoryPath object - to
   * reuse an existing object, use {@link #getPath(int, CategoryPath)}.
   * <P>
   * A null is returned if a category with the given ordinal does not exist. 
   */
  public CategoryPath getPath(int ordinal) throws IOException;

  /**
   * getPath() returns the path name of the category with the given
   * ordinal. The path is written to the given CategoryPath object (which
   * is cleared first).
   * <P>
   * If a category with the given ordinal does not exist, the given
   * CategoryPath object is not modified, and the method returns
   * <code>false</code>. Otherwise, the method returns <code>true</code>. 
   */
  public boolean getPath(int ordinal, CategoryPath result) throws IOException;

  /**
   * refresh() re-reads the taxonomy information if there were any changes to
   * the taxonomy since this instance was opened or last refreshed. Calling
   * refresh() is more efficient than close()ing the old instance and opening a
   * new one.
   * <P>
   * If there were no changes since this instance was opened or last refreshed,
   * then this call does nothing. Note, however, that this is still a relatively
   * slow method (as it needs to verify whether there have been any changes on
   * disk to the taxonomy), so it should not be called too often needlessly. In
   * faceted search, the taxonomy reader's refresh() should be called only after
   * a reopen() of the main index.
   * <P>
   * Refreshing the taxonomy might fail in some cases, for example 
   * if the taxonomy was recreated since this instance was opened or last refreshed.
   * In this case an {@link InconsistentTaxonomyException} is thrown,
   * suggesting that in order to obtain up-to-date taxonomy data a new
   * {@link TaxonomyReader} should be opened. Note: This {@link TaxonomyReader} 
   * instance remains unchanged and usable in this case, and the application can
   * continue to use it, and should still {@link #close()} when no longer needed.  
   * <P>
   * It should be noted that refresh() is similar in purpose to
   * IndexReader.reopen(), but the two methods behave differently. refresh()
   * refreshes the existing TaxonomyReader object, rather than opening a new one
   * in addition to the old one as reopen() does. The reason is that in a
   * taxonomy, one can only add new categories and cannot modify or delete
   * existing categories; Therefore, there is no reason to keep an old snapshot
   * of the taxonomy open - refreshing the taxonomy to the newest data and using
   * this new snapshots in all threads (whether new or old) is fine. This saves
   * us needing to keep multiple copies of the taxonomy open in memory.
   * @return true if anything has changed, false otherwise. 
   */
  public boolean refresh() throws IOException, InconsistentTaxonomyException;
  
  /**
   * getParent() returns the ordinal of the parent category of the category
   * with the given ordinal.
   * <P>
   * When a category is specified as a path name, finding the path of its
   * parent is as trivial as dropping the last component of the path.
   * getParent() is functionally equivalent to calling getPath() on the
   * given ordinal, dropping the last component of the path, and then calling
   * getOrdinal() to get an ordinal back. However, implementations are
   * expected to provide a much more efficient implementation:
   * <P>
   * getParent() should be a very quick method, as it is used during the
   * facet aggregation process in faceted search. Implementations will most
   * likely want to serve replies to this method from a pre-filled cache.
   * <P>
   * If the given ordinal is the ROOT_ORDINAL, an INVALID_ORDINAL is returned.
   * If the given ordinal is a top-level category, the ROOT_ORDINAL is returned.
   * If an invalid ordinal is given (negative or beyond the last available
   * ordinal), an ArrayIndexOutOfBoundsException is thrown. However, it is
   * expected that getParent will only be called for ordinals which are
   * already known to be in the taxonomy.
   */
  public int getParent(int ordinal) throws IOException;
  
  /**
   * getParentArray() returns an int array of size getSize() listing the
   * ordinal of the parent category of each category in the taxonomy.
   * <P>
   * The caller can hold on to the array it got indefinitely - it is
   * guaranteed that no-one else will modify it. The other side of the
   * same coin is that the caller must treat the array it got as read-only
   * and <B>not modify it</B>, because other callers might have gotten the
   * same array too (and getParent() calls might be answered from the
   * same array).
   * <P>
   * If you use getParentArray() instead of getParent(), remember that
   * the array you got is (naturally) not modified after a refresh(),
   * so you should always call getParentArray() again after a refresh().
   * <P>
   * This method's function is similar to allocating an array of size
   * getSize() and filling it with getParent() calls, but implementations
   * are encouraged to implement it much more efficiently, with O(1)
   * complexity. This can be done, for example, by the implementation
   * already keeping the parents in an array, and just returning this
   * array (without any allocation or copying) when requested.
   */
  public int[] getParentArray() throws IOException;
  
  /**
   * Equivalent representations of the taxonomy's parent info, 
   * used internally for efficient computation of facet results: 
   * "youngest child" and "oldest sibling"   
   */
  public static interface ChildrenArrays {
    /**
     * getYoungestChildArray() returns an int array of size getSize()
     * listing the ordinal of the youngest (highest numbered) child
     * category of each category in the taxonomy. The value for a leaf
     * category (a category without children) is
     * <code>INVALID_ORDINAL</code>.
     */
    public int[] getYoungestChildArray();
    /**
     * getOlderSiblingArray() returns an int array of size getSize()
     * listing for each category the ordinal of its immediate older
     * sibling (the sibling in the taxonomy tree with the highest ordinal
     * below that of the given ordinal). The value for a category with no
     * older sibling is <code>INVALID_ORDINAL</code>.
     */
    public int[] getOlderSiblingArray();
  }
  
  /**
   * getChildrenArrays() returns a {@link ChildrenArrays} object which can
   * be used together to efficiently enumerate the children of any category. 
   * <P>
   * The caller can hold on to the object it got indefinitely - it is
   * guaranteed that no-one else will modify it. The other side of the
   * same coin is that the caller must treat the object which it got (and
   * the arrays it contains) as read-only and <B>not modify it</B>, because
   * other callers might have gotten the same object too.
   * <P>
   * Implementations should have O(getSize()) time for the first call or
   * after a refresh(), but O(1) time for further calls. In neither case
   * there should be a need to read new data from disk. These guarantees
   * are most likely achieved by calculating this object (based on the
   * getParentArray()) when first needed, and later (if the taxonomy was not
   * refreshed) returning the same object (without any allocation or copying)
   * when requested.
   * <P>
   * The reason we have one method returning one object, rather than two
   * methods returning two arrays, is to avoid race conditions in a multi-
   * threaded application: We want to avoid the possibility of returning one
   * new array and one old array, as those could not be used together.
   */
  public ChildrenArrays getChildrenArrays();
  
  /**
   * Retrieve user committed data.
   * @see TaxonomyWriter#commit(Map)
   */
  public Map<String, String> getCommitUserData() throws IOException;

  /**
   * Expert: increments the refCount of this TaxonomyReader instance. 
   * RefCounts can be used to determine when a taxonomy reader can be closed 
   * safely, i.e. as soon as there are no more references. 
   * Be sure to always call a corresponding decRef(), in a finally clause; 
   * otherwise the reader may never be closed. 
   */
  public void incRef();

  /**
   * Expert: decreases the refCount of this TaxonomyReader instance. 
   * If the refCount drops to 0, then pending changes (if any) can be  
   * committed to the taxonomy index and this reader can be closed. 
   * @throws IOException 
   */
  public void decRef() throws IOException;
  
  /**
   * Expert: returns the current refCount for this taxonomy reader
   */
  public int getRefCount();

  /**
   * getSize() returns the number of categories in the taxonomy.
   * <P>
   * Because categories are numbered consecutively starting with 0, it
   * means the taxonomy contains ordinals 0 through getSize()-1.
   * <P>
   * Note that the number returned by getSize() is often slightly higher
   * than the number of categories inserted into the taxonomy; This is
   * because when a category is added to the taxonomy, its ancestors
   * are also added automatically (including the root, which always get
   * ordinal 0).
   */
  public int getSize();

}
