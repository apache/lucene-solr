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
package org.apache.lucene.facet.taxonomy;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.SlowRAMDirectory;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.junit.Test;

@SuppressCodecs("SimpleText")
public class TestTaxonomyCombined extends FacetTestCase {

  /**  The following categories will be added to the taxonomy by
    fillTaxonomy(), and tested by all tests below:
  */
  private final static String[][] categories = {
    { "Author", "Tom Clancy" },
    { "Author", "Richard Dawkins" },
    { "Author", "Richard Adams" },
    { "Price", "10", "11" },
    { "Price", "10", "12" },
    { "Price", "20", "27" },
    { "Date", "2006", "05" },
    { "Date", "2005" },
    { "Date", "2006" },
    { "Subject", "Nonfiction", "Children", "Animals" },
    { "Author", "Stephen Jay Gould" },
    { "Author", "\u05e0\u05d3\u05d1\u3042\u0628" },
  };
  
  /**  When adding the above categories with TaxonomyWriter.addCategory(), 
    the following paths are expected to be returned:
    (note that currently the full path is not returned, and therefore
    not tested - rather, just the last component, the ordinal, is returned
    and tested.
  */
  private final static int[][] expectedPaths = {
    { 1, 2 },
    { 1, 3 },
    { 1, 4 },
    { 5, 6, 7 },
    { 5, 6, 8 },
    { 5, 9, 10 },
    { 11, 12, 13 },
    { 11, 14 },
    { 11, 12 },
    { 15, 16, 17, 18 },
    { 1, 19 },
    { 1, 20 }
  };

  /**  The taxonomy index is expected to then contain the following
    generated categories, with increasing ordinals (note how parent
    categories are be added automatically when subcategories are added).
   */  
  private final static String[][] expectedCategories = {
    { }, // the root category
    { "Author" },
    { "Author", "Tom Clancy" },
    { "Author", "Richard Dawkins" },
    { "Author", "Richard Adams" },
    { "Price" },
    { "Price", "10" },
    { "Price", "10", "11" },
    { "Price", "10", "12" },
    { "Price", "20" },
    { "Price", "20", "27" },
    { "Date" },
    { "Date", "2006" },
    { "Date", "2006", "05" },
    { "Date", "2005" },
    { "Subject" },
    { "Subject", "Nonfiction" },
    { "Subject", "Nonfiction", "Children" },
    { "Subject", "Nonfiction", "Children", "Animals" },
    { "Author", "Stephen Jay Gould" },
    { "Author", "\u05e0\u05d3\u05d1\u3042\u0628" },
  };

  /**  fillTaxonomy adds the categories in the categories[] array, and asserts
    that the additions return exactly the ordinals (in the past - paths)
    specified in expectedPaths[].
    Note that this assumes that fillTaxonomy() is called on an empty taxonomy
    index. Calling it after something else was already added to the taxonomy
    index will surely have this method fail.
   */
  public static void fillTaxonomy(TaxonomyWriter tw) throws IOException {
    for (int i = 0; i < categories.length; i++) {
      int ordinal = tw.addCategory(new FacetLabel(categories[i]));
      int expectedOrdinal = expectedPaths[i][expectedPaths[i].length-1];
      if (ordinal!=expectedOrdinal) {
        fail("For category "+showcat(categories[i])+" expected ordinal "+
            expectedOrdinal+", but got "+ordinal);
      }
    }
  }

  public static String showcat(String[] path) {
    if (path==null) {
      return "<null>";
    }
    if (path.length==0) {
      return "<empty>";
    }
    if (path.length==1 && path[0].length()==0) {
      return "<\"\">";
    }
    StringBuilder sb = new StringBuilder(path[0]);
    for (int i=1; i<path.length; i++) {
      sb.append('/');
      sb.append(path[i]);
    }
    return sb.toString();
  }

  private String showcat(FacetLabel path) {
    if (path==null) {
      return "<null>";
    }
    if (path.length==0) {
      return "<empty>";
    }
    return "<"+path.toString()+">";
  }

  /**  Basic tests for TaxonomyWriter. Basically, we test that
    IndexWriter.addCategory works, i.e. returns the expected ordinals
    (this is tested by calling the fillTaxonomy() method above).
    We do not test here that after writing the index can be read -
    this will be done in more tests below.
   */
  @Test
  public void testWriter() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    fillTaxonomy(tw);
    // Also check TaxonomyWriter.getSize() - see that the taxonomy's size
    // is what we expect it to be.
    assertEquals(expectedCategories.length, tw.getSize());
    tw.close();
    indexDir.close();
  }

  /**  testWriterTwice is exactly like testWriter, except that after adding
    all the categories, we add them again, and see that we get the same
    old ids again - not new categories.
   */
  @Test
  public void testWriterTwice() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    fillTaxonomy(tw);
    // run fillTaxonomy again - this will try to add the same categories
    // again, and check that we see the same ordinal paths again, not
    // different ones. 
    fillTaxonomy(tw);
    // Let's check the number of categories again, to see that no
    // extraneous categories were created:
    assertEquals(expectedCategories.length, tw.getSize());    
    tw.close();
    indexDir.close();
  }

  /**  testWriterTwice2 is similar to testWriterTwice, except that the index
    is closed and reopened before attempting to write to it the same
    categories again. While testWriterTwice can get along with writing
    and reading correctly just to the cache, testWriterTwice2 checks also
    the actual disk read part of the writer:
   */
  @Test
  public void testWriterTwice2() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    fillTaxonomy(tw);
    tw.close();
    tw = new DirectoryTaxonomyWriter(indexDir);
    // run fillTaxonomy again - this will try to add the same categories
    // again, and check that we see the same ordinals again, not different
    // ones, and that the number of categories hasn't grown by the new
    // additions
    fillTaxonomy(tw);
    assertEquals(expectedCategories.length, tw.getSize());    
    tw.close();
    indexDir.close();
  }
  
  /**
   * testWriterTwice3 is yet another test which tests creating a taxonomy
   * in two separate writing sessions. This test used to fail because of
   * a bug involving commit(), explained below, and now should succeed.
   */
  @Test
  public void testWriterTwice3() throws Exception {
    Directory indexDir = newDirectory();
    // First, create and fill the taxonomy
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    fillTaxonomy(tw);
    tw.close();
    // Now, open the same taxonomy and add the same categories again.
    // After a few categories, the LuceneTaxonomyWriter implementation
    // will stop looking for each category on disk, and rather read them
    // all into memory and close its reader. The bug was that it closed
    // the reader, but forgot that it did (because it didn't set the reader
    // reference to null).
    tw = new DirectoryTaxonomyWriter(indexDir);
    fillTaxonomy(tw);
    // Add one new category, just to make commit() do something:
    tw.addCategory(new FacetLabel("hi"));
    // Do a commit(). Here was a bug - if tw had a reader open, it should
    // be reopened after the commit. However, in our case the reader should
    // not be open (as explained above) but because it was not set to null,
    // we forgot that, tried to reopen it, and got an AlreadyClosedException.
    tw.commit();
    assertEquals(expectedCategories.length+1, tw.getSize());    
    tw.close();
    indexDir.close();
  }  
  
  /**  Another set of tests for the writer, which don't use an array and
   *  try to distill the different cases, and therefore may be more helpful
   *  for debugging a problem than testWriter() which is hard to know why
   *  or where it failed. 
   */
  @Test
  public void testWriterSimpler() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    assertEquals(1, tw.getSize()); // the root only
    // Test that adding a new top-level category works
    assertEquals(1, tw.addCategory(new FacetLabel("a")));
    assertEquals(2, tw.getSize());
    // Test that adding the same category again is noticed, and the
    // same ordinal (and not a new one) is returned.
    assertEquals(1, tw.addCategory(new FacetLabel("a")));
    assertEquals(2, tw.getSize());
    // Test that adding another top-level category returns a new ordinal,
    // not the same one
    assertEquals(2, tw.addCategory(new FacetLabel("b")));
    assertEquals(3, tw.getSize());
    // Test that adding a category inside one of the above adds just one
    // new ordinal:
    assertEquals(3, tw.addCategory(new FacetLabel("a","c")));
    assertEquals(4, tw.getSize());
    // Test that adding the same second-level category doesn't do anything:
    assertEquals(3, tw.addCategory(new FacetLabel("a","c")));
    assertEquals(4, tw.getSize());
    // Test that adding a second-level category with two new components
    // indeed adds two categories
    assertEquals(5, tw.addCategory(new FacetLabel("d","e")));
    assertEquals(6, tw.getSize());
    // Verify that the parents were added above in the order we expected
    assertEquals(4, tw.addCategory(new FacetLabel("d")));
    // Similar, but inside a category that already exists:
    assertEquals(7, tw.addCategory(new FacetLabel("b", "d","e")));
    assertEquals(8, tw.getSize());
    // And now inside two levels of categories that already exist:
    assertEquals(8, tw.addCategory(new FacetLabel("b", "d","f")));
    assertEquals(9, tw.getSize());
    
    tw.close();
    indexDir.close();
  }
  
  /**  Test writing an empty index, and seeing that a reader finds in it
    the root category, and only it. We check all the methods on that
    root category return the expected results.
   */
  @Test
  public void testRootOnly() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    // right after opening the index, it should already contain the
    // root, so have size 1:
    assertEquals(1, tw.getSize());
    tw.close();
    TaxonomyReader tr = new DirectoryTaxonomyReader(indexDir);
    assertEquals(1, tr.getSize());
    assertEquals(0, tr.getPath(0).length);
    assertEquals(TaxonomyReader.INVALID_ORDINAL, tr.getParallelTaxonomyArrays().parents()[0]);
    assertEquals(0, tr.getOrdinal(new FacetLabel()));
    tr.close();
    indexDir.close();
  }

  /**  The following test is exactly the same as testRootOnly, except we
   *  do not close the writer before opening the reader. We want to see
   *  that the root is visible to the reader not only after the writer is
   *  closed, but immediately after it is created.
   */
  @Test
  public void testRootOnly2() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    tw.commit();
    TaxonomyReader tr = new DirectoryTaxonomyReader(indexDir);
    assertEquals(1, tr.getSize());
    assertEquals(0, tr.getPath(0).length);
    assertEquals(TaxonomyReader.INVALID_ORDINAL, tr.getParallelTaxonomyArrays().parents()[0]);
    assertEquals(0, tr.getOrdinal(new FacetLabel()));
    tw.close();
    tr.close();
    indexDir.close();
  }

  /**  Basic tests for TaxonomyReader's category &lt;=&gt; ordinal transformations
    (getSize(), getCategory() and getOrdinal()).
    We test that after writing the index, it can be read and all the
    categories and ordinals are there just as we expected them to be.
   */
  @Test
  public void testReaderBasic() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    fillTaxonomy(tw);
    tw.close();
    TaxonomyReader tr = new DirectoryTaxonomyReader(indexDir);

    // test TaxonomyReader.getSize():
    assertEquals(expectedCategories.length, tr.getSize());

    // test round trips of ordinal => category => ordinal
    for (int i=0; i<tr.getSize(); i++) {
      assertEquals(i, tr.getOrdinal(tr.getPath(i)));
    }

    // test TaxonomyReader.getCategory():
    for (int i = 1; i < tr.getSize(); i++) {
      FacetLabel expectedCategory = new FacetLabel(expectedCategories[i]);
      FacetLabel category = tr.getPath(i);
      if (!expectedCategory.equals(category)) {
        fail("For ordinal "+i+" expected category "+
            showcat(expectedCategory)+", but got "+showcat(category));
      }
    }
    //  (also test invalid ordinals:)
    assertNull(tr.getPath(-1));
    assertNull(tr.getPath(tr.getSize()));
    assertNull(tr.getPath(TaxonomyReader.INVALID_ORDINAL));

    // test TaxonomyReader.getOrdinal():
    for (int i = 1; i < expectedCategories.length; i++) {
      int expectedOrdinal = i;
      int ordinal = tr.getOrdinal(new FacetLabel(expectedCategories[i]));
      if (expectedOrdinal != ordinal) {
        fail("For category "+showcat(expectedCategories[i])+" expected ordinal "+
            expectedOrdinal+", but got "+ordinal);
      }
    }
    // (also test invalid categories:)
    assertEquals(TaxonomyReader.INVALID_ORDINAL, tr.getOrdinal(new FacetLabel("non-existant")));
    assertEquals(TaxonomyReader.INVALID_ORDINAL, tr.getOrdinal(new FacetLabel("Author", "Jules Verne")));

    tr.close();
    indexDir.close();
  }

  /**  Tests for TaxonomyReader's getParent() method.
    We check it by comparing its results to those we could have gotten by
    looking at the category string paths (where the parentage is obvious).
    Note that after testReaderBasic(), we already know we can trust the
    ordinal &lt;=&gt; category conversions.
    
    Note: At the moment, the parent methods in the reader are deprecated,
    but this does not mean they should not be tested! Until they are
    removed (*if* they are removed), these tests should remain to see
    that they still work correctly.
   */

  @Test
  public void testReaderParent() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    fillTaxonomy(tw);
    tw.close();
    TaxonomyReader tr = new DirectoryTaxonomyReader(indexDir);

    // check that the parent of the root ordinal is the invalid ordinal:
    int[] parents = tr.getParallelTaxonomyArrays().parents();
    assertEquals(TaxonomyReader.INVALID_ORDINAL, parents[0]);

    // check parent of non-root ordinals:
    for (int ordinal=1; ordinal<tr.getSize(); ordinal++) {
      FacetLabel me = tr.getPath(ordinal);
      int parentOrdinal = parents[ordinal];
      FacetLabel parent = tr.getPath(parentOrdinal);
      if (parent==null) {
        fail("Parent of "+ordinal+" is "+parentOrdinal+
        ", but this is not a valid category.");
      }
      // verify that the parent is indeed my parent, according to the strings
      if (!me.subpath(me.length-1).equals(parent)) {
        fail("Got parent "+parentOrdinal+" for ordinal "+ordinal+
            " but categories are "+showcat(parent)+" and "+showcat(me)+
            " respectively.");
      }
    }

    tr.close();
    indexDir.close();
  }
  
  /**
   * Tests for TaxonomyWriter's getParent() method. We check it by comparing
   * its results to those we could have gotten by looking at the category
   * string paths using a TaxonomyReader (where the parentage is obvious).
   * Note that after testReaderBasic(), we already know we can trust the
   * ordinal &lt;=&gt; category conversions from TaxonomyReader.
   *
   * The difference between testWriterParent1 and testWriterParent2 is that
   * the former closes the taxonomy writer before reopening it, while the
   * latter does not.
   * 
   * This test code is virtually identical to that of testReaderParent().
   */
  @Test
  public void testWriterParent1() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    fillTaxonomy(tw);
    tw.close();
    tw = new DirectoryTaxonomyWriter(indexDir);
    TaxonomyReader tr = new DirectoryTaxonomyReader(indexDir);
    
    checkWriterParent(tr, tw);
    
    tw.close();
    tr.close();
    indexDir.close();
  }

  @Test
  public void testWriterParent2() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    fillTaxonomy(tw);
    tw.commit();
    TaxonomyReader tr = new DirectoryTaxonomyReader(indexDir);
    
    checkWriterParent(tr, tw);
    
    tw.close();
    tr.close();
    indexDir.close();
  }
  
  private void checkWriterParent(TaxonomyReader tr, TaxonomyWriter tw) throws Exception {
    // check that the parent of the root ordinal is the invalid ordinal:
    assertEquals(TaxonomyReader.INVALID_ORDINAL, tw.getParent(0));

    // check parent of non-root ordinals:
    for (int ordinal = 1; ordinal < tr.getSize(); ordinal++) {
      FacetLabel me = tr.getPath(ordinal);
      int parentOrdinal = tw.getParent(ordinal);
      FacetLabel parent = tr.getPath(parentOrdinal);
      if (parent == null) {
        fail("Parent of " + ordinal + " is " + parentOrdinal
            + ", but this is not a valid category.");
      }
      // verify that the parent is indeed my parent, according to the
      // strings
      if (!me.subpath(me.length - 1).equals(parent)) {
        fail("Got parent " + parentOrdinal + " for ordinal " + ordinal
            + " but categories are " + showcat(parent) + " and "
            + showcat(me) + " respectively.");
      }
    }

    // check parent of of invalid ordinals:
    try {
      tw.getParent(-1);
      fail("getParent for -1 should throw exception");
    } catch (ArrayIndexOutOfBoundsException e) {
      // ok
    }
    try {
      tw.getParent(TaxonomyReader.INVALID_ORDINAL);
      fail("getParent for INVALID_ORDINAL should throw exception");
    } catch (ArrayIndexOutOfBoundsException e) {
      // ok
    }
    try {
      int parent = tw.getParent(tr.getSize());
      fail("getParent for getSize() should throw exception, but returned "
          + parent);
    } catch (ArrayIndexOutOfBoundsException e) {
      // ok
    }
  }

  /**
   * Test TaxonomyReader's child browsing method, getChildrenArrays()
   * This only tests for correctness of the data on one example - we have
   * below further tests on data refresh etc.
   */
  @Test
  public void testChildrenArrays() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    fillTaxonomy(tw);
    tw.close();
    TaxonomyReader tr = new DirectoryTaxonomyReader(indexDir);
    ParallelTaxonomyArrays ca = tr.getParallelTaxonomyArrays();
    int[] youngestChildArray = ca.children();
    assertEquals(tr.getSize(), youngestChildArray.length);
    int[] olderSiblingArray = ca.siblings();
    assertEquals(tr.getSize(), olderSiblingArray.length);
    for (int i=0; i<expectedCategories.length; i++) {
      // find expected children by looking at all expectedCategories
      // for children
      ArrayList<Integer> expectedChildren = new ArrayList<>();
      for (int j=expectedCategories.length-1; j>=0; j--) {
        if (expectedCategories[j].length != expectedCategories[i].length+1) {
          continue; // not longer by 1, so can't be a child
        }
        boolean ischild=true;
        for (int k=0; k<expectedCategories[i].length; k++) {
          if (!expectedCategories[j][k].equals(expectedCategories[i][k])) {
            ischild=false;
            break;
          }
        }
        if (ischild) {
          expectedChildren.add(j);
        }
      }
      // check that children and expectedChildren are the same, with the
      // correct reverse (youngest to oldest) order:
      if (expectedChildren.size()==0) {
        assertEquals(TaxonomyReader.INVALID_ORDINAL, youngestChildArray[i]);
      } else {
        int child = youngestChildArray[i];
        assertEquals(expectedChildren.get(0).intValue(),
            child);
        for (int j=1; j<expectedChildren.size(); j++) {
          child = olderSiblingArray[child];
          assertEquals(expectedChildren.get(j).intValue(),
              child);
          // if child is INVALID_ORDINAL we should stop, but
          // assertEquals would fail in this case anyway.
        }
        // When we're done comparing, olderSiblingArray should now point
        // to INVALID_ORDINAL, saying there are no more children. If it
        // doesn't, we found too many children...
        assertEquals(-1, olderSiblingArray[child]);
      }
    }
    tr.close();
    indexDir.close();
  }

  /**
   * Similar to testChildrenArrays, except rather than look at
   * expected results, we test for several "invariants" that the results
   * should uphold, e.g., that a child of a category indeed has this category
   * as its parent. This sort of test can more easily be extended to larger
   * example taxonomies, because we do not need to build the expected list
   * of categories like we did in the above test.
   */
  @Test
  public void testChildrenArraysInvariants() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    fillTaxonomy(tw);
    tw.close();
    TaxonomyReader tr = new DirectoryTaxonomyReader(indexDir);
    ParallelTaxonomyArrays ca = tr.getParallelTaxonomyArrays();
    int[] children = ca.children();
    assertEquals(tr.getSize(), children.length);
    int[] olderSiblingArray = ca.siblings();
    assertEquals(tr.getSize(), olderSiblingArray.length);
        
    // test that the "youngest child" of every category is indeed a child:
    int[] parents = tr.getParallelTaxonomyArrays().parents();
    for (int i=0; i<tr.getSize(); i++) {
      int youngestChild = children[i];
      if (youngestChild != TaxonomyReader.INVALID_ORDINAL) {
        assertEquals(i, parents[youngestChild]);
      }
    }
        
    // test that the "older sibling" of every category is indeed older (lower)
    // (it can also be INVALID_ORDINAL, which is lower than any ordinal)
    for (int i=0; i<tr.getSize(); i++) {
      assertTrue("olderSiblingArray["+i+"] should be <"+i, olderSiblingArray[i] < i);
    }
    
    // test that the "older sibling" of every category is indeed a sibling
    // (they share the same parent)
    for (int i=0; i<tr.getSize(); i++) {
      int sibling = olderSiblingArray[i];
      if (sibling == TaxonomyReader.INVALID_ORDINAL) {
        continue;
      }
      assertEquals(parents[i], parents[sibling]);
    }
    
    // And now for slightly more complex (and less "invariant-like"...)
    // tests:
    
    // test that the "youngest child" is indeed the youngest (so we don't
    // miss the first children in the chain)
    for (int i=0; i<tr.getSize(); i++) {
      // Find the really youngest child:
      int j;
      for (j=tr.getSize()-1; j>i; j--) {
        if (parents[j]==i) {
          break; // found youngest child
        }
      }
      if (j==i) { // no child found
        j=TaxonomyReader.INVALID_ORDINAL;
      }
      assertEquals(j, children[i]);
    }

    // test that the "older sibling" is indeed the least oldest one - and
    // not a too old one or -1 (so we didn't miss some children in the
    // middle or the end of the chain).
    for (int i=0; i<tr.getSize(); i++) {
      // Find the youngest older sibling:
      int j;
      for (j=i-1; j>=0; j--) {
        if (parents[j]==parents[i]) {
          break; // found youngest older sibling
        }
      }
      if (j<0) { // no sibling found
        j=TaxonomyReader.INVALID_ORDINAL;
      }
      assertEquals(j, olderSiblingArray[i]);
    }
  
    tr.close();
    indexDir.close();
  }
  
  /**
   * Test how getChildrenArrays() deals with the taxonomy's growth:
   */
  @Test
  public void testChildrenArraysGrowth() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    tw.addCategory(new FacetLabel("hi", "there"));
    tw.commit();
    TaxonomyReader tr = new DirectoryTaxonomyReader(indexDir);
    ParallelTaxonomyArrays ca = tr.getParallelTaxonomyArrays();
    assertEquals(3, tr.getSize());
    assertEquals(3, ca.siblings().length);
    assertEquals(3, ca.children().length);
    assertTrue(Arrays.equals(new int[] { 1, 2, -1 }, ca.children()));
    assertTrue(Arrays.equals(new int[] { -1, -1, -1 }, ca.siblings()));
    tw.addCategory(new FacetLabel("hi", "ho"));
    tw.addCategory(new FacetLabel("hello"));
    tw.commit();
    // Before refresh, nothing changed..
    ParallelTaxonomyArrays newca = tr.getParallelTaxonomyArrays();
    assertSame(newca, ca); // we got exactly the same object
    assertEquals(3, tr.getSize());
    assertEquals(3, ca.siblings().length);
    assertEquals(3, ca.children().length);
    // After the refresh, things change:
    TaxonomyReader newtr = TaxonomyReader.openIfChanged(tr);
    assertNotNull(newtr);
    tr.close();
    tr = newtr;
    ca = tr.getParallelTaxonomyArrays();
    assertEquals(5, tr.getSize());
    assertEquals(5, ca.siblings().length);
    assertEquals(5, ca.children().length);
    assertTrue(Arrays.equals(new int[] { 4, 3, -1, -1, -1 }, ca.children()));
    assertTrue(Arrays.equals(new int[] { -1, -1, -1, 2, 1 }, ca.siblings()));
    tw.close();
    tr.close();
    indexDir.close();
  }
  
  // Test that getParentArrays is valid when retrieved during refresh
  @Test
  public void testTaxonomyReaderRefreshRaces() throws Exception {
    // compute base child arrays - after first chunk, and after the other
    Directory indexDirBase = newDirectory();
    TaxonomyWriter twBase = new DirectoryTaxonomyWriter(indexDirBase);
    twBase.addCategory(new FacetLabel("a", "0"));
    final FacetLabel abPath = new FacetLabel("a", "b");
    twBase.addCategory(abPath);
    twBase.commit();
    TaxonomyReader trBase = new DirectoryTaxonomyReader(indexDirBase);

    final ParallelTaxonomyArrays ca1 = trBase.getParallelTaxonomyArrays();
    
    final int abOrd = trBase.getOrdinal(abPath);
    final int abYoungChildBase1 = ca1.children()[abOrd]; 
    
    final int numCategories = atLeast(800);
    for (int i = 0; i < numCategories; i++) {
      twBase.addCategory(new FacetLabel("a", "b", Integer.toString(i)));
    }
    twBase.close();
    
    TaxonomyReader newTaxoReader = TaxonomyReader.openIfChanged(trBase);
    assertNotNull(newTaxoReader);
    trBase.close();
    trBase = newTaxoReader;
    
    final ParallelTaxonomyArrays ca2 = trBase.getParallelTaxonomyArrays();
    final int abYoungChildBase2 = ca2.children()[abOrd];
    
    int numRetries = atLeast(50);
    for (int retry = 0; retry < numRetries; retry++) {
      assertConsistentYoungestChild(abPath, abOrd, abYoungChildBase1, abYoungChildBase2, retry, numCategories);
    }
    
    trBase.close();
    indexDirBase.close();
  }

  private void assertConsistentYoungestChild(final FacetLabel abPath,
      final int abOrd, final int abYoungChildBase1, final int abYoungChildBase2, final int retry, int numCategories)
      throws Exception {
    SlowRAMDirectory indexDir = new SlowRAMDirectory(-1, null); // no slowness for initialization
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    tw.addCategory(new FacetLabel("a", "0"));
    tw.addCategory(abPath);
    tw.commit();
    
    final DirectoryTaxonomyReader tr = new DirectoryTaxonomyReader(indexDir);
    for (int i = 0; i < numCategories; i++) {
      final FacetLabel cp = new FacetLabel("a", "b", Integer.toString(i));
      tw.addCategory(cp);
      assertEquals("Ordinal of "+cp+" must be invalid until Taxonomy Reader was refreshed", TaxonomyReader.INVALID_ORDINAL, tr.getOrdinal(cp));
    }
    tw.close();
    
    final AtomicBoolean stop = new AtomicBoolean(false);
    final Throwable[] error = new Throwable[] { null };
    final int retrieval[] = { 0 }; 
    
    Thread thread = new Thread("Child Arrays Verifier") {
      @Override
      public void run() {
        setPriority(1 + getPriority());
        try {
          while (!stop.get()) {
            int lastOrd = tr.getParallelTaxonomyArrays().parents().length - 1;
            assertNotNull("path of last-ord " + lastOrd + " is not found!", tr.getPath(lastOrd));
            assertChildrenArrays(tr.getParallelTaxonomyArrays(), retry, retrieval[0]++);
            sleep(10); // don't starve refresh()'s CPU, which sleeps every 50 bytes for 1 ms
          }
        } catch (Throwable e) {
          error[0] = e;
          stop.set(true);
        }
      }

      private void assertChildrenArrays(ParallelTaxonomyArrays ca, int retry, int retrieval) {
        final int abYoungChild = ca.children()[abOrd];
        assertTrue(
            "Retry "+retry+": retrieval: "+retrieval+": wrong youngest child for category "+abPath+" (ord="+abOrd+
            ") - must be either "+abYoungChildBase1+" or "+abYoungChildBase2+" but was: "+abYoungChild,
            abYoungChildBase1==abYoungChild ||
            abYoungChildBase2==ca.children()[abOrd]);
      }
    };
    thread.start();
    
    indexDir.setSleepMillis(1); // some delay for refresh
    TaxonomyReader newTaxoReader = TaxonomyReader.openIfChanged(tr);
    if (newTaxoReader != null) {
      newTaxoReader.close();
    }
    
    stop.set(true);
    thread.join();
    assertNull("Unexpcted exception at retry "+retry+" retrieval "+retrieval[0]+": \n"+stackTraceStr(error[0]), error[0]);
    
    tr.close();
  }

  /** Grab the stack trace into a string since the exception was thrown in a thread and we want the assert 
   * outside the thread to show the stack trace in case of failure.   */
  private String stackTraceStr(final Throwable error) {
    if (error == null) {
      return "";
    }
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    error.printStackTrace(pw);
    pw.close();
    return sw.toString();
  }
  
  /**  Test that if separate reader and writer objects are opened, new
    categories written into the writer are available to a reader only
    after a commit().
    Note that this test obviously doesn't cover all the different
    concurrency scenarios, all different methods, and so on. We may
    want to write more tests of this sort.

    This test simulates what would happen when there are two separate
    processes, one doing indexing, and the other searching, and each opens
    its own object (with obviously no connection between the objects) using
    the same disk files. Note, though, that this test does not test what
    happens when the two processes do their actual work at exactly the same
    time.
    It also doesn't test multi-threading.
   */
  @Test
  public void testSeparateReaderAndWriter() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    tw.commit();
    TaxonomyReader tr = new DirectoryTaxonomyReader(indexDir);

    assertEquals(1, tr.getSize()); // the empty taxonomy has size 1 (the root)
    tw.addCategory(new FacetLabel("Author"));
    assertEquals(1, tr.getSize()); // still root only...
    assertNull(TaxonomyReader.openIfChanged(tr)); // this is not enough, because tw.commit() hasn't been done yet
    assertEquals(1, tr.getSize()); // still root only...
    tw.commit();
    assertEquals(1, tr.getSize()); // still root only...
    TaxonomyReader newTaxoReader = TaxonomyReader.openIfChanged(tr);
    assertNotNull(newTaxoReader);
    tr.close();
    tr = newTaxoReader;
    
    int author = 1;
    try {
      assertEquals(TaxonomyReader.ROOT_ORDINAL, tr.getParallelTaxonomyArrays().parents()[author]);
      // ok
    } catch (ArrayIndexOutOfBoundsException e) {
      fail("After category addition, commit() and refresh(), getParent for "+author+" should NOT throw exception");
    }
    assertEquals(2, tr.getSize()); // finally, see there are two categories

    // now, add another category, and verify that after commit and refresh
    // the parent of this category is correct (this requires the reader
    // to correctly update its prefetched parent vector), and that the
    // old information also wasn't ruined:
    tw.addCategory(new FacetLabel("Author", "Richard Dawkins"));
    int dawkins = 2;
    tw.commit();
    newTaxoReader = TaxonomyReader.openIfChanged(tr);
    assertNotNull(newTaxoReader);
    tr.close();
    tr = newTaxoReader;
    int[] parents = tr.getParallelTaxonomyArrays().parents();
    assertEquals(author, parents[dawkins]);
    assertEquals(TaxonomyReader.ROOT_ORDINAL, parents[author]);
    assertEquals(TaxonomyReader.INVALID_ORDINAL, parents[TaxonomyReader.ROOT_ORDINAL]);
    assertEquals(3, tr.getSize()); 
    tw.close();
    tr.close();
    indexDir.close();
  }
  
  @Test
  public void testSeparateReaderAndWriter2() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    tw.commit();
    TaxonomyReader tr = new DirectoryTaxonomyReader(indexDir);

    // Test getOrdinal():
    FacetLabel author = new FacetLabel("Author");

    assertEquals(1, tr.getSize()); // the empty taxonomy has size 1 (the root)
    assertEquals(TaxonomyReader.INVALID_ORDINAL, tr.getOrdinal(author));
    tw.addCategory(author);
    // before commit and refresh, no change:
    assertEquals(TaxonomyReader.INVALID_ORDINAL, tr.getOrdinal(author));
    assertEquals(1, tr.getSize()); // still root only...
    assertNull(TaxonomyReader.openIfChanged(tr)); // this is not enough, because tw.commit() hasn't been done yet
    assertEquals(TaxonomyReader.INVALID_ORDINAL, tr.getOrdinal(author));
    assertEquals(1, tr.getSize()); // still root only...
    tw.commit();
    // still not enough before refresh:
    assertEquals(TaxonomyReader.INVALID_ORDINAL, tr.getOrdinal(author));
    assertEquals(1, tr.getSize()); // still root only...
    TaxonomyReader newTaxoReader = TaxonomyReader.openIfChanged(tr);
    assertNotNull(newTaxoReader);
    tr.close();
    tr = newTaxoReader;
    assertEquals(1, tr.getOrdinal(author));
    assertEquals(2, tr.getSize());
    tw.close();
    tr.close();
    indexDir.close();
  }
  
  /**
   * fillTaxonomyCheckPaths adds the categories in the categories[] array,
   * and asserts that the additions return exactly paths specified in
   * expectedPaths[]. This is the same add fillTaxonomy() but also checks
   * the correctness of getParent(), not just addCategory().
   * Note that this assumes that fillTaxonomyCheckPaths() is called on an empty
   * taxonomy index. Calling it after something else was already added to the
   * taxonomy index will surely have this method fail.
   */
  public static void fillTaxonomyCheckPaths(TaxonomyWriter tw) throws IOException {
    for (int i = 0; i < categories.length; i++) {
      int ordinal = tw.addCategory(new FacetLabel(categories[i]));
      int expectedOrdinal = expectedPaths[i][expectedPaths[i].length-1];
      if (ordinal!=expectedOrdinal) {
        fail("For category "+showcat(categories[i])+" expected ordinal "+
            expectedOrdinal+", but got "+ordinal);
      }
      for (int j=expectedPaths[i].length-2; j>=0; j--) {
        ordinal = tw.getParent(ordinal);
        expectedOrdinal = expectedPaths[i][j];
        if (ordinal!=expectedOrdinal) {
          fail("For category "+showcat(categories[i])+" expected ancestor level "+
              (expectedPaths[i].length-1-j)+" was "+expectedOrdinal+
              ", but got "+ordinal);
        }
      }    
    }
  }
  
  // After fillTaxonomy returned successfully, checkPaths() checks that
  // the getParent() calls return as expected, from the table
  public static void checkPaths(TaxonomyWriter tw) throws IOException {
    for (int i = 0; i < categories.length; i++) {
      int ordinal = expectedPaths[i][expectedPaths[i].length-1];
      for (int j=expectedPaths[i].length-2; j>=0; j--) {
        ordinal = tw.getParent(ordinal);
        int expectedOrdinal = expectedPaths[i][j];
        if (ordinal!=expectedOrdinal) {
          fail("For category "+showcat(categories[i])+" expected ancestor level "+
              (expectedPaths[i].length-1-j)+" was "+expectedOrdinal+
              ", but got "+ordinal);
        }
      }
      assertEquals(TaxonomyReader.ROOT_ORDINAL, tw.getParent(expectedPaths[i][0]));
    }
    assertEquals(TaxonomyReader.INVALID_ORDINAL, tw.getParent(TaxonomyReader.ROOT_ORDINAL));
  }
  
  /**
   * Basic test for TaxonomyWriter.getParent(). This is similar to testWriter
   * above, except we also check the parents of the added categories, not just
   * the categories themselves.
   */
  @Test
  public void testWriterCheckPaths() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    fillTaxonomyCheckPaths(tw);
    // Also check TaxonomyWriter.getSize() - see that the taxonomy's size
    // is what we expect it to be.
    assertEquals(expectedCategories.length, tw.getSize());
    tw.close();
    indexDir.close();
  }
  
  /**
   * testWriterCheckPaths2 is the path-checking variant of testWriterTwice
   * and testWriterTwice2. After adding all the categories, we add them again,
   * and see that we get the same old ids and paths. We repeat the path checking
   * yet again after closing and opening the index for writing again - to see
   * that the reading of existing data from disk works as well.
   */
  @Test
  public void testWriterCheckPaths2() throws Exception {
    Directory indexDir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(indexDir);
    fillTaxonomy(tw);
    checkPaths(tw);
    fillTaxonomy(tw);
    checkPaths(tw);
    tw.close();

    tw = new DirectoryTaxonomyWriter(indexDir);
    checkPaths(tw);
    fillTaxonomy(tw);
    checkPaths(tw);
    tw.close();
    indexDir.close();
  }

  @Test
  public void testNRT() throws Exception {
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(dir);
    TaxonomyReader reader = new DirectoryTaxonomyReader(writer);
    
    FacetLabel cp = new FacetLabel("a");
    writer.addCategory(cp);
    TaxonomyReader newReader = TaxonomyReader.openIfChanged(reader);
    assertNotNull("expected a new instance", newReader);
    assertEquals(2, newReader.getSize());
    assertNotSame(TaxonomyReader.INVALID_ORDINAL, newReader.getOrdinal(cp));
    reader.close();
    reader = newReader;
    
    writer.close();
    reader.close();
    
    dir.close();
  }

//  TODO (Facet): test multiple readers, one writer. Have the multiple readers
//  using the same object (simulating threads) or different objects
//  (simulating processes).
}
