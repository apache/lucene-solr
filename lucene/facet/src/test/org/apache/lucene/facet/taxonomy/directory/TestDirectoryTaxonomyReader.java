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
package org.apache.lucene.facet.taxonomy.directory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyReader.ChildrenIterator;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

public class TestDirectoryTaxonomyReader extends FacetTestCase {

  @Test
  public void testCloseAfterIncRef() throws Exception {
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter ltw = new DirectoryTaxonomyWriter(dir);
    ltw.addCategory(new FacetLabel("a"));
    ltw.close();
    
    DirectoryTaxonomyReader ltr = new DirectoryTaxonomyReader(dir);
    ltr.incRef();
    ltr.close();
    
    // should not fail as we incRef() before close
    ltr.getSize();
    ltr.decRef();
    
    dir.close();
  }
  
  @Test
  public void testCloseTwice() throws Exception {
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter ltw = new DirectoryTaxonomyWriter(dir);
    ltw.addCategory(new FacetLabel("a"));
    ltw.close();
    
    DirectoryTaxonomyReader ltr = new DirectoryTaxonomyReader(dir);
    ltr.close();
    ltr.close(); // no exception should be thrown
    
    dir.close();
  }
  
  @Test
  public void testOpenIfChangedResult() throws Exception {
    Directory dir = null;
    DirectoryTaxonomyWriter ltw = null;
    DirectoryTaxonomyReader ltr = null;
    
    try {
      dir = newDirectory();
      ltw = new DirectoryTaxonomyWriter(dir);
      
      ltw.addCategory(new FacetLabel("a"));
      ltw.commit();
      
      ltr = new DirectoryTaxonomyReader(dir);
      assertNull("Nothing has changed", TaxonomyReader.openIfChanged(ltr));
      
      ltw.addCategory(new FacetLabel("b"));
      ltw.commit();
      
      DirectoryTaxonomyReader newtr = TaxonomyReader.openIfChanged(ltr);
      assertNotNull("changes were committed", newtr);
      assertNull("Nothing has changed", TaxonomyReader.openIfChanged(newtr));
      newtr.close();
    } finally {
      IOUtils.close(ltw, ltr, dir);
    }
  }
  
  @Test
  public void testAlreadyClosed() throws Exception {
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter ltw = new DirectoryTaxonomyWriter(dir);
    ltw.addCategory(new FacetLabel("a"));
    ltw.close();
    
    DirectoryTaxonomyReader ltr = new DirectoryTaxonomyReader(dir);
    ltr.close();
    try {
      ltr.getSize();
      fail("An AlreadyClosedException should have been thrown here");
    } catch (AlreadyClosedException ace) {
      // good!
    }
    dir.close();
  }
  
  /**
   * recreating a taxonomy should work well with a freshly opened taxonomy reader 
   */
  @Test
  public void testFreshReadRecreatedTaxonomy() throws Exception {
    doTestReadRecreatedTaxonomy(random(), true);
  }
  
  @Test
  public void testOpenIfChangedReadRecreatedTaxonomy() throws Exception {
    doTestReadRecreatedTaxonomy(random(), false);
  }
  
  private void doTestReadRecreatedTaxonomy(Random random, boolean closeReader) throws Exception {
    Directory dir = null;
    TaxonomyWriter tw = null;
    TaxonomyReader tr = null;
    
    // prepare a few categories
    int  n = 10;
    FacetLabel[] cp = new FacetLabel[n];
    for (int i=0; i<n; i++) {
      cp[i] = new FacetLabel("a", Integer.toString(i));
    }
    
    try {
      dir = newDirectory();
      
      tw = new DirectoryTaxonomyWriter(dir);
      tw.addCategory(new FacetLabel("a"));
      tw.close();
      
      tr = new DirectoryTaxonomyReader(dir);
      int baseNumCategories = tr.getSize();
      
      for (int i=0; i<n; i++) {
        int k = random.nextInt(n);
        tw = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE);
        for (int j = 0; j <= k; j++) {
          tw.addCategory(cp[j]);
        }
        tw.close();
        if (closeReader) {
          tr.close();
          tr = new DirectoryTaxonomyReader(dir);
        } else {
          TaxonomyReader newtr = TaxonomyReader.openIfChanged(tr);
          assertNotNull(newtr);
          tr.close();
          tr = newtr;
        }
        assertEquals("Wrong #categories in taxonomy (i="+i+", k="+k+")", baseNumCategories + 1 + k, tr.getSize());
      }
    } finally {
      IOUtils.close(tr, tw, dir);
    }
  }
  
  @Test
  public void testOpenIfChangedAndRefCount() throws Exception {
    Directory dir = new RAMDirectory(); // no need for random directories here

    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir);
    taxoWriter.addCategory(new FacetLabel("a"));
    taxoWriter.commit();

    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(dir);
    assertEquals("wrong refCount", 1, taxoReader.getRefCount());

    taxoReader.incRef();
    assertEquals("wrong refCount", 2, taxoReader.getRefCount());

    taxoWriter.addCategory(new FacetLabel("a", "b"));
    taxoWriter.commit();
    TaxonomyReader newtr = TaxonomyReader.openIfChanged(taxoReader);
    assertNotNull(newtr);
    taxoReader.close();
    taxoReader = newtr;
    assertEquals("wrong refCount", 1, taxoReader.getRefCount());

    taxoWriter.close();
    taxoReader.close();
    dir.close();
  }

  @Test
  public void testOpenIfChangedManySegments() throws Exception {
    // test openIfChanged() when the taxonomy contains many segments
    Directory dir = newDirectory();
    
    DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(dir) {
      @Override
      protected IndexWriterConfig createIndexWriterConfig(OpenMode openMode) {
        IndexWriterConfig conf = super.createIndexWriterConfig(openMode);
        LogMergePolicy lmp = (LogMergePolicy) conf.getMergePolicy();
        lmp.setMergeFactor(2);
        return conf;
      }
    };
    TaxonomyReader reader = new DirectoryTaxonomyReader(writer);
    
    int numRounds = random().nextInt(10) + 10;
    int numCategories = 1; // one for root
    for (int i = 0; i < numRounds; i++) {
      int numCats = random().nextInt(4) + 1;
      for (int j = 0; j < numCats; j++) {
        writer.addCategory(new FacetLabel(Integer.toString(i), Integer.toString(j)));
      }
      numCategories += numCats + 1 /* one for round-parent */;
      TaxonomyReader newtr = TaxonomyReader.openIfChanged(reader);
      assertNotNull(newtr);
      reader.close();
      reader = newtr;
      
      // assert categories
      assertEquals(numCategories, reader.getSize());
      int roundOrdinal = reader.getOrdinal(new FacetLabel(Integer.toString(i)));
      int[] parents = reader.getParallelTaxonomyArrays().parents();
      assertEquals(0, parents[roundOrdinal]); // round's parent is root
      for (int j = 0; j < numCats; j++) {
        int ord = reader.getOrdinal(new FacetLabel(Integer.toString(i), Integer.toString(j)));
        assertEquals(roundOrdinal, parents[ord]); // round's parent is root
      }
    }
    
    reader.close();
    writer.close();
    dir.close();
  }
  
  @Test
  public void testOpenIfChangedMergedSegment() throws Exception {
    // test openIfChanged() when all index segments were merged - used to be
    // a bug in ParentArray, caught by testOpenIfChangedManySegments - only
    // this test is not random
    Directory dir = newDirectory();
    
    // hold onto IW to forceMerge
    // note how we don't close it, since DTW will close it.
    final IndexWriter iw = new IndexWriter(dir,
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setMergePolicy(new LogByteSizeMergePolicy()));
    DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(dir) {
      @Override
      protected IndexWriter openIndexWriter(Directory directory,
          IndexWriterConfig config) throws IOException {
        return iw;
      }
    };
    
    TaxonomyReader reader = new DirectoryTaxonomyReader(writer);
    assertEquals(1, reader.getSize());
    assertEquals(1, reader.getParallelTaxonomyArrays().parents().length);

    // add category and call forceMerge -- this should flush IW and merge segments down to 1
    // in ParentArray.initFromReader, this used to fail assuming there are no parents.
    writer.addCategory(new FacetLabel("1"));
    iw.forceMerge(1);
    
    // now calling openIfChanged should trip on the bug
    TaxonomyReader newtr = TaxonomyReader.openIfChanged(reader);
    assertNotNull(newtr);
    reader.close();
    reader = newtr;
    assertEquals(2, reader.getSize());
    assertEquals(2, reader.getParallelTaxonomyArrays().parents().length);
    
    reader.close();
    writer.close();
    dir.close();
  }
  
  @Test
  public void testOpenIfChangedNoChangesButSegmentMerges() throws Exception {
    // test openIfChanged() when the taxonomy hasn't really changed, but segments
    // were merged. The NRT reader will be reopened, and ParentArray used to assert
    // that the new reader contains more ordinals than were given from the old
    // TaxReader version
    Directory dir = newDirectory();
    
    // hold onto IW to forceMerge
    // note how we don't close it, since DTW will close it.
    final IndexWriter iw = new IndexWriter(dir,
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setMergePolicy(new LogByteSizeMergePolicy()));
    DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(dir) {
      @Override
      protected IndexWriter openIndexWriter(Directory directory,
          IndexWriterConfig config) throws IOException {
        return iw;
      }
    };
    
    // add a category so that the following DTR open will cause a flush and 
    // a new segment will be created
    writer.addCategory(new FacetLabel("a"));
    
    TaxonomyReader reader = new DirectoryTaxonomyReader(writer);
    assertEquals(2, reader.getSize());
    assertEquals(2, reader.getParallelTaxonomyArrays().parents().length);

    // merge all the segments so that NRT reader thinks there's a change 
    iw.forceMerge(1);
    
    // now calling openIfChanged should trip on the wrong assert in ParetArray's ctor
    TaxonomyReader newtr = TaxonomyReader.openIfChanged(reader);
    assertNotNull(newtr);
    reader.close();
    reader = newtr;
    assertEquals(2, reader.getSize());
    assertEquals(2, reader.getParallelTaxonomyArrays().parents().length);
    
    reader.close();
    writer.close();
    dir.close();
  }
 
  @Test
  public void testOpenIfChangedReuseAfterRecreate() throws Exception {
    // tests that if the taxonomy is recreated, no data is reused from the previous taxonomy
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(dir);
    FacetLabel cp_a = new FacetLabel("a");
    writer.addCategory(cp_a);
    writer.close();
    
    DirectoryTaxonomyReader r1 = new DirectoryTaxonomyReader(dir);
    // fill r1's caches
    assertEquals(1, r1.getOrdinal(cp_a));
    assertEquals(cp_a, r1.getPath(1));
    
    // now recreate, add a different category
    writer = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE);
    FacetLabel cp_b = new FacetLabel("b");
    writer.addCategory(cp_b);
    writer.close();
    
    DirectoryTaxonomyReader r2 = TaxonomyReader.openIfChanged(r1);
    assertNotNull(r2);
    
    // fill r2's caches
    assertEquals(1, r2.getOrdinal(cp_b));
    assertEquals(cp_b, r2.getPath(1));
    
    // check that r1 doesn't see cp_b
    assertEquals(TaxonomyReader.INVALID_ORDINAL, r1.getOrdinal(cp_b));
    assertEquals(cp_a, r1.getPath(1));

    // check that r2 doesn't see cp_a
    assertEquals(TaxonomyReader.INVALID_ORDINAL, r2.getOrdinal(cp_a));
    assertEquals(cp_b, r2.getPath(1));

    r2.close();
    r1.close();
    dir.close();
  }
  
  @Test
  public void testOpenIfChangedReuse() throws Exception {
    // test the reuse of data from the old DTR instance
    for (boolean nrt : new boolean[] {false, true}) {
      Directory dir = newDirectory();
      DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(dir);
      
      FacetLabel cp_a = new FacetLabel("a");
      writer.addCategory(cp_a);
      if (!nrt) writer.commit();
      
      DirectoryTaxonomyReader r1 = nrt ? new DirectoryTaxonomyReader(writer) : new DirectoryTaxonomyReader(dir);
      // fill r1's caches
      assertEquals(1, r1.getOrdinal(cp_a));
      assertEquals(cp_a, r1.getPath(1));
      
      FacetLabel cp_b = new FacetLabel("b");
      writer.addCategory(cp_b);
      if (!nrt) writer.commit();
      
      DirectoryTaxonomyReader r2 = TaxonomyReader.openIfChanged(r1);
      assertNotNull(r2);
      
      // add r2's categories to the caches
      assertEquals(2, r2.getOrdinal(cp_b));
      assertEquals(cp_b, r2.getPath(2));
      
      // check that r1 doesn't see cp_b
      assertEquals(TaxonomyReader.INVALID_ORDINAL, r1.getOrdinal(cp_b));
      assertNull(r1.getPath(2));
      
      r1.close();
      r2.close();
      writer.close();
      dir.close();
    }
  }
  
  @Test
  public void testOpenIfChangedReplaceTaxonomy() throws Exception {
    // test openIfChanged when replaceTaxonomy is called, which is equivalent to recreate
    // only can work with NRT as well
    Directory src = newDirectory();
    DirectoryTaxonomyWriter w = new DirectoryTaxonomyWriter(src);
    FacetLabel cp_b = new FacetLabel("b");
    w.addCategory(cp_b);
    w.close();
    
    for (boolean nrt : new boolean[] {false, true}) {
      Directory dir = newDirectory();
      DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(dir);
      
      FacetLabel cp_a = new FacetLabel("a");
      writer.addCategory(cp_a);
      if (!nrt) writer.commit();
      
      DirectoryTaxonomyReader r1 = nrt ? new DirectoryTaxonomyReader(writer) : new DirectoryTaxonomyReader(dir);
      // fill r1's caches
      assertEquals(1, r1.getOrdinal(cp_a));
      assertEquals(cp_a, r1.getPath(1));

      // now replace taxonomy
      writer.replaceTaxonomy(src);
      if (!nrt) writer.commit();
      
      DirectoryTaxonomyReader r2 = TaxonomyReader.openIfChanged(r1);
      assertNotNull(r2);
      
      // fill r2's caches
      assertEquals(1, r2.getOrdinal(cp_b));
      assertEquals(cp_b, r2.getPath(1));
      
      // check that r1 doesn't see cp_b
      assertEquals(TaxonomyReader.INVALID_ORDINAL, r1.getOrdinal(cp_b));
      assertEquals(cp_a, r1.getPath(1));

      // check that r2 doesn't see cp_a
      assertEquals(TaxonomyReader.INVALID_ORDINAL, r2.getOrdinal(cp_a));
      assertEquals(cp_b, r2.getPath(1));

      r2.close();
      r1.close();
      writer.close();
      dir.close();
    }
    
    src.close();
  }

  @Test
  public void testGetChildren() throws Exception {
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir);
    int numCategories = atLeast(10);
    int numA = 0, numB = 0;
    Random random = random();
    // add the two categories for which we'll also add children (so asserts are simpler)
    taxoWriter.addCategory(new FacetLabel("a"));
    taxoWriter.addCategory(new FacetLabel("b"));
    for (int i = 0; i < numCategories; i++) {
      if (random.nextBoolean()) {
        taxoWriter.addCategory(new FacetLabel("a", Integer.toString(i)));
        ++numA;
      } else {
        taxoWriter.addCategory(new FacetLabel("b", Integer.toString(i)));
        ++numB;
      }
    }
    // add category with no children
    taxoWriter.addCategory(new FacetLabel("c"));
    taxoWriter.close();
    
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(dir);

    // non existing category
    ChildrenIterator it = taxoReader.getChildren(taxoReader.getOrdinal(new FacetLabel("invalid")));
    assertEquals(TaxonomyReader.INVALID_ORDINAL, it.next());

    // a category with no children
    it = taxoReader.getChildren(taxoReader.getOrdinal(new FacetLabel("c")));
    assertEquals(TaxonomyReader.INVALID_ORDINAL, it.next());

    // arbitrary negative ordinal
    it = taxoReader.getChildren(-2);
    assertEquals(TaxonomyReader.INVALID_ORDINAL, it.next());

    // root's children
    Set<String> roots = new HashSet<>(Arrays.asList("a", "b", "c"));
    it = taxoReader.getChildren(TaxonomyReader.ROOT_ORDINAL);
    while (!roots.isEmpty()) {
      FacetLabel root = taxoReader.getPath(it.next());
      assertEquals(1, root.length);
      assertTrue(roots.remove(root.components[0]));
    }
    assertEquals(TaxonomyReader.INVALID_ORDINAL, it.next());
    
    for (int i = 0; i < 2; i++) {
      FacetLabel cp = i == 0 ? new FacetLabel("a") : new FacetLabel("b");
      int ordinal = taxoReader.getOrdinal(cp);
      it = taxoReader.getChildren(ordinal);
      int numChildren = 0;
      int child;
      while ((child = it.next()) != TaxonomyReader.INVALID_ORDINAL) {
        FacetLabel path = taxoReader.getPath(child);
        assertEquals(2, path.length);
        assertEquals(path.components[0], i == 0 ? "a" : "b");
        ++numChildren;
      }
      int expected = i == 0 ? numA : numB;
      assertEquals("invalid num children", expected, numChildren);
    }
    taxoReader.close();
    
    dir.close();
  }
  
}
