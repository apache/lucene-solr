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
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.DiskOrdinalMap;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.MemoryOrdinalMap;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.OrdinalMap;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

@LuceneTestCase.SuppressCodecs("SimpleText")
public class TestAddTaxonomy extends FacetTestCase {

  private void dotest(int ncats, final int range) throws Exception {
    final AtomicInteger numCats = new AtomicInteger(ncats);
    Directory dirs[] = new Directory[2];
    for (int i = 0; i < dirs.length; i++) {
      dirs[i] = newDirectory();
      final DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(dirs[i]);
      Thread[] addThreads = new Thread[4];
      for (int j = 0; j < addThreads.length; j++) {
        addThreads[j] = new Thread() {
          @Override
          public void run() {
            Random random = random();
            while (numCats.decrementAndGet() > 0) {
              String cat = Integer.toString(random.nextInt(range));
              try {
                tw.addCategory(new FacetLabel("a", cat));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          }
        };
      }
      
      for (Thread t : addThreads) t.start();
      for (Thread t : addThreads) t.join();
      tw.close();
    }

    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(dirs[0]);
    OrdinalMap map = randomOrdinalMap();
    tw.addTaxonomy(dirs[1], map);
    tw.close();
    
    validate(dirs[0], dirs[1], map);
    
    IOUtils.close(dirs);
  }
  
  private OrdinalMap randomOrdinalMap() throws IOException {
    if (random().nextBoolean()) {
      return new DiskOrdinalMap(createTempFile("taxoMap", ""));
    } else {
      return new MemoryOrdinalMap();
    }
  }

  private void validate(Directory dest, Directory src, OrdinalMap ordMap) throws Exception {
    DirectoryTaxonomyReader destTR = new DirectoryTaxonomyReader(dest);
    try {
      final int destSize = destTR.getSize();
      DirectoryTaxonomyReader srcTR = new DirectoryTaxonomyReader(src);
      try {
        int[] map = ordMap.getMap();
        
        // validate taxo sizes
        int srcSize = srcTR.getSize();
        assertTrue("destination taxonomy expected to be larger than source; dest="
            + destSize + " src=" + srcSize,
            destSize >= srcSize);
        
        // validate that all source categories exist in destination, and their
        // ordinals are as expected.
        for (int j = 1; j < srcSize; j++) {
          FacetLabel cp = srcTR.getPath(j);
          int destOrdinal = destTR.getOrdinal(cp);
          assertTrue(cp + " not found in destination", destOrdinal > 0);
          assertEquals(destOrdinal, map[j]);
        }
      } finally {
        srcTR.close();
      }
    } finally {
      destTR.close();
    }
  }

  public void testAddEmpty() throws Exception {
    Directory dest = newDirectory();
    DirectoryTaxonomyWriter destTW = new DirectoryTaxonomyWriter(dest);
    destTW.addCategory(new FacetLabel("Author", "Rob Pike"));
    destTW.addCategory(new FacetLabel("Aardvarks", "Bob"));
    destTW.commit();
    
    Directory src = newDirectory();
    new DirectoryTaxonomyWriter(src).close(); // create an empty taxonomy
    
    OrdinalMap map = randomOrdinalMap();
    destTW.addTaxonomy(src, map);
    destTW.close();
    
    validate(dest, src, map);
    
    IOUtils.close(dest, src);
  }
  
  public void testAddToEmpty() throws Exception {
    Directory dest = newDirectory();
    
    Directory src = newDirectory();
    DirectoryTaxonomyWriter srcTW = new DirectoryTaxonomyWriter(src);
    srcTW.addCategory(new FacetLabel("Author", "Rob Pike"));
    srcTW.addCategory(new FacetLabel("Aardvarks", "Bob"));
    srcTW.close();
    
    DirectoryTaxonomyWriter destTW = new DirectoryTaxonomyWriter(dest);
    OrdinalMap map = randomOrdinalMap();
    destTW.addTaxonomy(src, map);
    destTW.close();
    
    validate(dest, src, map);
    
    IOUtils.close(dest, src);
  }
  
  // A more comprehensive and big random test.
  public void testBig() throws Exception {
    dotest(200, 10000);
    dotest(1000, 20000);
    dotest(400000, 1000000);
  }

  // a reasonable random test
  public void testMedium() throws Exception {
    Random random = random();
    int numTests = atLeast(3);
    for (int i = 0; i < numTests; i++) {
      dotest(TestUtil.nextInt(random, 2, 100),
             TestUtil.nextInt(random, 100, 1000));
    }
  }
  
  public void testSimple() throws Exception {
    Directory dest = newDirectory();
    DirectoryTaxonomyWriter tw1 = new DirectoryTaxonomyWriter(dest);
    tw1.addCategory(new FacetLabel("Author", "Mark Twain"));
    tw1.addCategory(new FacetLabel("Animals", "Dog"));
    tw1.addCategory(new FacetLabel("Author", "Rob Pike"));
    
    Directory src = newDirectory();
    DirectoryTaxonomyWriter tw2 = new DirectoryTaxonomyWriter(src);
    tw2.addCategory(new FacetLabel("Author", "Rob Pike"));
    tw2.addCategory(new FacetLabel("Aardvarks", "Bob"));
    tw2.close();

    OrdinalMap map = randomOrdinalMap();

    tw1.addTaxonomy(src, map);
    tw1.close();

    validate(dest, src, map);
    
    IOUtils.close(dest, src);
  }

  public void testConcurrency() throws Exception {
    // tests that addTaxonomy and addCategory work in parallel
    final int numCategories = atLeast(10000);
    
    // build an input taxonomy index
    Directory src = newDirectory();
    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(src);
    for (int i = 0; i < numCategories; i++) {
      tw.addCategory(new FacetLabel("a", Integer.toString(i)));
    }
    tw.close();
    
    // now add the taxonomy to an empty taxonomy, while adding the categories
    // again, in parallel -- in the end, no duplicate categories should exist.
    Directory dest = newDirectory();
    final DirectoryTaxonomyWriter destTW = new DirectoryTaxonomyWriter(dest);
    Thread t = new Thread() {
      @Override
      public void run() {
        for (int i = 0; i < numCategories; i++) {
          try {
            destTW.addCategory(new FacetLabel("a", Integer.toString(i)));
          } catch (IOException e) {
            // shouldn't happen - if it does, let the test fail on uncaught exception.
            throw new RuntimeException(e);
          }
        }
      }
    };
    t.start();
    
    OrdinalMap map = new MemoryOrdinalMap();
    destTW.addTaxonomy(src, map);
    t.join();
    destTW.close();
    
    // now validate
    
    DirectoryTaxonomyReader dtr = new DirectoryTaxonomyReader(dest);
    // +2 to account for the root category + "a"
    assertEquals(numCategories + 2, dtr.getSize());
    HashSet<FacetLabel> categories = new HashSet<>();
    for (int i = 1; i < dtr.getSize(); i++) {
      FacetLabel cat = dtr.getPath(i);
      assertTrue("category " + cat + " already existed", categories.add(cat));
    }
    dtr.close();
    
    IOUtils.close(src, dest);
  }
  
}
