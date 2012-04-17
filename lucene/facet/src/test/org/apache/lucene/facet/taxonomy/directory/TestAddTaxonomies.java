package org.apache.lucene.facet.taxonomy.directory;

import java.io.File;

import org.apache.lucene.store.Directory;
import org.junit.Test;

import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.DiskOrdinalMap;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.MemoryOrdinalMap;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.OrdinalMap;

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

public class TestAddTaxonomies extends LuceneTestCase {

  @Test
  public void test1() throws Exception {
    Directory dir1 = newDirectory();
    DirectoryTaxonomyWriter tw1 = new DirectoryTaxonomyWriter(dir1);
    tw1.addCategory(new CategoryPath("Author", "Mark Twain"));
    tw1.addCategory(new CategoryPath("Animals", "Dog"));
    Directory dir2 = newDirectory();
    DirectoryTaxonomyWriter tw2 = new DirectoryTaxonomyWriter(dir2);
    tw2.addCategory(new CategoryPath("Author", "Rob Pike"));
    tw2.addCategory(new CategoryPath("Aardvarks", "Bob"));
    tw2.close();
    Directory dir3 = newDirectory();
    DirectoryTaxonomyWriter tw3 = new DirectoryTaxonomyWriter(dir3);
    tw3.addCategory(new CategoryPath("Author", "Zebra Smith"));
    tw3.addCategory(new CategoryPath("Aardvarks", "Bob"));
    tw3.addCategory(new CategoryPath("Aardvarks", "Aaron"));
    tw3.close();

    MemoryOrdinalMap[] maps = new MemoryOrdinalMap[2];
    maps[0] = new MemoryOrdinalMap();
    maps[1] = new MemoryOrdinalMap();

    tw1.addTaxonomies(new Directory[] { dir2, dir3 }, maps);
    tw1.close();

    TaxonomyReader tr = new DirectoryTaxonomyReader(dir1);

    // Test that the merged taxonomy now contains what we expect:
    // First all the categories of the original taxonomy, in their original order:
    assertEquals(tr.getPath(0).toString(), "");
    assertEquals(tr.getPath(1).toString(), "Author");
    assertEquals(tr.getPath(2).toString(), "Author/Mark Twain");
    assertEquals(tr.getPath(3).toString(), "Animals");
    assertEquals(tr.getPath(4).toString(), "Animals/Dog");
    // Then the categories new in the new taxonomy, in alphabetical order: 
    assertEquals(tr.getPath(5).toString(), "Aardvarks");
    assertEquals(tr.getPath(6).toString(), "Aardvarks/Aaron");
    assertEquals(tr.getPath(7).toString(), "Aardvarks/Bob");
    assertEquals(tr.getPath(8).toString(), "Author/Rob Pike");
    assertEquals(tr.getPath(9).toString(), "Author/Zebra Smith");
    assertEquals(tr.getSize(), 10);

    // Test that the maps contain what we expect
    int[] map0 = maps[0].getMap();
    assertEquals(5, map0.length);
    assertEquals(0, map0[0]);
    assertEquals(1, map0[1]);
    assertEquals(8, map0[2]);
    assertEquals(5, map0[3]);
    assertEquals(7, map0[4]);

    int[] map1 = maps[1].getMap();
    assertEquals(6, map1.length);
    assertEquals(0, map1[0]);
    assertEquals(1, map1[1]);
    assertEquals(9, map1[2]);
    assertEquals(5, map1[3]);
    assertEquals(7, map1[4]);
    assertEquals(6, map1[5]);
    
    tr.close();
    dir1.close();
    dir2.close();
    dir3.close();
  }

  // a reasonable random test
  public void testmedium() throws Exception {
    int numTests = atLeast(3);
    for (int i = 0; i < numTests; i++) {
      dotest(_TestUtil.nextInt(random(), 1, 10), 
             _TestUtil.nextInt(random(), 1, 100), 
             _TestUtil.nextInt(random(), 100, 1000),
             random().nextBoolean());
    }
  }

  // A more comprehensive and big random test.
  @Test @Nightly
  public void testbig() throws Exception {
    dotest(2, 1000, 5000, false);
    dotest(10, 10000, 100, false);
    dotest(50, 20, 100, false);
    dotest(10, 1000, 10000, false);
    dotest(50, 20, 10000, false);
    dotest(1, 20, 10000, false);
    dotest(10, 1, 10000, false);
    dotest(10, 1000, 20000, true);
  }

  private void dotest(int ntaxonomies, int ncats, int range, boolean disk) throws Exception {
    Directory dirs[] = new Directory[ntaxonomies];
    Directory copydirs[] = new Directory[ntaxonomies];

    for (int i=0; i<ntaxonomies; i++) {
      dirs[i] = newDirectory();
      copydirs[i] = newDirectory();
      DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(dirs[i]);
      DirectoryTaxonomyWriter copytw = new DirectoryTaxonomyWriter(copydirs[i]);
      for (int j=0; j<ncats; j++) {
        String cat = Integer.toString(random().nextInt(range));
        tw.addCategory(new CategoryPath("a",cat));
        copytw.addCategory(new CategoryPath("a",cat));
      }
      // System.err.println("Taxonomy "+i+": "+tw.getSize());
      tw.close();
      copytw.close();
    }

    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(dirs[0]);
    Directory otherdirs[] = new Directory[ntaxonomies-1];
    System.arraycopy(dirs, 1, otherdirs, 0, ntaxonomies-1);

    OrdinalMap[] maps = new OrdinalMap[ntaxonomies-1];
    if (ntaxonomies>1) {
      for (int i=0; i<ntaxonomies-1; i++) {
        if (disk) {
          // TODO: use a LTC tempfile
          maps[i] = new DiskOrdinalMap(new File(System.getProperty("java.io.tmpdir"),
              "tmpmap"+i));
        } else {
          maps[i] = new MemoryOrdinalMap();
        }
      }
    }

    tw.addTaxonomies(otherdirs, maps);
    // System.err.println("Merged axonomy: "+tw.getSize());
    tw.close();

    // Check that all original categories in the main taxonomy remain in
    // unchanged, and the rest of the taxonomies are completely unchanged.
    for (int i=0; i<ntaxonomies; i++) {
      TaxonomyReader tr = new DirectoryTaxonomyReader(dirs[i]);
      TaxonomyReader copytr = new DirectoryTaxonomyReader(copydirs[i]);
      if (i==0) {
        assertTrue(tr.getSize() >= copytr.getSize());
      } else {
        assertEquals(copytr.getSize(), tr.getSize());
      }
      for (int j=0; j<copytr.getSize(); j++) {
        String expected = copytr.getPath(j).toString();
        String got = tr.getPath(j).toString();
        assertTrue("Comparing category "+j+" of taxonomy "+i+": expected "+expected+", got "+got,
            expected.equals(got));
      }
      tr.close();
      copytr.close();
    }

    // Check that all the new categories in the main taxonomy are in
    // lexicographic order. This isn't a requirement of our API, but happens
    // this way in our current implementation.
    TaxonomyReader tr = new DirectoryTaxonomyReader(dirs[0]);
    TaxonomyReader copytr = new DirectoryTaxonomyReader(copydirs[0]);
    if (tr.getSize() > copytr.getSize()) {
      String prev = tr.getPath(copytr.getSize()).toString();
      for (int j=copytr.getSize()+1; j<tr.getSize(); j++) {
        String n = tr.getPath(j).toString();
        assertTrue(prev.compareTo(n)<0);
        prev=n;
      }
    }
    int oldsize = copytr.getSize(); // remember for later
    tr.close();
    copytr.close();

    // Check that all the categories from other taxonomies exist in the new
    // taxonomy.
    TaxonomyReader main = new DirectoryTaxonomyReader(dirs[0]);
    for (int i=1; i<ntaxonomies; i++) {
      TaxonomyReader other = new DirectoryTaxonomyReader(dirs[i]);
      for (int j=0; j<other.getSize(); j++) {
        int otherord = main.getOrdinal(other.getPath(j));
        assertTrue(otherord != TaxonomyReader.INVALID_ORDINAL);
      }
      other.close();
    }

    // Check that all the new categories in the merged taxonomy exist in
    // one of the added taxonomies.
    TaxonomyReader[] others = new TaxonomyReader[ntaxonomies-1]; 
    for (int i=1; i<ntaxonomies; i++) {
      others[i-1] = new DirectoryTaxonomyReader(dirs[i]);
    }
    for (int j=oldsize; j<main.getSize(); j++) {
      boolean found=false;
      CategoryPath path = main.getPath(j);
      for (int i=1; i<ntaxonomies; i++) {
        if (others[i-1].getOrdinal(path) != TaxonomyReader.INVALID_ORDINAL) {
          found=true;
          break;
        }
      }
      if (!found) {
        fail("Found category "+j+" ("+path+") in merged taxonomy not in any of the separate ones");
      }
    }

    // Check that all the maps are correct
    for (int i=0; i<ntaxonomies-1; i++) {
      int[] map = maps[i].getMap();
      for (int j=0; j<map.length; j++) {
        assertEquals(map[j], main.getOrdinal(others[i].getPath(j)));
      }
    }

    for (int i=1; i<ntaxonomies; i++) {
      others[i-1].close();
    }

    main.close();
    IOUtils.close(dirs);
    IOUtils.close(copydirs);
  }

}
