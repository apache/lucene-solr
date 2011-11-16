package org.apache.lucene.facet.taxonomy.directory;

import java.util.Random;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.InconsistentTaxonomyException;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

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

public class TestDirectoryTaxonomyReader extends LuceneTestCase {

  @Test
  public void testCloseAfterIncRef() throws Exception {
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter ltw = new DirectoryTaxonomyWriter(dir);
    ltw.addCategory(new CategoryPath("a"));
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
    ltw.addCategory(new CategoryPath("a"));
    ltw.close();
    
    DirectoryTaxonomyReader ltr = new DirectoryTaxonomyReader(dir);
    ltr.close();
    ltr.close(); // no exception should be thrown
    
    dir.close();
  }
  
  /**
   * Test the boolean returned by TR.refresh
   * @throws Exception
   */
  @Test
  public void testReaderRefreshResult() throws Exception {
    Directory dir = null;
    DirectoryTaxonomyWriter ltw = null;
    DirectoryTaxonomyReader ltr = null;
    
    try {
      dir = newDirectory();
      ltw = new DirectoryTaxonomyWriter(dir);
      
      ltw.addCategory(new CategoryPath("a"));
      ltw.commit();
      
      ltr = new DirectoryTaxonomyReader(dir);
      assertFalse("Nothing has changed",ltr.refresh());
      
      ltw.addCategory(new CategoryPath("b"));
      ltw.commit();
      
      assertTrue("changes were committed",ltr.refresh());
      assertFalse("Nothing has changed",ltr.refresh());
    } finally {
      IOUtils.close(ltw, ltr, dir);
    }
  }
  
  @Test
  public void testAlreadyClosed() throws Exception {
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter ltw = new DirectoryTaxonomyWriter(dir);
    ltw.addCategory(new CategoryPath("a"));
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
    doTestReadRecreatedTaxono(random, true);
  }
  
  /**
   * recreating a taxonomy should work well with a refreshed taxonomy reader 
   */
  @Test
  public void testRefreshReadRecreatedTaxonomy() throws Exception {
    doTestReadRecreatedTaxono(random, false);
  }
  
  private void doTestReadRecreatedTaxono(Random random, boolean closeReader) throws Exception {
    Directory dir = null;
    TaxonomyWriter tw = null;
    TaxonomyReader tr = null;
    
    // prepare a few categories
    int  n = 10;
    CategoryPath[] cp = new CategoryPath[n];
    for (int i=0; i<n; i++) {
      cp[i] = new CategoryPath("a", Integer.toString(i));
    }
    
    try {
      dir = newDirectory();
      
      tw = new DirectoryTaxonomyWriter(dir);
      tw.addCategory(new CategoryPath("a"));
      tw.close();
      
      tr = new DirectoryTaxonomyReader(dir);
      int baseNumcategories = tr.getSize();
      
      for (int i=0; i<n; i++) {
        int k = random.nextInt(n);
        tw = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE);
        for (int j=0; j<=k; j++) {
          tw.addCategory(new CategoryPath(cp[j]));
        }
        tw.close();
        if (closeReader) {
          tr.close();
          tr = new DirectoryTaxonomyReader(dir);
        } else {
          try {
            tr.refresh();
            fail("Expected InconsistentTaxonomyException");
          } catch (InconsistentTaxonomyException e) {
            tr.close();
            tr = new DirectoryTaxonomyReader(dir);
          }
        }
        assertEquals("Wrong #categories in taxonomy (i="+i+", k="+k+")", baseNumcategories + 1 + k, tr.getSize());
      }
    } finally {
      IOUtils.close(tr, tw, dir);
    }
  }
  
}
