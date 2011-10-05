package org.apache.lucene.facet.taxonomy.lucene;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
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

public class TestLuceneTaxonomyReader extends LuceneTestCase {

  @Test
  public void testCloseAfterIncRef() throws Exception {
    Directory dir = newDirectory();
    LuceneTaxonomyWriter ltw = new LuceneTaxonomyWriter(dir);
    ltw.addCategory(new CategoryPath("a"));
    ltw.close();
    
    LuceneTaxonomyReader ltr = new LuceneTaxonomyReader(dir);
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
    LuceneTaxonomyWriter ltw = new LuceneTaxonomyWriter(dir);
    ltw.addCategory(new CategoryPath("a"));
    ltw.close();
    
    LuceneTaxonomyReader ltr = new LuceneTaxonomyReader(dir);
    ltr.close();
    ltr.close(); // no exception should be thrown
    
    dir.close();
  }
  
  @Test
  public void testAlreadyClosed() throws Exception {
    Directory dir = newDirectory();
    LuceneTaxonomyWriter ltw = new LuceneTaxonomyWriter(dir);
    ltw.addCategory(new CategoryPath("a"));
    ltw.close();
    
    LuceneTaxonomyReader ltr = new LuceneTaxonomyReader(dir);
    ltr.close();
    try {
      ltr.getSize();
      fail("An AlreadyClosedException should have been thrown here");
    } catch (AlreadyClosedException ace) {
      // good!
    }
    dir.close();
  }
  
}
