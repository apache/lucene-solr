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
package org.apache.lucene.uninverting;

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.uninverting.FieldCacheSanityChecker.Insanity;
import org.apache.lucene.uninverting.FieldCacheSanityChecker.InsanityType;
import org.apache.lucene.util.LuceneTestCase;

public class TestFieldCacheSanityChecker extends LuceneTestCase {

  protected LeafReader readerA;
  protected LeafReader readerB;
  protected LeafReader readerX;
  protected LeafReader readerAclone;
  protected Directory dirA, dirB;
  private static final int NUM_DOCS = 1000;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dirA = newDirectory();
    dirB = newDirectory();

    IndexWriter wA = new IndexWriter(dirA, newIndexWriterConfig(new MockAnalyzer(random())));
    IndexWriter wB = new IndexWriter(dirB, newIndexWriterConfig(new MockAnalyzer(random())));

    long theLong = Long.MAX_VALUE;
    double theDouble = Double.MAX_VALUE;
    int theInt = Integer.MAX_VALUE;
    float theFloat = Float.MAX_VALUE;
    for (int i = 0; i < NUM_DOCS; i++){
      Document doc = new Document();
      doc.add(new LongField("theLong", theLong--, Field.Store.NO));
      doc.add(new DoubleField("theDouble", theDouble--, Field.Store.NO));
      doc.add(new IntField("theInt", theInt--, Field.Store.NO));
      doc.add(new FloatField("theFloat", theFloat--, Field.Store.NO));
      if (0 == i % 3) {
        wA.addDocument(doc);
      } else {
        wB.addDocument(doc);
      }
    }
    wA.close();
    wB.close();
    DirectoryReader rA = DirectoryReader.open(dirA);
    readerA = SlowCompositeReaderWrapper.wrap(rA);
    readerAclone = SlowCompositeReaderWrapper.wrap(rA);
    readerA = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dirA));
    readerB = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dirB));
    readerX = SlowCompositeReaderWrapper.wrap(new MultiReader(readerA, readerB));
  }

  @Override
  public void tearDown() throws Exception {
    readerA.close();
    readerAclone.close();
    readerB.close();
    readerX.close();
    dirA.close();
    dirB.close();
    super.tearDown();
  }

  public void testSanity() throws IOException {
    FieldCache cache = FieldCache.DEFAULT;
    cache.purgeAllCaches();

    cache.getNumerics(readerA, "theDouble", FieldCache.NUMERIC_UTILS_DOUBLE_PARSER, false);
    cache.getNumerics(readerAclone, "theDouble", FieldCache.NUMERIC_UTILS_DOUBLE_PARSER, false);
    cache.getNumerics(readerB, "theDouble", FieldCache.NUMERIC_UTILS_DOUBLE_PARSER, false);

    cache.getNumerics(readerX, "theInt", FieldCache.NUMERIC_UTILS_INT_PARSER, false);

    // // // 

    Insanity[] insanity = 
      FieldCacheSanityChecker.checkSanity(cache.getCacheEntries());
    
    if (0 < insanity.length)
      dumpArray(getTestClass().getName() + "#" + getTestName() 
          + " INSANITY", insanity, System.err);

    assertEquals("shouldn't be any cache insanity", 0, insanity.length);
    cache.purgeAllCaches();
  }

  public void testInsanity1() throws IOException {
    FieldCache cache = FieldCache.DEFAULT;
    cache.purgeAllCaches();

    cache.getNumerics(readerX, "theInt", FieldCache.NUMERIC_UTILS_INT_PARSER, false);
    cache.getTerms(readerX, "theInt", false);

    // // // 

    Insanity[] insanity = 
      FieldCacheSanityChecker.checkSanity(cache.getCacheEntries());

    assertEquals("wrong number of cache errors", 1, insanity.length);
    assertEquals("wrong type of cache error", 
                 InsanityType.VALUEMISMATCH,
                 insanity[0].getType());
    assertEquals("wrong number of entries in cache error", 2,
                 insanity[0].getCacheEntries().length);

    // we expect bad things, don't let tearDown complain about them
    cache.purgeAllCaches();
  }

  public void testInsanity2() throws IOException {
    FieldCache cache = FieldCache.DEFAULT;
    cache.purgeAllCaches();

    cache.getTerms(readerA, "theInt", false);
    cache.getTerms(readerB, "theInt", false);
    cache.getTerms(readerX, "theInt", false);


    // // // 

    Insanity[] insanity = 
      FieldCacheSanityChecker.checkSanity(cache.getCacheEntries());
    
    assertEquals("wrong number of cache errors", 1, insanity.length);
    assertEquals("wrong type of cache error", 
                 InsanityType.SUBREADER,
                 insanity[0].getType());
    assertEquals("wrong number of entries in cache error", 3,
                 insanity[0].getCacheEntries().length);

    // we expect bad things, don't let tearDown complain about them
    cache.purgeAllCaches();
  }

}
