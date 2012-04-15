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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FieldCacheSanityChecker.Insanity;
import org.apache.lucene.util.FieldCacheSanityChecker.InsanityType;

import java.io.IOException;

public class TestFieldCacheSanityChecker extends LuceneTestCase {

  protected AtomicReader readerA;
  protected AtomicReader readerB;
  protected AtomicReader readerX;
  protected AtomicReader readerAclone;
  protected Directory dirA, dirB;
  private static final int NUM_DOCS = 1000;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dirA = newDirectory();
    dirB = newDirectory();

    IndexWriter wA = new IndexWriter(dirA, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    IndexWriter wB = new IndexWriter(dirB, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    long theLong = Long.MAX_VALUE;
    double theDouble = Double.MAX_VALUE;
    byte theByte = Byte.MAX_VALUE;
    short theShort = Short.MAX_VALUE;
    int theInt = Integer.MAX_VALUE;
    float theFloat = Float.MAX_VALUE;
    for (int i = 0; i < NUM_DOCS; i++){
      Document doc = new Document();
      doc.add(newField("theLong", String.valueOf(theLong--), StringField.TYPE_UNSTORED));
      doc.add(newField("theDouble", String.valueOf(theDouble--), StringField.TYPE_UNSTORED));
      doc.add(newField("theByte", String.valueOf(theByte--), StringField.TYPE_UNSTORED));
      doc.add(newField("theShort", String.valueOf(theShort--), StringField.TYPE_UNSTORED));
      doc.add(newField("theInt", String.valueOf(theInt--), StringField.TYPE_UNSTORED));
      doc.add(newField("theFloat", String.valueOf(theFloat--), StringField.TYPE_UNSTORED));
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

    cache.getDoubles(readerA, "theDouble", false);
    cache.getDoubles(readerA, "theDouble", FieldCache.DEFAULT_DOUBLE_PARSER, false);
    cache.getDoubles(readerAclone, "theDouble", FieldCache.DEFAULT_DOUBLE_PARSER, false);
    cache.getDoubles(readerB, "theDouble", FieldCache.DEFAULT_DOUBLE_PARSER, false);

    cache.getInts(readerX, "theInt", false);
    cache.getInts(readerX, "theInt", FieldCache.DEFAULT_INT_PARSER, false);

    // // // 

    Insanity[] insanity = 
      FieldCacheSanityChecker.checkSanity(cache.getCacheEntries());
    
    if (0 < insanity.length)
      dumpArray(getTestLabel() + " INSANITY", insanity, System.err);

    assertEquals("shouldn't be any cache insanity", 0, insanity.length);
    cache.purgeAllCaches();
  }

  public void testInsanity1() throws IOException {
    FieldCache cache = FieldCache.DEFAULT;
    cache.purgeAllCaches();

    cache.getInts(readerX, "theInt", FieldCache.DEFAULT_INT_PARSER, false);
    cache.getTerms(readerX, "theInt");
    cache.getBytes(readerX, "theByte", false);

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

    cache.getTerms(readerA, "theString");
    cache.getTerms(readerB, "theString");
    cache.getTerms(readerX, "theString");

    cache.getBytes(readerX, "theByte", false);


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
