package org.apache.lucene.codecs.pulsing;

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

import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.nestedpulsing.NestedPulsingPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Tests that pulsing codec reuses its enums and wrapped enums
 */
public class TestPulsingReuse extends LuceneTestCase {
  // TODO: this is a basic test. this thing is complicated, add more
  public void testSophisticatedReuse() throws Exception {
    // we always run this test with pulsing codec.
    Codec cp = _TestUtil.alwaysPostingsFormat(new Pulsing40PostingsFormat(1));
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setCodec(cp));
    Document doc = new Document();
    doc.add(new Field("foo", "a b b c c c d e f g g h i i j j k", TextField.TYPE_UNSTORED));
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();
    iw.close();
    
    AtomicReader segment = getOnlySegmentReader(ir);
    DocsEnum reuse = null;
    Map<DocsEnum,Boolean> allEnums = new IdentityHashMap<DocsEnum,Boolean>();
    TermsEnum te = segment.terms("foo").iterator(null);
    while (te.next() != null) {
      reuse = te.docs(null, reuse, false);
      allEnums.put(reuse, true);
    }
    
    assertEquals(2, allEnums.size());
    
    allEnums.clear();
    DocsAndPositionsEnum posReuse = null;
    te = segment.terms("foo").iterator(null);
    while (te.next() != null) {
      posReuse = te.docsAndPositions(null, posReuse, false);
      allEnums.put(posReuse, true);
    }
    
    assertEquals(2, allEnums.size());
    
    ir.close();
    dir.close();
  }
  
  /** tests reuse with Pulsing1(Pulsing2(Standard)) */
  public void testNestedPulsing() throws Exception {
    // we always run this test with pulsing codec.
    Codec cp = _TestUtil.alwaysPostingsFormat(new NestedPulsingPostingsFormat());
    MockDirectoryWrapper dir = newDirectory();
    dir.setCheckIndexOnClose(false); // will do this ourselves, custom codec
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setCodec(cp));
    Document doc = new Document();
    doc.add(new Field("foo", "a b b c c c d e f g g g h i i j j k l l m m m", TextField.TYPE_UNSTORED));
    // note: the reuse is imperfect, here we would have 4 enums (lost reuse when we get an enum for 'm')
    // this is because we only track the 'last' enum we reused (not all).
    // but this seems 'good enough' for now.
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();
    iw.close();
    
    AtomicReader segment = getOnlySegmentReader(ir);
    DocsEnum reuse = null;
    Map<DocsEnum,Boolean> allEnums = new IdentityHashMap<DocsEnum,Boolean>();
    TermsEnum te = segment.terms("foo").iterator(null);
    while (te.next() != null) {
      reuse = te.docs(null, reuse, false);
      allEnums.put(reuse, true);
    }
    
    assertEquals(4, allEnums.size());
    
    allEnums.clear();
    DocsAndPositionsEnum posReuse = null;
    te = segment.terms("foo").iterator(null);
    while (te.next() != null) {
      posReuse = te.docsAndPositions(null, posReuse, false);
      allEnums.put(posReuse, true);
    }
    
    assertEquals(4, allEnums.size());
    
    ir.close();
    CheckIndex ci = new CheckIndex(dir);
    ci.checkIndex(null);
    dir.close();
  }
}
