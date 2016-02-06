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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;



public class TestReaderWrapperDVTypeCheck extends LuceneTestCase {
  
  public void testNoDVFieldOnSegment() throws IOException{
    Directory dir = newDirectory();
    IndexWriterConfig cfg = new IndexWriterConfig(new MockAnalyzer(random())).setCodec(TestUtil.alwaysDocValuesFormat(TestUtil.getDefaultDocValuesFormat()));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, cfg);
    
    boolean sdvExist = false;
    boolean ssdvExist = false;
    
    final long seed = random().nextLong();
    {
      final Random indexRandom = new Random(seed);
      final int docs;
      docs = indexRandom.nextInt(4);
     // System.out.println("docs:"+docs);
      
      for(int i=0; i< docs; i++){
        Document d = new Document();
        d.add(newStringField("id", ""+i, Store.NO));
        if (rarely(indexRandom)) {
         // System.out.println("on:"+i+" rarely: true");
          d.add(new SortedDocValuesField("sdv", new BytesRef(""+i)));
          sdvExist = true;
        }else{
         // System.out.println("on:"+i+" rarely: false");
        }
        final int numSortedSet = indexRandom.nextInt(5)-3;
        for (int j = 0; j < numSortedSet; ++j) {
         // System.out.println("on:"+i+" add ssdv:"+j);
          d.add(new SortedSetDocValuesField("ssdv", new BytesRef(""+j)));
          ssdvExist = true;
        }
        iw.addDocument(d);
        iw.commit();
      }
    }
    final DirectoryReader reader = iw.getReader();
    
   // System.out.println("sdv:"+ sdvExist+ " ssdv:"+ssdvExist+", segs: "+reader.leaves().size() +", "+reader.leaves());
    
    iw.close();
    final LeafReader wrapper = SlowCompositeReaderWrapper.wrap(reader);
    
    {
      //final Random indexRandom = new Random(seed);
      final SortedDocValues sdv = wrapper.getSortedDocValues("sdv");
      final SortedSetDocValues ssdv = wrapper.getSortedSetDocValues("ssdv");
      
      assertNull("confusing DV type", wrapper.getSortedDocValues("ssdv"));
      assertNull("confusing DV type", wrapper.getSortedSetDocValues("sdv"));
      
      assertNull("absent field", wrapper.getSortedDocValues("NOssdv"));
      assertNull("absent field", wrapper.getSortedSetDocValues("NOsdv"));
      
      assertTrue("optional sdv field", sdvExist == (sdv!=null));
      assertTrue("optional ssdv field", ssdvExist == (ssdv!=null));
    } 
    reader.close();
    
    dir.close();
  }
  
}
