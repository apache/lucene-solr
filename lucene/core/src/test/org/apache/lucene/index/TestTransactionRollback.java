package org.apache.lucene.index;

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


import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;

/**
 * Test class to illustrate using IndexDeletionPolicy to provide multi-level rollback capability.
 * This test case creates an index of records 1 to 100, introducing a commit point every 10 records.
 * 
 * A "keep all" deletion policy is used to ensure we keep all commit points for testing purposes
 */

public class TestTransactionRollback extends LuceneTestCase {
	
  private static final String FIELD_RECORD_ID = "record_id";
  private Directory dir;
	
  //Rolls back index to a chosen ID
  private void rollBackLast(int id) throws Exception {
		
    // System.out.println("Attempting to rollback to "+id);
    String ids="-"+id;
    IndexCommit last=null;
    Collection<IndexCommit> commits = DirectoryReader.listCommits(dir);
    for (Iterator<IndexCommit> iterator = commits.iterator(); iterator.hasNext();) {
      IndexCommit commit =  iterator.next();
      Map<String,String> ud=commit.getUserData();
      if (ud.size() > 0)
        if (ud.get("index").endsWith(ids))
          last=commit;
    }

    if (last==null)
      throw new RuntimeException("Couldn't find commit point "+id);
		
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())).setIndexDeletionPolicy(
        new RollbackDeletionPolicy(id)).setIndexCommit(last));
    Map<String,String> data = new HashMap<String,String>();
    data.put("index", "Rolled back to 1-"+id);
    w.commit(data);
    w.close();
  }

  public void testRepeatedRollBacks() throws Exception {		

    int expectedLastRecordId=100;
    while (expectedLastRecordId>10) {
      expectedLastRecordId -=10;			
      rollBackLast(expectedLastRecordId);
      
      BitSet expecteds = new BitSet(100);
      expecteds.set(1,(expectedLastRecordId+1),true);
      checkExpecteds(expecteds);			
    }
  }
	
  private void checkExpecteds(BitSet expecteds) throws Exception {
    IndexReader r = IndexReader.open(dir);
		
    //Perhaps not the most efficient approach but meets our
    //needs here.
    final Bits liveDocs = MultiFields.getLiveDocs(r);
    for (int i = 0; i < r.maxDoc(); i++) {
      if (liveDocs == null || liveDocs.get(i)) {
        String sval=r.document(i).get(FIELD_RECORD_ID);
        if(sval!=null) {
          int val=Integer.parseInt(sval);
          assertTrue("Did not expect document #"+val, expecteds.get(val));
          expecteds.set(val,false);
        }
      }
    }
    r.close();
    assertEquals("Should have 0 docs remaining ", 0 ,expecteds.cardinality());
  }

  /*
  private void showAvailableCommitPoints() throws Exception {
    Collection commits = DirectoryReader.listCommits(dir);
    for (Iterator iterator = commits.iterator(); iterator.hasNext();) {
      IndexCommit comm = (IndexCommit) iterator.next();
      System.out.print("\t Available commit point:["+comm.getUserData()+"] files=");
      Collection files = comm.getFileNames();
      for (Iterator iterator2 = files.iterator(); iterator2.hasNext();) {
        String filename = (String) iterator2.next();
        System.out.print(filename+", ");				
      }
      System.out.println();
    }
  }
  */

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    //Build index, of records 1 to 100, committing after each batch of 10
    IndexDeletionPolicy sdp=new KeepAllDeletionPolicy();
    IndexWriter w=new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setIndexDeletionPolicy(sdp));

    for(int currentRecordId=1;currentRecordId<=100;currentRecordId++) {
      Document doc=new Document();
      doc.add(newField(FIELD_RECORD_ID,""+currentRecordId,TextField.TYPE_STORED));
      w.addDocument(doc);
			
      if (currentRecordId%10 == 0) {
        Map<String,String> data = new HashMap<String,String>();
        data.put("index", "records 1-"+currentRecordId);
        w.commit(data);
      }
    }

    w.close();
  }
  
  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }

  // Rolls back to previous commit point
  class RollbackDeletionPolicy implements IndexDeletionPolicy {
    private int rollbackPoint;

    public RollbackDeletionPolicy(int rollbackPoint) {
      this.rollbackPoint = rollbackPoint;
    }

    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
    }

    public void onInit(List<? extends IndexCommit> commits) throws IOException {
      for (final IndexCommit commit : commits) {
        Map<String,String> userData=commit.getUserData();
        if (userData.size() > 0) {
          // Label for a commit point is "Records 1-30"
          // This code reads the last id ("30" in this example) and deletes it
          // if it is after the desired rollback point
          String x = userData.get("index");
          String lastVal = x.substring(x.lastIndexOf("-")+1);
          int last = Integer.parseInt(lastVal);
          if (last>rollbackPoint) {
            /*
            System.out.print("\tRolling back commit point:" +
                             " UserData="+commit.getUserData() +")  ("+(commits.size()-1)+" commit points left) files=");
            Collection files = commit.getFileNames();
            for (Iterator iterator2 = files.iterator(); iterator2.hasNext();) {
              System.out.print(" "+iterator2.next());				
            }
            System.out.println();
            */
						
            commit.delete();									
          }
        }
      }
    }		
  }

  class DeleteLastCommitPolicy implements IndexDeletionPolicy {

    public void onCommit(List<? extends IndexCommit> commits) throws IOException {}

    public void onInit(List<? extends IndexCommit> commits) throws IOException {
      commits.get(commits.size()-1).delete();
    }
  }

  public void testRollbackDeletionPolicy() throws Exception {		
    for(int i=0;i<2;i++) {
      // Unless you specify a prior commit point, rollback
      // should not work:
      new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random()))
          .setIndexDeletionPolicy(new DeleteLastCommitPolicy())).close();
      IndexReader r = IndexReader.open(dir);
      assertEquals(100, r.numDocs());
      r.close();
    }
  }
	
  // Keeps all commit points (used to build index)
  class KeepAllDeletionPolicy implements IndexDeletionPolicy {
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {}
    public void onInit(List<? extends IndexCommit> commits) throws IOException {}
  }
}
