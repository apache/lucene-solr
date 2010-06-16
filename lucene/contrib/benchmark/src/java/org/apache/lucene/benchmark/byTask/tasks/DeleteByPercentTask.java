package org.apache.lucene.benchmark.byTask.tasks;

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

import java.util.Random;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.util.Bits;

/**
 * Deletes a percentage of documents from an index randomly
 * over the number of documents.  The parameter, X, is in
 * percent.  EG 50 means 1/2 of all documents will be
 * deleted.
 *
 * <p><b>NOTE</b>: the param is an absolute percentage of
 * maxDoc().  This means if you delete 50%, and then delete
 * 50% again, the 2nd delete will do nothing.
 *
 * <p> Parameters:
 * <ul>
 * <li> delete.percent.rand.seed - defines the seed to
 * initialize Random (default 1717)
 * </ul>
 */
public class DeleteByPercentTask extends PerfTask {
  double percent;
  int numDeleted = 0;
  final Random random;

  public DeleteByPercentTask(PerfRunData runData) {
    super(runData);
    random = new Random(runData.getConfig().get("delete.percent.rand.seed", 1717));
  }
  
  @Override
  public void setParams(String params) {
    super.setParams(params);
    percent = Double.parseDouble(params)/100;
  }

  @Override
  public boolean supportsParams() {
    return true;
  }

  @Override
  public int doLogic() throws Exception {
    IndexReader r = getRunData().getIndexReader();
    int maxDoc = r.maxDoc();
    int numDeleted = 0;
    // percent is an absolute target:
    int numToDelete = ((int) (maxDoc * percent)) - r.numDeletedDocs();
    if (numToDelete < 0) {
      r.undeleteAll();
      numToDelete = (int) (maxDoc * percent);
    }
    while (numDeleted < numToDelete) {
      double delRate = ((double) (numToDelete-numDeleted))/r.numDocs();
      Bits delDocs = MultiFields.getDeletedDocs(r);
      int doc = 0;
      while (doc < maxDoc && numDeleted < numToDelete) {
        if (!delDocs.get(doc) && random.nextDouble() <= delRate) {
          r.deleteDocument(doc);
          numDeleted++;
        }
      }
    }
    System.out.println("--> processed (delete) " + numDeleted + " docs");
    r.decRef();
    return numDeleted;
  }
}
