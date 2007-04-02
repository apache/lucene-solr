package org.apache.lucene.benchmark.byTask.feeds;

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

/**
 * Create documents for the test.
 */
public class SimpleDocMaker extends BasicDocMaker {
  
  private int docID = 0;

  static final String DOC_TEXT = // from a public first aid info at http://firstaid.ie.eu.org 
    "Well it may be a little dramatic but sometimes it true. " +
    "If you call the emergency medical services to an incident, " +
    "your actions have started the chain of survival. " +
    "You have acted to help someone you may not even know. " +
    "First aid is helping, first aid is making that call, " +
    "putting a Band-Aid on a small wound, controlling bleeding in large " +
    "wounds or providing CPR for a collapsed person whose not breathing " +
    "and heart has stopped beating. You can help yourself, your loved " +
    "ones and the stranger whose life may depend on you being in the " +
    "right place at the right time with the right knowledge.";
  
  // return a new docid
  private synchronized int newdocid() {
    return docID++;
  }

  /*
   *  (non-Javadoc)
   * @see DocMaker#resetIinputs()
   */
  public synchronized void resetInputs() {
    super.resetInputs();
    docID = 0;
  }

  /*
   *  (non-Javadoc)
   * @see DocMaker#numUniqueTexts()
   */
  public int numUniqueTexts() {
    return 0; // not applicable
  }

  protected DocData getNextDocData() throws NoMoreDataException {
    if (docID>0 && !forever) {
      throw new NoMoreDataException();
    }
    addBytes(DOC_TEXT.length());
    return new DocData("doc"+newdocid(),DOC_TEXT, null, null, null);
  }

}
