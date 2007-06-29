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

  static final String DOC_TEXT =  
    "Well, this is just some plain text we use for creating the " +
    "test documents. It used to be a text from an online collection " +
    "devoted to first aid, but if there was there an (online) lawyers " +
    "first aid collection with legal advices, \"it\" might have quite " +
    "probably advised one not to include \"it\"'s text or the text of " +
    "any other online collection in one's code, unless one has money " +
    "that one don't need and one is happy to donate for lawyers " +
    "charity. Anyhow at some point, rechecking the usage of this text, " +
    "it became uncertain that this text is free to use, because " +
    "the web site in the disclaimer of he eBook containing that text " +
    "was not responding anymore, and at the same time, in projGut, " +
    "searching for first aid no longer found that eBook as well. " +
    "So here we are, with a perhaps much less interesting " +
    "text for the test, but oh much much safer. ";
  
  // return a new docid
  private synchronized int newdocid() throws NoMoreDataException {
    if (docID>0 && !forever) {
      throw new NoMoreDataException();
    }
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
    int id = newdocid();
    addBytes(DOC_TEXT.length());
    return new DocData("doc"+id, DOC_TEXT, null, null, null);
  }

}
