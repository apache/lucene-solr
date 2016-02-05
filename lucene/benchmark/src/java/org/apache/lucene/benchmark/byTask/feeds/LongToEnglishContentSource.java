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
package org.apache.lucene.benchmark.byTask.feeds;


import java.io.IOException;
import java.util.Date;
import java.util.Locale;

import com.ibm.icu.text.RuleBasedNumberFormat;

/**
 * Creates documents whose content is a <code>long</code> number starting from
 * <code>{@link Long#MIN_VALUE} + 10</code>.
 */
public class LongToEnglishContentSource extends ContentSource{
  private long counter = 0;

  @Override
  public void close() throws IOException {
  }

  // TODO: we could take param to specify locale...
  private final RuleBasedNumberFormat rnbf = new RuleBasedNumberFormat(Locale.ROOT,
                                                                       RuleBasedNumberFormat.SPELLOUT);
  @Override
  public synchronized DocData getNextDocData(DocData docData) throws NoMoreDataException, IOException {
    docData.clear();
    // store the current counter to avoid synchronization later on
    long curCounter;
    synchronized (this) {
      curCounter = counter;
      if (counter == Long.MAX_VALUE){
        counter = Long.MIN_VALUE;//loop around
      } else {
        ++counter;
      }
    }    

    docData.setBody(rnbf.format(curCounter));
    docData.setName("doc_" + String.valueOf(curCounter));
    docData.setTitle("title_" + String.valueOf(curCounter));
    docData.setDate(new Date());
    return docData;
  }

  @Override
  public void resetInputs() throws IOException {
    counter = Long.MIN_VALUE + 10;
  }
  
}
