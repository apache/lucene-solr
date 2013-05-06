package org.apache.solr.search;

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
 * Thrown by {@link EarlyTerminatingCollector} when the maximum to abort
 * the scoring / collection process early, when the specified maximum number
 * of documents were collected.
 */
public class EarlyTerminatingCollectorException extends RuntimeException {
  private static final long serialVersionUID = 5939241340763428118L;  
  private int lastDocId = -1;
  private int numberCollected;
  
  public EarlyTerminatingCollectorException(int numberCollected, int lastDocId) {
    this.numberCollected = numberCollected;
    this.lastDocId = lastDocId;
  }
  public int getLastDocId() {
    return lastDocId;
  }
  public void setLastDocId(int lastDocId) {
    this.lastDocId = lastDocId;
  }
  public int getNumberCollected() {
    return numberCollected;
  }
  public void setNumberCollected(int numberCollected) {
    this.numberCollected = numberCollected;
  }
}
