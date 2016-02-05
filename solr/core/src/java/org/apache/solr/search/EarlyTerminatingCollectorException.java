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
package org.apache.solr.search;

/**
 * Thrown by {@link EarlyTerminatingCollector} when the maximum to abort
 * the scoring / collection process early, when the specified maximum number
 * of documents were collected.
 */
public class EarlyTerminatingCollectorException extends RuntimeException {
  private static final long serialVersionUID = 5939241340763428118L;  
  private int numberScanned;
  private int numberCollected;
  
  public EarlyTerminatingCollectorException(int numberCollected, int numberScanned) {
    assert numberCollected <= numberScanned : numberCollected+"<="+numberScanned;
    assert 0 < numberCollected;
    assert 0 < numberScanned;

    this.numberCollected = numberCollected;
    this.numberScanned = numberScanned;
  }
  /**
   * The total number of documents in the index that were "scanned" by 
   * the index when collecting the {@link #getNumberCollected()} documents 
   * that triggered this exception.
   * <p>
   * This number represents the sum of:
   * </p>
   * <ul>
   *  <li>The total number of documents in all LeafReaders
   *      that were fully exhausted during collection
   *  </li>
   *  <li>The id of the last doc collected in the last LeafReader
   *      consulted during collection.
   *  </li>
   * </ul>
   **/
  public int getNumberScanned() {
    return numberScanned;
  }
  /**
   * The number of documents collected that resulted in early termination
   */
  public int getNumberCollected() {
    return numberCollected;
  }
}
