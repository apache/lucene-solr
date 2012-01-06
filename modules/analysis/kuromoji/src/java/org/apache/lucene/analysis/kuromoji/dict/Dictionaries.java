package org.apache.lucene.analysis.kuromoji.dict;

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

public final class Dictionaries {
  
  private static TokenInfoDictionary dictionary;
  private static UnknownDictionary unknownDictionary;
  private static ConnectionCosts costs;
  static {
    dictionary = TokenInfoDictionary.getInstance();
    unknownDictionary = UnknownDictionary.getInstance();
    costs = ConnectionCosts.getInstance();
  }
  
  private Dictionaries() {} // no instance!
  
  /**
   * @return the dictionary
   */
  public static TokenInfoDictionary getDictionary() {
    return dictionary;
  }
  
  /**
   * @param dictionary the dictionary to set
   */
  public static void setDictionary(TokenInfoDictionary dictionary) {
    Dictionaries.dictionary = dictionary;
  }
  
  /**
   * @return the unknownDictionary
   */
  public static UnknownDictionary getUnknownDictionary() {
    return unknownDictionary;
  }
  
  /**
   * @param unknownDictionary the unknownDictionary to set
   */
  public static void setUnknownDictionary(UnknownDictionary unknownDictionary) {
    Dictionaries.unknownDictionary = unknownDictionary;
  }
  
  /**
   * @return the costs
   */
  public static ConnectionCosts getCosts() {
    return costs;
  }
  
  /**
   * @param costs the costs to set
   */
  public static void setCosts(ConnectionCosts costs) {
    Dictionaries.costs = costs;
  }
}
