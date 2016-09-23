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

package org.apache.lucene.concordance.classic;

/**
 * Options for sorting ConcordanceWindows
 */
public enum ConcordanceSortOrder {
  PRE, // sort on the first token before the target, then the second word, etc.
  POST, // sort on words after the target
  TARGET_PRE, // sort on the target and then words before the target
  TARGET_POST, // sort on the target and then words after the target
  DOC, // sort on a string representing a doc id and then by target char offset within the document
  NONE // no sort
}
