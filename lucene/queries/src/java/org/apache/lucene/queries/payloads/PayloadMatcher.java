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
package org.apache.lucene.queries.payloads;

import org.apache.lucene.util.BytesRef;

/** Defines an interface for testing if two payloads should be consider to match */
public interface PayloadMatcher {

  /**
   * This method tests if two BytesRef match.
   *
   * @param source left side of the compare
   * @param payload right side of the compare
   * @return true if the BytesRefs are matching, otherwise false.
   */
  public boolean comparePayload(BytesRef source, BytesRef payload);
}
