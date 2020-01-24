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

package org.apache.lucene.index;

/**
 * Utility methods for working with a {@link IndexReader} as if it were a {@link LeafReader}.
 *
 * <p><b>NOTE</b>: for composite readers, you'll get better
 * performance by gathering the sub readers using
 * {@link IndexReader#getContext()} to get the
 * atomic leaves and then operate per-LeafReader,
 * instead of using this class.
 * @lucene.experimental
 */
public class MultiLeafReader {

  private MultiLeafReader() {}

}
