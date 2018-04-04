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
package org.apache.solr.util;

/**
 * A <code>long</code>-based iterator.  This is not <i>is-a</i> {@link java.util.Iterator}
 * to prevent autoboxing between <code>Long</code> and <code>long</code>.
 */
public interface LongIterator {
    /**
     * @return <code>true</code> if and only if there are more elements to
     *         iterate over.  <code>false</code> otherwise.
     */
    boolean hasNext();

    /**
     * @return the next <code>long</code> in the collection.  Only valid after hasNext() has been called and returns true.
     */
    long next();
}