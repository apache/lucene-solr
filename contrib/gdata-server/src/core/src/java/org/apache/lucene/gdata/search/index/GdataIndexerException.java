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

package org.apache.lucene.gdata.search.index;

/**
 * This exception will be thrown if an exception in the indexing component
 * occurs
 * 
 * @author Simon Willnauer
 * 
 */
public class GdataIndexerException extends RuntimeException {

    private static final long serialVersionUID = -8245420079471690182L;

    /**
     * Creates a new GdataIndexerException
     */
    public GdataIndexerException() {
        super();

    }

    /**
     * Creates a new GdataIndexerException with a new exception message
     * 
     * @param arg0 -
     *            exception message
     */
    public GdataIndexerException(String arg0) {
        super(arg0);

    }

    /**
     * Creates a new GdataIndexerException with a new exception message and a
     * root cause
     * 
     * @param arg0 -
     *            exception message
     * @param arg1 -
     *            the root cause
     */
    public GdataIndexerException(String arg0, Throwable arg1) {
        super(arg0, arg1);

    }

    /**
     * Creates a new GdataIndexerException with a root cause
     * 
     * @param arg0 -
     *            the root cause
     */
    public GdataIndexerException(Throwable arg0) {
        super(arg0);

    }

}
