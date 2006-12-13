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

package org.apache.lucene.gdata.search.analysis;

/**
 * This exception will be thrown by ContentStrategy instances if an exception
 * occurs while retrieving content from entries
 * 
 * @author Simon Willnauer
 * 
 */
public class NotIndexableException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 1538388864181786380L;

    /**
     * Constructs a new NotIndexableException
     */
    public NotIndexableException() {
        super();

    }

    /**
     * Constructs a new NotIndexableException with the specified detail message.
     * 
     * @param arg0 -
     *            detail message
     */
    public NotIndexableException(String arg0) {
        super(arg0);

    }

    /**
     * Constructs a new NotIndexableException with the specified detail message
     * and nested exception.
     * 
     * @param arg0 -
     *            detail message
     * @param arg1 -
     *            nested exception
     */
    public NotIndexableException(String arg0, Throwable arg1) {
        super(arg0, arg1);

    }

    /**
     * Constructs a new NotIndexableException with a nested exception caused
     * this exception.
     * 
     * @param arg0 -
     *            nested exception
     */
    public NotIndexableException(Throwable arg0) {
        super(arg0);

    }

}
