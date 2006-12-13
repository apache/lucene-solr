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

package org.apache.lucene.gdata.server.registry.configuration;

/**
 * Will be throw if an exception occures while injecting properties, a type or
 * cast exception occures or a
 * {@link org.apache.lucene.gdata.server.registry.configuration.Requiered}
 * property is not available.
 * 
 * @author Simon Willnauer
 * 
 */
public class InjectionException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 3559845601111510210L;

    /**
     * Constructs a new InjectionException
     */
    public InjectionException() {
        super();
    }

    /**
     * Constructs a new InjectionException
     * 
     * @param message -
     *            the exception message
     */
    public InjectionException(String message) {
        super(message);

    }

    /**
     * Constructs a new InjectionException
     * 
     * @param message -
     *            the exception message
     * @param cause -
     *            the root cause of this exception
     */
    public InjectionException(String message, Throwable cause) {
        super(message, cause);

    }

    /**
     * Constructs a new InjectionException
     * 
     * @param cause -
     *            the root cause of this exception
     */
    public InjectionException(Throwable cause) {
        super(cause);

    }

}
