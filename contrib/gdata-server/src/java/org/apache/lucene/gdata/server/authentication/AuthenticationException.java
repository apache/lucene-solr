/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.gdata.server.authentication;

/**
 * 
 * @author Simon Willnauer
 *
 */
public class AuthenticationException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 5467768078178612671L;

    /**
     * Constructs a new Authentication Exception
     */
    public AuthenticationException() {
        super();
       
    }

    /**
     * Constructs a new Authentication Exception with the specified detail message 
     * @param arg0 - detail message
     */
    public AuthenticationException(String arg0) {
        super(arg0);
       
    }

    /**
     * Constructs a new Authentication Exception with the specified detail message and
     * nested exception caused this exception.
      * @param arg0 -
     *            detail message
     * @param arg1 -
     *            nested exception
     */
    public AuthenticationException(String arg0, Throwable arg1) {
        super(arg0, arg1);
       
    }

    /**
     * Constructs a new Authentication Exception with a nested exception caused this exception
     * @param arg0 - nested exception
     */
    public AuthenticationException(Throwable arg0) {
        super(arg0);
       
    }

}
