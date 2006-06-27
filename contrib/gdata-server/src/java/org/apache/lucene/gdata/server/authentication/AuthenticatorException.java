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
 * This exception will be thrown by
 * {@link org.apache.lucene.gdata.server.authentication.AuthenticationController}
 * implementations if an error while de/encrypting the token occures.
 * 
 * @author Simon Willnauer
 * 
 */
public class AuthenticatorException extends RuntimeException {

    private static final long serialVersionUID = -5690495392712564651L;

    /**
     * Constructs a new Authenticator Exception
     */ 
    public AuthenticatorException() {
        super();
        
    }

    /**
     * Constructs a new Authenticator Exception with the specified detail message.
     * @param arg0 - detail message
     */
    public AuthenticatorException(String arg0) {
        super(arg0);
        
    }

    /**
     * Constructs a new Authenticator Exception with the specified detail message and
     * nested exception.
     * 
     * @param arg0 -
     *            detail message
     * @param arg1 -
     *            nested exception
     */
    public AuthenticatorException(String arg0, Throwable arg1) {
        super(arg0, arg1);
        
    }

    /**
     * Constructs a new Authenticator Exception with a nested exception caused this exception.
     * @param arg0 - nested exception
     */
    public AuthenticatorException(Throwable arg0) {
        super(arg0);
        
    }

}
