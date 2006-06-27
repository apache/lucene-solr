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

package org.apache.lucene.gdata.storage;

/**
 * This exception will be thrown if an requested resource of a resource to modify can not be found
 * @author Simon Willnauer
 *
 */
public class ResourceNotFoundException extends StorageException {

   
    private static final long serialVersionUID = -8549987918130998249L;

    /**
     * Constructs an empty ResourceNotFoundException
     */
    public ResourceNotFoundException() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * Constructs a new ResourceNotFoundException with an exception message
     * @param message - the exception message
     */
    public ResourceNotFoundException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    /**
     * Constructs a new ResourceNotFoundException with an exception message and a root cause 
     * @param message - the exception message
     * @param cause - the root cause of this exception
     */
    public ResourceNotFoundException(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

    /**
     * Constructs a new ResourceNotFoundException with  a root cause
     * @param cause - the root cause of this exception
     * 
     */
    public ResourceNotFoundException(Throwable cause) {
        super(cause);
        // TODO Auto-generated constructor stub
    }

}
