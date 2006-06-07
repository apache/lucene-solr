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
 * The StorageException will be throw if any error or exception inside the 
 * storage implementation occures. This exception hides all other exceptions 
 * from inside the storage. 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class StorageException extends Exception { 
 
    /** 
     *  
     */ 
    private static final long serialVersionUID = -4997572416934126511L; 
 
    /** 
     * Constructs a new StorageException 
     */ 
    public StorageException() { 
        super(); 
 
    } 
 
    /** 
     * Constructs a new StorageException 
     *  
     * @param message - 
     *            the exception message 
     */ 
    public StorageException(String message) { 
        super(message); 
 
    } 
 
    /** 
     * Constructs a new StorageException 
     *  
     * @param message - 
     *            the exception message 
     * @param cause - 
     *            the root cause of this exception 
     */ 
    public StorageException(String message, Throwable cause) { 
        super(message, cause); 
 
    } 
 
    /** 
     * Constructs a new StorageException 
     *  
     * @param cause - 
     *            the root cause of this exception 
     */ 
    public StorageException(Throwable cause) { 
        super(cause); 
 
    } 
 
} 
