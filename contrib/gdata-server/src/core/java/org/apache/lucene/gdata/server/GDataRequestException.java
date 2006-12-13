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
 
package org.apache.lucene.gdata.server; 
 
/** 
 * This exception wraps all exceptions occur inside the {@link org.apache.lucene.gdata.server.GDataRequest} 
 * @author Simon Willnauer 
 * 
 */ 
public class GDataRequestException extends Exception { 
    private final int errorCode;
    /** 
     * Serial version ID. -> Implements Serializable 
     */ 
    private static final long serialVersionUID = -4440777051466950723L; 
 
    /** 
       /**
     * Constructs a new GDataRequestException
     * @param errorCode - gdata request error code
     */
    public GDataRequestException(int errorCode) {
        super();
        this.errorCode = errorCode;

    }

    /**
     * Constructs a new GDataRequestException
     * @param arg0 - the exception message
     * @param errorCode - gdata request error code
     */
    public GDataRequestException(String arg0,int errorCode) {
        super(arg0);
        this.errorCode = errorCode;
    }

    /**
     * Constructs a new GDataRequestException
     * @param arg0 - the exception message
     * @param arg1 - the exception cause
     * @param errorCode - gdata request error code
     */
    public GDataRequestException(String arg0, Throwable arg1,int errorCode) {
        super(arg0, arg1);
        this.errorCode = errorCode;
        
    }

    /**
     * Constructs a new GDataRequestException
     * @param arg0 - the exception cause
     * @param errorCode - gdata request error code
     */
    public GDataRequestException(Throwable arg0,int errorCode) {
        super(arg0);
        this.errorCode = errorCode;
    }

    /**
     * @return Returns the errorCode.
     */
    public int getErrorCode() {
        return this.errorCode;
    }
} 
