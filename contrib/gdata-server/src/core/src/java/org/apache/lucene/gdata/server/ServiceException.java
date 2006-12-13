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
 * The ServiceException is used to encapsulate all {@link java.lang.Exception}
 * throw by underlying layers of the
 * {@link org.apache.lucene.gdata.server.Service} layer.
 * 
 * @author Simon Willnauer
 * 
 */
public class ServiceException extends Exception {
    
    private int errorCode;
    /**
     * 
     */
    private static final long serialVersionUID = -7099825107871876584L;

    /**
     * Constructs a new ServiceException
     * @param errorCode - gdata request error code
     */
    public ServiceException(int errorCode) {
        super();
        this.errorCode = errorCode;

    }

    /**
     * Constructs a new ServiceException
     * @param arg0 - the exception message
     * @param errorCode - gdata request error code
     */
    public ServiceException(String arg0,int errorCode) {
        super(arg0);
        this.errorCode = errorCode;
    }

    /**
     * Constructs a new ServiceException
     * @param arg0 - the exception message
     * @param arg1 - the exception cause
     * @param errorCode - gdata request error code
     */
    public ServiceException(String arg0, Throwable arg1,int errorCode) {
        super(arg0, arg1);
        this.errorCode = errorCode;
        
    }

    /**
     * Constructs a new ServiceException
     * @param arg0 - the exception cause
     * @param errorCode - gdata request error code
     */
    public ServiceException(Throwable arg0,int errorCode) {
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
