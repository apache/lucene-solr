package org.apache.lucene.gdata.server; 

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

 
/** 
 * Will be thrown if a requested feed could not be found or is not 
 * registerd. 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class FeedNotFoundException extends ServiceException { 
 
    private static final long serialVersionUID = 1L; 
 
    /**
     * Constructs a new FeedNotFoundException
     * @param errorCode - gdata request errorcode
     */
    public FeedNotFoundException(int errorCode) {
        super(errorCode);
        

    }

    /**
     * Constructs a new FeedNotFoundException
     * @param arg0 - the exception message
     * @param errorCode - gdata request errorcode
     */
    public FeedNotFoundException(String arg0,int errorCode) {
        super(arg0, errorCode);
        
    }

    /**
     * Constructs a new FeedNotFoundException
     * @param arg0 - the exceptin message
     * @param arg1 - the exception cause
     * @param errorCode - gdata request errorcode
     */
    public FeedNotFoundException(String arg0, Throwable arg1,int errorCode) {
        super(arg0, arg1, errorCode);
        
        
    }

    /**
     * Constructs a new FeedNotFoundException
     * @param arg0 - the exception cause
     * @param errorCode - gdata request errorcode
     */
    public FeedNotFoundException(Throwable arg0,int errorCode) {
        super(arg0, errorCode);
        
    }
 
} 
