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
 
package org.apache.lucene.gdata.servlet.handler; 
 
/** 
 * @author Simon Willnauer 
 * 
 */ 
public class GDataRequestHandlerException extends RuntimeException { 
 
    /** 
     *  
     */ 
    private static final long serialVersionUID = -418225239671624153L; 
    
 
    /** 
     *  
     */ 
    public GDataRequestHandlerException() { 
        super(); 
        
    } 
 
    /** 
     * @param arg0 
     */ 
    public GDataRequestHandlerException(String arg0) { 
        super(arg0); 
        
    } 
 
    /** 
     * @param arg0 
     * @param arg1 
     */ 
    public GDataRequestHandlerException(String arg0, Throwable arg1) { 
        super(arg0, arg1); 
        
    } 
 
    /** 
     * @param arg0 
     */ 
    public GDataRequestHandlerException(Throwable arg0) { 
        super(arg0); 
        
    } 
 
} 
