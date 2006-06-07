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
 * This exception wraps all exceptions occure inside the {@link org.apache.lucene.gdata.server.GDataRequest} 
 * @author Simon Willnauer 
 * 
 */ 
public class GDataRequestException extends Exception { 
 
    /** 
     * Serial version ID. -> Implements Serializable 
     */ 
    private static final long serialVersionUID = -4440777051466950723L; 
 
    /** 
     * Constructs a new GDataException 
     */ 
    public GDataRequestException() { 
        super(); 
         
    } 
 
    /** 
     * Constructs a new GDataException with a given message string 
     * @param arg0 - the excpetion message  
     */ 
    public GDataRequestException(String arg0) { 
        super(arg0); 
         
    } 
 
    /** 
     * Constructs a new GDataException with a given message string and cause 
     * @param arg0 - the exception message 
     * @param arg1 - the exception who caused this exception 
     */ 
    public GDataRequestException(String arg0, Throwable arg1) { 
        super(arg0, arg1); 
         
    } 
 
    /** 
     * Constructs a new GDataException with a given cause 
     * @param arg0 - exception cause 
     */ 
    public GDataRequestException(Throwable arg0) { 
        super(arg0); 
         
    } 
 
} 
