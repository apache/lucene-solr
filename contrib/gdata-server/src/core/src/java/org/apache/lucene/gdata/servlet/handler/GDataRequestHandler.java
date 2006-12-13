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
 
import java.io.IOException; 
 
import javax.servlet.ServletException; 
import javax.servlet.http.HttpServletRequest; 
import javax.servlet.http.HttpServletResponse; 
 
/** 
 *  
 * Based on the Command pattern [GoF], the Command and Controller Strategy 
 * suggests providing a generic interface to the handler components to which the 
 * controller may delegate responsibility, minimizing the coupling among these 
 * components. 
 *  
 * Adding to or changing the work that needs to be completed by these handlers 
 * does not require any changes to the interface between the controller and the 
 * handlers, but rather to the type and/or content of the commands. This provides 
 * a flexible and easily extensible mechanism for developers to add request 
 * handling behaviors. 
 *  
 * The controller invokes the processRequest method from the corresponding servlet <i>doXXX</i> 
 * method to delegate the request to the handler. 
 *   
 *  
 * @author Simon Willnauer 
 *  
 */ 
public interface GDataRequestHandler { 
    /** 
     * Processes the GDATA Client request 
     *  
     * @param request - the client request to be processed 
     * @param response - the response to the client request 
     * @throws ServletException - if a servlet exception is thrown by the request or response   
     * @throws IOException -  if an input/output error occurs due to accessing an IO steam 
     */ 
    public abstract void processRequest(HttpServletRequest request, 
            HttpServletResponse response) throws ServletException, IOException; 
} 
