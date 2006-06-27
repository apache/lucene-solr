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

package org.apache.lucene.gdata.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.gdata.servlet.handler.AuthenticationHandler;

/**
 * REST interface for handling authentification requests from clients to get a
 * auth token either as a cookie or as a plain auth token. This Servlet uses a
 * single {@link org.apache.lucene.gdata.servlet.handler.AuthenticationHandler}
 * instance to handle the incoming requests.
 * 
 * @author Simon Willnauer
 * 
 */
public class AuthenticationServlet extends HttpServlet {

    private final AuthenticationHandler handler = new AuthenticationHandler();

    private static final long serialVersionUID = 7132478125868917848L;

    @SuppressWarnings("unused")
    @Override
    protected void doPost(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        this.handler.processRequest(request, response);
    }

}
