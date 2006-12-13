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
package org.apache.lucene.gdata.hivemind.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hivemind.Registry;
import org.apache.hivemind.servlet.HiveMindFilter;
import org.apache.lucene.gdata.hivemind.webservice.HessianServiceSkeletonInvoker;
import org.apache.lucene.gdata.hivemind.webservice.HessianSkeletonProvider;

/**
 * Central Hessian servlet which provides access to all hessian exposed services
 * via a single servlet.
 * 
 * @author Simon Willnauer
 * 
 */
public class HessianServiceServlet extends HttpServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 5519783120466089391L;

    /**
     * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest,
     *      javax.servlet.http.HttpServletResponse)
     */
    @Override
    protected void doPost(HttpServletRequest arg0, HttpServletResponse arg1)
            throws ServletException, IOException {
        try {
            HessianSkeletonProvider provider = getSkeletonProvider(arg0);
            HessianServiceSkeletonInvoker invoker = provider
                    .getServiceSkeletonInvoker(arg0);
            invoker.invoke(arg0, arg1);
        } catch (Throwable e) {
            throw new ServletException("Nested Exception occured -- Message: "
                    + e.getMessage(), e);
        }

    }

    private HessianSkeletonProvider getSkeletonProvider(
            HttpServletRequest request) {
        Registry reg = HiveMindFilter.getRegistry(request);
        return (HessianSkeletonProvider) reg
                .getService(HessianSkeletonProvider.class);
    }

}
