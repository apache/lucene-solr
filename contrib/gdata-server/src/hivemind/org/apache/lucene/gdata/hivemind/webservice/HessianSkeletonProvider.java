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

package org.apache.lucene.gdata.hivemind.webservice;

import javax.servlet.http.HttpServletRequest;

/**
 * Serviceprovider for within hivemind configured services to expose as
 * webservices. This SkeletonProvider will return the corresponding service to
 * the given <code>HttpServletRequest</code>.
 * <p>
 * Services exported via this provider will be available via a Hessian service
 * endpoint, accessible via a Hessian proxy
 * </p>
 * <p>
 * Fo information on Hessian see: <a
 * href="http://www.caucho.com/hessian">Hessian protocol</a>
 * </p>
 * 
 * @author Simon Willnauer
 * 
 */
public interface HessianSkeletonProvider {

    /**
     * Selects the configured Service according to the given request.
     * 
     * @param arg0 -
     *            the current HttpServletRequest
     * @return - a corresponding HessianServiceSkeletonInvoker
     */
    HessianServiceSkeletonInvoker getServiceSkeletonInvoker(
            HttpServletRequest arg0);

}
