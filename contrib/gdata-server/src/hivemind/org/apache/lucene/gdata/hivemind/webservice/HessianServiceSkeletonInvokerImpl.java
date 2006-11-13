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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.caucho.hessian.io.AbstractHessianOutput;
import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.caucho.hessian.io.HessianOutput;
import com.caucho.hessian.io.SerializerFactory;
import com.caucho.hessian.server.HessianSkeleton;

/**
 * Wrapps the hessian skeleton invokation logic.
 * This is based on the protocol description of the hessian protocol
 * @author Simon Willnauer
 *
 */
class HessianServiceSkeletonInvokerImpl implements
        HessianServiceSkeletonInvoker {
    private static final Log LOG = LogFactory.getLog(HessianServiceSkeletonInvokerImpl.class);
    private SerializerFactory serializerFactory;
    private final HessianSkeleton skeleton;
    /**
     * Creates a new HessianServiceSkeletonInvoker
     * @param skeleton - The skeleton instance to invoke to process the request
     * 
     */
    HessianServiceSkeletonInvokerImpl(final HessianSkeleton skeleton) {
        this.skeleton = skeleton;
       
    }

    /**
     * @throws Throwable 
     * @see org.apache.lucene.gdata.hivemind.webservice.HessianServiceSkeletonInvoker#invoke(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    public void invoke(HttpServletRequest arg0, HttpServletResponse arg1) throws Throwable {
        InputStream inputStream = arg0.getInputStream();
        OutputStream outputStream = arg1.getOutputStream();
        /*
         *This works only with hessian >= hessian-3.0.20!!
         *but remember this is not a framework  
         */
        Hessian2Input hessianInput = new Hessian2Input(inputStream);
        if (this.serializerFactory != null) {
                hessianInput.setSerializerFactory(this.serializerFactory);
        }

        int code = hessianInput.read();
        if (code != 'c') {
                throw new IOException("expected 'c' in hessian input at " + code);
        }

        AbstractHessianOutput  hessianOutput = null;
        int major = hessianInput.read();
        // useless read just get the stream in the right position.
        int minor = hessianInput.read();
        if (major >= 2) {
                hessianOutput = new Hessian2Output(outputStream);
        }
        else {
                hessianOutput = new HessianOutput(outputStream);
        }
        if (this.serializerFactory != null) {
                hessianOutput.setSerializerFactory(this.serializerFactory);
        }

        try {
            this.skeleton.invoke(hessianInput, hessianOutput);
        } catch (Throwable e) {
            LOG.error("Unexpected Exception thrown -- "+e.getMessage(),e);
            throw e;
            
        }

    }

    /**
     * @return Returns the serializerFactory.
     */
    public SerializerFactory getSerializerFactory() {
        return this.serializerFactory;
    }

    /**
     * @param serializerFactory The serializerFactory to set.
     */
    public void setSerializerFactory(SerializerFactory serializerFactory) {
        this.serializerFactory = serializerFactory;
    }

}
