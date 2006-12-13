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

package org.apache.lucene.gdata.utils;




import org.apache.lucene.gdata.data.ServerBaseEntry;

import com.google.gdata.data.BaseEntry;

/**
 * @author Simon Willnauer
 *
 */
public class MultiThreadEntryStub extends ServerBaseEntry {
    
    
    private Visitor getEntryVisitor;
    private Visitor getVersionVisitor;
    /**
     * 
     */
    public MultiThreadEntryStub() {
        
     
    }
    
    /**
     * @param arg0
     */
    public MultiThreadEntryStub(BaseEntry arg0) {
        super(arg0);
        
    }
   
    
    
    public void acceptGetEntryVisitor(Visitor visitor){
        this.getEntryVisitor = visitor;
    }
    public void acceptGetVersionVisitor(Visitor visitor){
       this.getVersionVisitor = visitor;
    }
    /**
     * @see org.apache.lucene.gdata.data.ServerBaseEntry#getEntry()
     */
    @Override
    public BaseEntry getEntry() {
       
        if(this.getEntryVisitor != null){
            this.getEntryVisitor.execute(null);

        }
        return super.getEntry();
    }

    /**
     * @see org.apache.lucene.gdata.data.ServerBaseEntry#getVersion()
     */
    @Override
    public int getVersion() {

        if(this.getVersionVisitor != null){
            this.getVersionVisitor.execute(null);

        }
        return super.getVersion();
    }


    
    
    
    
    

}
