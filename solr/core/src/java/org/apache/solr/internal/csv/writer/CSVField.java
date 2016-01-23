/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.solr.internal.csv.writer;


/**
 * 
 */
public class CSVField {

    private String name;
    private int size;
    private int fill;
    private boolean overrideFill;

    /**
     * 
     */
    public CSVField() {
    }

    /**
     * @param name the name of the field
     */
    public CSVField(String name) {
        setName(name);
    }

    /**
     * @param name the name of the field
     * @param size the size of the field
     */
    public CSVField(String name, int size) {
        setName(name);
        setSize(size);
    }

    /**
     * @return the name of the field
     */
    public String getName() {
        return name;
    }
    
    /**
     * Set the name of the field
     * @param name the name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 
     * @return the size of the field
     */
    public int getSize() {
        return size;
    }

    /**
     * Set the size of the field.
     * The size will be ignored when fixedwidth is set to false in the CSVConfig
     * @param size the size of the field.
     */
    public void setSize(int size) {
        this.size = size;
    }

    /**
     * @return the fill pattern.
     */
    public int getFill() {
        return fill;
    }

    /**
     * Sets overrideFill to true.
     * @param fill the file pattern
     */
    public void setFill(int fill) {
        overrideFill = true;
        this.fill = fill;
    }
    
    /**
     * Does this field override fill ?
     * 
     */
    public boolean overrideFill() {
        return overrideFill;
    }

}
