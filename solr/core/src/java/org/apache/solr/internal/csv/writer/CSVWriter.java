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

import java.io.Writer;
import java.util.Arrays;
import java.util.Map;


/**
 * CSVWriter
 *
 */
public class CSVWriter {

    /** The CSV config **/
    private CSVConfig config;
    /** The writer **/
    private Writer writer;
    /**
     * 
     */
    public CSVWriter() {
    }
    
    public CSVWriter(CSVConfig config) {
        setConfig(config);
    }

    public void writeRecord(Map map) {
        CSVField[] fields = config.getFields();
        try {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < fields.length; i++) {
                Object o = map.get(fields[i].getName());
                if (o != null) {
                    String value = o.toString();
                    value = writeValue(fields[i], value);
                    sb.append(value);
                }
                if (!config.isDelimiterIgnored() && fields.length != (i+1)) {
                    sb.append(config.getDelimiter());
                }
            }
            if (config.isEndTrimmed()) {
                for (int i = sb.length()-1; i >= 0; i--) {
                    System.out.println("i : " + i);
                    if (Character.isWhitespace(sb.charAt(i))) {
                        sb.deleteCharAt(i);
                    } else {
                        break;
                    }
                }
            }
            sb.append("\n");
            String line = sb.toString();
            writer.write(line);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    protected String writeValue(CSVField field, String value) throws Exception {
        if (config.isFixedWidth()) {
            if (value.length() < field.getSize()) {
                int fillPattern = config.getFill();
                if (field.overrideFill()) {
                    fillPattern = field.getFill();
                }
                StringBuilder sb = new StringBuilder();
                int fillSize = (field.getSize() - value.length());
                char[] fill = new char[fillSize];
                Arrays.fill(fill, config.getFillChar());
                if (fillPattern == CSVConfig.FILLLEFT) {
                    sb.append(fill);
                    sb.append(value);
                    value = sb.toString();
                } else {
                    // defaults to fillpattern FILLRIGHT when fixedwidth is used
                    sb.append(value);
                    sb.append(fill);
                    value = sb.toString();
                }
            } else if (value.length() > field.getSize()) {
                // value to big..
                value = value.substring(0, field.getSize());
            }
            if (!config.isValueDelimiterIgnored()) {
                // add the value delimiter..
                value = config.getValueDelimiter()+value+config.getValueDelimiter();
            }
        }
        return value;
    }
    /**
     * @return the CVSConfig or null if not present
     */
    public CSVConfig getConfig() {
        return config;
    }

    /**
     * Set the CSVConfig
     * @param config the CVSConfig
     */
    public void setConfig(CSVConfig config) {
        this.config = config;
    }
    
    /**
     * Set the writer to write the CSV file to.
     * @param writer the writer.
     */
    public void setWriter(Writer writer) {
        this.writer = writer;
    }

}
