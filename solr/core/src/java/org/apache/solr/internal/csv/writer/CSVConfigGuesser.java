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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Tries to guess a config based on an InputStream.
 *
 */
public class CSVConfigGuesser {

    /** The stream to read */
    private InputStream in;
    /** 
     * if the file has a field header (need this info, to be able to guess better)
     * Defaults to false
     */
    private boolean hasFieldHeader = false;
    /** The found config */
    protected CSVConfig config;
    
    /**
     * 
     */
    public CSVConfigGuesser() {
        this.config = new CSVConfig();
    }
    
    /**
     * @param in the inputstream to guess from
     */
    public CSVConfigGuesser(InputStream in) {
        this();
        setInputStream(in);
    }
    
    public void setInputStream(InputStream in) {
        this.in = in;
    }
    
    /**
     * Allow override.
     * @return the inputstream that was set.
     */
    protected InputStream getInputStream() {
        return in;
    }
    
    /**
     * Guess the config based on the first 10 (or less when less available) 
     * records of a CSV file.
     * 
     * @return the guessed config.
     */
    public CSVConfig guess() {
        try {
            // tralalal
            BufferedReader bIn = new BufferedReader(new InputStreamReader(getInputStream(), StandardCharsets.UTF_8));
            String[] lines = new String[10];
            String line = null;
            int counter = 0;
            while ( (line = bIn.readLine()) != null && counter <= 10) {
                lines[counter] = line;
                counter++;
            }
            if (counter < 10) {
                // remove nulls from the array, so we can skip the null checking.
                String[] newLines = new String[counter];
                System.arraycopy(lines, 0, newLines, 0, counter);
                lines = newLines;
            }
            analyseLines(lines);
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch(Exception e) {
                    // ignore exception.
                }
            }
        }
        CSVConfig conf = config;
        // cleanup the config.
        config = null;
        return conf;
    }
    
    protected void analyseLines(String[] lines) {
        guessFixedWidth(lines);
        guessFieldSeperator(lines);
    }
    
    /**
     * Guess if this file is fixedwidth.
     * Just basing the fact on all lines being of the same length
     */
    protected void guessFixedWidth(String[] lines) {
        int lastLength = 0;
        // assume fixedlength.
        config.setFixedWidth(true);
        for (int i = 0; i < lines.length; i++) {
            if (i == 0) {
                lastLength = lines[i].length();
            } else {
                if (lastLength != lines[i].length()) {
                    config.setFixedWidth(false);
                }
            }
        }
    }
        

    protected void guessFieldSeperator(String[] lines) {
        if (config.isFixedWidth()) {
            guessFixedWidthSeperator(lines);
            return;
        }
        for (int i = 0; i < lines.length; i++) {
        }
    }
    
    protected void guessFixedWidthSeperator(String[] lines) {
        // keep track of the fieldlength
        int previousMatch = -1;
        for (int i = 0; i < lines[0].length(); i++) {
            char last = ' ';
            boolean charMatches = true;
            for (int j = 0; j < lines.length; j++) {
                if (j == 0) {
                    last = lines[j].charAt(i);
                }
                if (last != lines[j].charAt(i)) {
                    charMatches = false;
                    break;
                } 
            }
            if (charMatches) {
                if (previousMatch == -1) {
                    previousMatch = 0;
                }
                CSVField field = new CSVField();
                field.setName("field"+config.getFields().length+1);
                field.setSize((i-previousMatch));
                config.addField(field);
            }
        }
    }
    /**
     * 
     * @return if the field uses a field header. Defaults to false.
     */
    public boolean hasFieldHeader() {
        return hasFieldHeader;
    }

    /**
     * Specify if the CSV file has a field header
     * @param hasFieldHeader true or false
     */
    public void setHasFieldHeader(boolean hasFieldHeader) {
        this.hasFieldHeader = hasFieldHeader;
    }
    
 
}
