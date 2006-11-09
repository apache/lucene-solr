package org.apache.lucene.search;

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

import java.util.Random;

import junit.framework.TestCase;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;

public class BaseTestRangeFilter extends TestCase {

    public static final boolean F = false;
    public static final boolean T = true;
    
    RAMDirectory index = new RAMDirectory();
    Random rand = new Random(101); // use a set seed to test is deterministic
    
    int maxR = Integer.MIN_VALUE;
    int minR = Integer.MAX_VALUE;

    int minId = 0;
    int maxId = 10000;

    static final int intLength = Integer.toString(Integer.MAX_VALUE).length();
    
    /**
     * a simple padding function that should work with any int
     */
    public static String pad(int n) {
        StringBuffer b = new StringBuffer(40);
        String p = "0";
        if (n < 0) {
            p = "-";
            n = Integer.MAX_VALUE + n + 1;
        }
        b.append(p);
        String s = Integer.toString(n);
        for (int i = s.length(); i <= intLength; i++) {
            b.append("0");
        }
        b.append(s);
        
        return b.toString();
    }

    public BaseTestRangeFilter(String name) {
	super(name);
        build();
    }
    public BaseTestRangeFilter() {
        build();
    }
    
    private void build() {
        try {
            
            /* build an index */
            IndexWriter writer = new IndexWriter(index,
                                                 new SimpleAnalyzer(), T);

            for (int d = minId; d <= maxId; d++) {
                Document doc = new Document();
                doc.add(new Field("id",pad(d), Field.Store.YES, Field.Index.UN_TOKENIZED));
                int r= rand.nextInt();
                if (maxR < r) {
                    maxR = r;
                }
                if (r < minR) {
                    minR = r;
                }
                doc.add(new Field("rand",pad(r), Field.Store.YES, Field.Index.UN_TOKENIZED));
                doc.add(new Field("body","body", Field.Store.YES, Field.Index.UN_TOKENIZED));
                writer.addDocument(doc);
            }
            
            writer.optimize();
            writer.close();

        } catch (Exception e) {
            throw new RuntimeException("can't build index", e);
        }

    }

    public void testPad() {

        int[] tests = new int[] {
            -9999999, -99560, -100, -3, -1, 0, 3, 9, 10, 1000, 999999999
        };
        for (int i = 0; i < tests.length - 1; i++) {
            int a = tests[i];
            int b = tests[i+1];
            String aa = pad(a);
            String bb = pad(b);
            String label = a + ":" + aa + " vs " + b + ":" + bb;
            assertEquals("length of " + label, aa.length(), bb.length());
            assertTrue("compare less than " + label, aa.compareTo(bb) < 0);
        }

    }

}
