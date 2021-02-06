/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cli;

import java.util.Collections;
import java.util.List;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

class JLineZNodeCompleter implements org.jline.reader.Completer {

    private ZooKeeper zk;

    public JLineZNodeCompleter(ZooKeeper zk) {
        this.zk = zk;
    }

    private int completeCommand(String buffer, String token, List<Candidate> candidates) {
        for (String cmd : Stellar.getCommands()) {
            if (cmd.startsWith(token)) {
                candidates.add(new Candidate(cmd));
            }
        }
        return buffer.lastIndexOf(" ") + 1;
    }

    private void completeZNode(String buffer, String token, List<Candidate> candidates) {
     //   System.out.println("");
      //  System.out.println("complete for " + buffer + " " + token);
     //   System.out.println("");
        String path = token;
        int idx = path.lastIndexOf("/") + 1;
        String prefix = path.substring(idx);
        try {
            // Only the root path can end in a /, so strip it off every other prefix
            String dir = idx == 1 ? "/" : path.substring(0, idx - 1);
           // System.out.println("");
           // System.out.println("get children for " + dir);
          //  System.out.println("");
            List<String> children = zk.getChildren(dir, false);

           ///System.out.println("prefix=" + prefix);
         //   System.out.println("");
            for (String child : children) {
                if (child.startsWith(prefix)) {
                   // System.out.println("");
                   // System.out.println("add child " + dir +  "/" + child);
                   // System.out.println("");
                    String value = (dir.equals("/") ? dir : dir + "/") + child;
                    Candidate candidate = new Candidate(value,
                        child, null, null, null, null, false);

                    candidates.add(candidate);
                 //   System.out.println("candidates " + candidates);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        Collections.sort(candidates);
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        // Guarantee that the final token is the one we're expanding
        String theLine = line.line();
        theLine = theLine.substring(0, line.cursor());
        String token = "";
        if (!theLine.endsWith(" ")) {
            String[] tokens = theLine.split(" ");
            if (tokens.length != 0) {
                token = tokens[tokens.length - 1];
            }
        }

        if (token.startsWith("/")) {
            completeZNode(theLine, token, candidates);
        } else {
            completeCommand(theLine, token, candidates);
        }

       // System.out.println("complete candidates " + candidates);
    }
}
