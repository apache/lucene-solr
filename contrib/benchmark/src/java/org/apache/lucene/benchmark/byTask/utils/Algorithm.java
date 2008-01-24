package org.apache.lucene.benchmark.byTask.utils;

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

import java.io.StreamTokenizer;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.tasks.RepSumByPrefTask;
import org.apache.lucene.benchmark.byTask.tasks.TaskSequence;

/**
 * Test algorithm, as read from file
 */
public class Algorithm {
  
  private TaskSequence sequence;
  
  /**
   * Read algorithm from file
   * @param runData perf-run-data used at running the tasks.
   * @throws Exception if errors while parsing the algorithm 
   */
  public Algorithm (PerfRunData runData) throws Exception {
    String algTxt = runData.getConfig().getAlgorithmText();
    sequence = new TaskSequence(runData,null,null,false);
    TaskSequence currSequence = sequence;
    PerfTask prevTask = null;
    StreamTokenizer stok = new StreamTokenizer(new StringReader(algTxt));
    stok.commentChar('#');
    stok.eolIsSignificant(false);
    stok.ordinaryChar('"');
    stok.ordinaryChar('/');
    stok.ordinaryChar('(');
    stok.ordinaryChar(')');
    stok.ordinaryChar('-');
    boolean colonOk = false; 
    boolean isDisableCountNextTask = false; // only for primitive tasks
    currSequence.setDepth(0);
    String taskPackage = PerfTask.class.getPackage().getName() + ".";
    
    Class paramClass[] = {PerfRunData.class};
    PerfRunData paramObj[] = {runData};
    
    while (stok.nextToken() != StreamTokenizer.TT_EOF) { 
      switch(stok.ttype) {
  
        case StreamTokenizer.TT_WORD:
          String s = stok.sval;
          Constructor cnstr = Class.forName(taskPackage+s+"Task").getConstructor(paramClass);
          PerfTask task = (PerfTask) cnstr.newInstance(paramObj);
          task.setDisableCounting(isDisableCountNextTask);
          isDisableCountNextTask = false;
          currSequence.addTask(task);
          if (task instanceof RepSumByPrefTask) {
            stok.nextToken();
            String prefix = stok.sval;
            if (prefix==null || prefix.length()==0) { 
              throw new Exception("named report prefix problem - "+stok.toString()); 
            }
            ((RepSumByPrefTask) task).setPrefix(prefix);
          }
          // check for task param: '(' someParam ')'
          stok.nextToken();
          if (stok.ttype!='(') {
            stok.pushBack();
          } else {
            // get params, for tasks that supports them, - anything until next ')'
            StringBuffer params = new StringBuffer();
            stok.nextToken();
            while (stok.ttype!=')') { 
              switch (stok.ttype) {
                case StreamTokenizer.TT_NUMBER:  
                  params.append(stok.nval);
                  break;
                case StreamTokenizer.TT_WORD:    
                  params.append(stok.sval);             
                  break;
                case StreamTokenizer.TT_EOF:     
                  throw new Exception("unexpexted EOF: - "+stok.toString());
                default:
                  params.append((char)stok.ttype);
              }
              stok.nextToken();
            }
            String prm = params.toString().trim();
            if (prm.length()>0) {
              task.setParams(prm);
            }
          }

          // ---------------------------------------
          colonOk = false; prevTask = task;
          break;
  
        default:
          char c = (char)stok.ttype;
          
          switch(c) {
          
            case ':' :
              if (!colonOk) throw new Exception("colon unexpexted: - "+stok.toString());
              colonOk = false;
              // get repetitions number
              stok.nextToken();
              if ((char)stok.ttype == '*') {
                ((TaskSequence)prevTask).setRepetitions(TaskSequence.REPEAT_EXHAUST);
              } else {
                if (stok.ttype!=StreamTokenizer.TT_NUMBER) 
                  throw new Exception("expected repetitions number: - "+stok.toString());
                ((TaskSequence)prevTask).setRepetitions((int)stok.nval);
              }
              // check for rate specification (ops/min)
              stok.nextToken();
              if (stok.ttype!=':') {
                stok.pushBack();
              } else {
                // get rate number
                stok.nextToken();
                if (stok.ttype!=StreamTokenizer.TT_NUMBER) throw new Exception("expected rate number: - "+stok.toString());
                // check for unit - min or sec, sec is default
                stok.nextToken();
                if (stok.ttype!='/') {
                  stok.pushBack();
                  ((TaskSequence)prevTask).setRate((int)stok.nval,false); // set rate per sec
                } else {
                  stok.nextToken();
                  if (stok.ttype!=StreamTokenizer.TT_WORD) throw new Exception("expected rate unit: 'min' or 'sec' - "+stok.toString());
                  String unit = stok.sval.toLowerCase();
                  if ("min".equals(unit)) {
                    ((TaskSequence)prevTask).setRate((int)stok.nval,true); // set rate per min
                  } else if ("sec".equals(unit)) {
                    ((TaskSequence)prevTask).setRate((int)stok.nval,false); // set rate per sec
                  } else {
                    throw new Exception("expected rate unit: 'min' or 'sec' - "+stok.toString());
                  }
                }
              }
              colonOk = false;
              break;
    
            case '{' : 
            case '[' :  
              // a sequence
              // check for sequence name
              String name = null;
              stok.nextToken();
              if (stok.ttype!='"') {
                stok.pushBack();
              } else {
                stok.nextToken();
                name = stok.sval;
                stok.nextToken();
                if (stok.ttype!='"' || name==null || name.length()==0) { 
                  throw new Exception("sequence name problem - "+stok.toString()); 
                }
              }
              // start the sequence
              TaskSequence seq2 = new TaskSequence(runData, name, currSequence, c=='[');
              currSequence.addTask(seq2);
              currSequence = seq2;
              colonOk = false;
              break;
    
            case '>' :
              currSequence.setNoChildReport();
            case '}' : 
            case ']' : 
              // end sequence
              colonOk = true; prevTask = currSequence;
              currSequence = currSequence.getParent();
              break;
          
            case '-' :
              isDisableCountNextTask = true;
              break;
              
          } //switch(c)
          break;
          
      } //switch(stok.ttype)
      
    }
    
    if (sequence != currSequence) {
      throw new Exception("Unmatched sequences");
    }
    
    // remove redundant top level enclosing sequences
    while (sequence.isCollapsable() && sequence.getRepetitions()==1 && sequence.getRate()==0) {
      ArrayList t = sequence.getTasks();
      if (t!=null && t.size()==1) {
        PerfTask p = (PerfTask) t.get(0);
        if (p instanceof TaskSequence) {
          sequence = (TaskSequence) p;
          continue;
        }
      }
      break;
    }
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  public String toString() {
    String newline = System.getProperty("line.separator");
    StringBuffer sb = new StringBuffer();
    sb.append(sequence.toString());
    sb.append(newline);
    return sb.toString();
  }

  /**
   * Execute this algorithm
   * @throws Exception 
   */
  public void execute() throws Exception {
    sequence.runAndMaybeStats(true);
  }

  /**
   * Expert: for test purposes, return all tasks participating in this algorithm.
   * @return all tasks participating in this algorithm.
   */
  public ArrayList extractTasks() {
    ArrayList res = new ArrayList();
    extractTasks(res, sequence);
    return res;
  }
  private void extractTasks (ArrayList extrct, TaskSequence seq) {
    if (seq==null) 
      return;
    extrct.add(seq);
    ArrayList t = sequence.getTasks();
    if (t==null) 
      return;
    for (int i = 0; i < t.size(); i++) {
      PerfTask p = (PerfTask) t.get(0);
      if (p instanceof TaskSequence) {
        extractTasks(extrct, (TaskSequence)p);
      } else {
        extrct.add(p);
      }
    }
  }
  
}

