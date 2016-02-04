/*
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
package org.apache.lucene.benchmark.byTask.utils;


import java.io.StreamTokenizer;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.tasks.RepSumByPrefTask;
import org.apache.lucene.benchmark.byTask.tasks.TaskSequence;

/**
 * Test algorithm, as read from file
 */
@SuppressWarnings("try")
public class Algorithm implements AutoCloseable {
  
  private TaskSequence sequence;
  private final String[] taskPackages;
  
  /**
   * Read algorithm from file
   * Property examined: alt.tasks.packages == comma separated list of 
   * alternate package names where tasks would be searched for, when not found 
   * in the default package (that of {@link PerfTask}{@link #getClass()}).
   * If the same task class appears in more than one package, the package 
   * indicated first in this list will be used.
   * @param runData perf-run-data used at running the tasks.
   * @throws Exception if errors while parsing the algorithm 
   */
  @SuppressWarnings("fallthrough")
  public Algorithm (PerfRunData runData) throws Exception {
    Config config = runData.getConfig();
    taskPackages = initTasksPackages(config);
    String algTxt = config.getAlgorithmText();
    sequence = new TaskSequence(runData,null,null,false);
    TaskSequence currSequence = sequence;
    PerfTask prevTask = null;
    StreamTokenizer stok = new StreamTokenizer(new StringReader(algTxt));
    stok.commentChar('#');
    stok.eolIsSignificant(false);
    stok.quoteChar('"');
    stok.quoteChar('\'');
    stok.ordinaryChar('/');
    stok.ordinaryChar('(');
    stok.ordinaryChar(')');
    boolean colonOk = false;
    boolean isDisableCountNextTask = false; // only for primitive tasks
    currSequence.setDepth(0);
    
    while (stok.nextToken() != StreamTokenizer.TT_EOF) { 
      switch(stok.ttype) {
  
        case StreamTokenizer.TT_WORD:
          String s = stok.sval;
          Constructor<? extends PerfTask> cnstr = taskClass(config,s)
            .asSubclass(PerfTask.class).getConstructor(PerfRunData.class);
          PerfTask task = cnstr.newInstance(runData);
          task.setAlgLineNum(stok.lineno());
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
            // get params, for tasks that supports them - allow recursive parenthetical expressions
            stok.eolIsSignificant(true);  // Allow params tokenizer to keep track of line number
            StringBuilder params = new StringBuilder();
            stok.nextToken();
            if (stok.ttype != ')') {
              int count = 1;
              BALANCED_PARENS: while (true) {
                switch (stok.ttype) {
                  case StreamTokenizer.TT_NUMBER: {
                    params.append(stok.nval);
                    break;
                  }
                  case StreamTokenizer.TT_WORD: {
                    params.append(stok.sval);
                    break;
                  }
                  case StreamTokenizer.TT_EOF: {
                    throw new RuntimeException("Unexpexted EOF: - "+stok.toString());
                  }
                  case '"':
                  case '\'': {
                    params.append((char)stok.ttype);
                    // re-escape delimiters, if any
                    params.append(stok.sval.replaceAll("" + (char)stok.ttype, "\\\\" + (char)stok.ttype));
                    params.append((char)stok.ttype);
                    break;
                  }
                  case '(': {
                    params.append((char)stok.ttype);
                    ++count;
                    break;
                  }
                  case ')': {
                    if (--count >= 1) {  // exclude final closing parenthesis
                      params.append((char)stok.ttype);
                    } else {
                      break BALANCED_PARENS;
                    }
                    break;
                  }
                  default: {
                    params.append((char)stok.ttype);
                  }
                }
                stok.nextToken();
              }
            }
            stok.eolIsSignificant(false);
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
                if (stok.ttype!=StreamTokenizer.TT_NUMBER)  {
                  throw new Exception("expected repetitions number or XXXs: - "+stok.toString());
                } else {
                  double num = stok.nval;
                  stok.nextToken();
                  if (stok.ttype == StreamTokenizer.TT_WORD && stok.sval.equals("s")) {
                    ((TaskSequence) prevTask).setRunTime(num);
                  } else {
                    stok.pushBack();
                    ((TaskSequence) prevTask).setRepetitions((int) num);
                  }
                }
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
                  String unit = stok.sval.toLowerCase(Locale.ROOT);
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
                name = stok.sval;
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

            case '&' :
              if (currSequence.isParallel()) {
                throw new Exception("Can only create background tasks within a serial task");
              }
              stok.nextToken();
              final int deltaPri;
              if (stok.ttype != StreamTokenizer.TT_NUMBER) {
                stok.pushBack();
                deltaPri = 0;
              } else {
                // priority
                deltaPri = (int) stok.nval;
              }

              if (prevTask == null) {
                throw new Exception("& was unexpected");
              } else if (prevTask.getRunInBackground()) {
                throw new Exception("double & was unexpected");
              } else {
                prevTask.setRunInBackground(deltaPri);
              }
              break;
    
            case '>' :
              currSequence.setNoChildReport(); /* intentional fallthrough */
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
      ArrayList<PerfTask> t = sequence.getTasks();
      if (t!=null && t.size()==1) {
        PerfTask p = t.get(0);
        if (p instanceof TaskSequence) {
          sequence = (TaskSequence) p;
          continue;
        }
      }
      break;
    }
  }

  private String[] initTasksPackages(Config config) {
    String alts = config.get("alt.tasks.packages", null);
    String dfltPkg = PerfTask.class.getPackage().getName();
    if (alts==null) {
      return new String[]{ dfltPkg };
    }
    ArrayList<String> pkgs = new ArrayList<>();
    pkgs.add(dfltPkg);
    for (String alt : alts.split(",")) {
      pkgs.add(alt);
    }
    return pkgs.toArray(new String[0]);
  }

  private Class<?> taskClass(Config config, String taskName)
      throws ClassNotFoundException {
    for (String pkg : taskPackages) {
      try {
        return Class.forName(pkg+'.'+taskName+"Task");
      } catch (ClassNotFoundException e) {
        // failed in this package, might succeed in the next one... 
      }
    }
    // can only get here if failed to instantiate
    throw new ClassNotFoundException(taskName+" not found in packages "+Arrays.toString(taskPackages));
  }

  @Override
  public String toString() {
    String newline = System.getProperty("line.separator");
    StringBuilder sb = new StringBuilder();
    sb.append(sequence.toString());
    sb.append(newline);
    return sb.toString();
  }

  /**
   * Execute this algorithm
   */
  public void execute() throws Exception {
    try {
      sequence.runAndMaybeStats(true);
    } finally {
      sequence.close();
    }
  }

  /**
   * Expert: for test purposes, return all tasks participating in this algorithm.
   * @return all tasks participating in this algorithm.
   */
  public ArrayList<PerfTask> extractTasks() {
    ArrayList<PerfTask> res = new ArrayList<>();
    extractTasks(res, sequence);
    return res;
  }
  private void extractTasks (ArrayList<PerfTask> extrct, TaskSequence seq) {
    if (seq==null) 
      return;
    extrct.add(seq);
    ArrayList<PerfTask> t = sequence.getTasks();
    if (t==null) 
      return;
    for (final PerfTask p : t) {
      if (p instanceof TaskSequence) {
        extractTasks(extrct, (TaskSequence)p);
      } else {
        extrct.add(p);
      }
    }
  }

  @Override
  public void close() throws Exception {
    sequence.close();
  }
  
}

