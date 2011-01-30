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

package org.apache.solr.core;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

/**
 */
class RunExecutableListener extends AbstractSolrEventListener {
  public RunExecutableListener(SolrCore core) {
    super(core);
  }
  protected String[] cmd;
  protected File dir;
  protected String[] envp;
  protected boolean wait=true;

  @Override
  public void init(NamedList args) {
    super.init(args);

    List cmdlist = new ArrayList();
    cmdlist.add(args.get("exe"));
    List lst = (List)args.get("args");
    if (lst != null) cmdlist.addAll(lst);
    cmd = (String[])cmdlist.toArray(new String[cmdlist.size()]);

    lst = (List)args.get("env");
    if (lst != null) {
      envp = (String[])lst.toArray(new String[lst.size()]);
    }

    String str = (String)args.get("dir");
    if (str==null || str.equals("") || str.equals(".") || str.equals("./")) {
      dir = null;
    } else {
      dir = new File(str);
    }

    if ("false".equals(args.get("wait")) || Boolean.FALSE.equals(args.get("wait"))) wait=false;
  }

  /**
   * External executable listener.
   * 
   * @param callback Unused (As of solr 1.4-dev)
   * @return Error code indicating if the command has executed successfully. <br />
   *  0 , indicates normal termination.<br />
   *  non-zero , otherwise.
   */
  protected int exec(String callback) {
    int ret = 0;

    try {
      boolean doLog = log.isDebugEnabled();
      if (doLog) {
        log.debug("About to exec " + cmd[0]);
      }
      Process proc = Runtime.getRuntime().exec(cmd, envp ,dir);

      if (wait) {
        try {
          ret = proc.waitFor();
        } catch (InterruptedException e) {
          SolrException.log(log,e);
          ret = INVALID_PROCESS_RETURN_CODE;
        }
      }

      if (wait && doLog) {
        log.debug("Executable " + cmd[0] + " returned " + ret);
      }

    } catch (IOException e) {
      // don't throw exception, just log it...
      SolrException.log(log,e);
      ret = INVALID_PROCESS_RETURN_CODE;
    }

    return ret;
  }


  @Override
  public void postCommit() {
    // anything generic need to be passed to the external program?
    // the directory of the index?  the command that caused it to be
    // invoked?  the version of the index?
    exec("postCommit");
  }

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    exec("newSearcher");
  }

  /** Non-zero value for an invalid return code **/
  private static int INVALID_PROCESS_RETURN_CODE = -1;

}
