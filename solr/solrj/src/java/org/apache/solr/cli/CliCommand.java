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

import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.ZooKeeper;

import java.io.PrintStream;
import java.util.Map;

/**
 * base class for all CLI commands
 */
public abstract class CliCommand {

    protected ZooKeeper zk;
    protected ZkStateReader zkStateReader;
    protected CloudHttp2SolrClient solrClient;

    protected PrintStream out;
    protected PrintStream err;
    private String cmdStr;
    private String optionStr;

    /**
     * a CLI command with command string and options.
     * Using System.out and System.err for printing
     * @param cmdStr the string used to call this command
     * @param optionStr the string used to call this command
     */
    public CliCommand(String cmdStr, String optionStr) {
        this.out = System.out;
        this.err = System.err;
        this.cmdStr = cmdStr;
        this.optionStr = optionStr;
    }

    /**
     * Set out printStream (usable for testing)
     */
    public void setOut(PrintStream out) {
        this.out = out;
    }

    /**
     * Set err printStream (usable for testing)
     */
    public void setErr(PrintStream err) {
        this.err = err;
    }

    /**
     * set the zookeeper instance
     * @param zk the ZooKeeper instance.
     */
    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public void setZkStateReader(ZkStateReader zkStateReader) {
        this.zkStateReader = zkStateReader;
    }

    public void setSolrClient(CloudHttp2SolrClient solrClient) {
        this.solrClient = solrClient;
    }

    /**
     * get the string used to call this command
     */
    public String getCmdStr() {
        return cmdStr;
    }

    /**
     * get the option string
     */
    public String getOptionStr() {
        return optionStr;
    }

    /**
     * get a usage string, contains the command and the options
     */
    public String getUsageStr() {
        return cmdStr + " " + optionStr;
    }

    /**
     * add this command to a map. Use the command string as key.
     * @param cmdMap to use
     */
    public void addToMap(Map<String,CliCommand> cmdMap) {
        cmdMap.put(cmdStr, this);
    }

    /**
     * parse the command arguments
     * @param cmdArgs to use
     * @return this CliCommand
     * @throws CliParseException on exception
     */
    public abstract CliCommand parse(String[] cmdArgs) throws CliParseException;

    /**
     *
     * @throws CliException on exception
     */
    public abstract boolean exec() throws CliException;

}
