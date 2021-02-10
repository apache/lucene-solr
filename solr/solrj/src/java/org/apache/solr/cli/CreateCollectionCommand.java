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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;

/**
 * create command for cli
 */
public class CreateCollectionCommand extends CliCommand {

    private static Options options = new Options();
    private String[] args;
    private CommandLine cl;

    static {
        options.addOption(new Option("c", "config", true, "config set"));
      //  options.addOption(new Option("r", "router", true, "router"));
        options.addOption(new Option("s", "num_shards", true, "shards"));
        options.addOption(new Option("rf", "replication_factor", true, "replication factor"));
        options.addOption(new Option("n", "nrt_replicas", true, "number of nrt replicas"));
        options.addOption(new Option("p", "pull_replicas", true, "number of pull replicas"));
        options.addOption(new Option("t", "tlog_replicas", true, "number of tlog replicas"));
    }

    public CreateCollectionCommand() {
        super("create_collection", "[-c config] [-s num_shards] [[-rf replication_factor]|[[-n nrt_replicas] [-p pull_replicas] [-t tlog_replicas]] name");
    }

    @Override
    public CliCommand parse(String[] cmdArgs) throws CliParseException {
        Parser parser = new PosixParser();
        try {
            cl = parser.parse(options, cmdArgs);
        } catch (ParseException ex) {
            throw new CliParseException(ex);
        }
        args = cl.getArgs();
        if (args.length < 2) {
            throw new CliParseException(getUsageStr());
        }
        return this;
    }

    @Override
    public boolean exec() throws CliException {
        // System.out.println("create shards=" + cl.getOptionValue("s", "1") + " " + args[1]);
        String name = args[1];

        String config = cl.getOptionValue("c", null);
       // String router = cl.getOptionValue("r", null);

        int numShards = Integer.parseInt(cl.getOptionValue("s", "1"));

        if (cl.hasOption("rf")) {
            if (cl.hasOption("n") || cl.hasOption("p")|| cl.hasOption("t")) {
                throw new MalformedCommandException("Cannot specify number of replica types with replication factory");
            }
        }

        CollectionAdminRequest.Create create;
        if (cl.hasOption("rf")) {
            String replication = cl.getOptionValue("rf", "1");
            int replicationInt =  Integer.parseInt(replication);
            create = CollectionAdminRequest.createCollection(name, config, numShards, replicationInt);
        } else {
            String nrtReplicas = cl.getOptionValue("n", "0");
            String pullReplics = cl.getOptionValue("p", "0");
            String tlogLogReplicas = cl.getOptionValue("t", "0");

            int nrtReplicasInt = Integer.parseInt(nrtReplicas);
            int pullReplicsInt = Integer.parseInt(pullReplics);
            int tlogLogReplicasInt = Integer.parseInt(tlogLogReplicas);

            create = CollectionAdminRequest.createCollection(name, config, numShards, nrtReplicasInt, tlogLogReplicasInt, pullReplicsInt);
        }


        try {
            create.process(solrClient);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

}
