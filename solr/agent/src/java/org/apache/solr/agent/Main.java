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
package org.apache.solr.agent;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executable class for the Solr Agent. Accepts commands on the CLI. TODO: Work out the lifecycle and which commands
 * will send requests over a socket to an already running agent. If the Solr process that this agent is monitoring dies,
 * this agent will shut down automatically shortly afterwards.
 */
public class Main {
  private static final String JVM_IS_SHORT_TERM = "JVM_IS_SHORT_TERM";
  private static final String SOLR_AGENT_PROPERTIES = "SOLR_AGENT_PROPERTIES";
  private static final String DEFAULT_AGENT_PROPERTIES = "agent.properties";
  private static final AtomicBoolean osIsWindows = new AtomicBoolean(false);
  private static final AtomicBoolean jvmIsOracle = new AtomicBoolean(false);
  private static final AtomicBoolean jvmIsShortTerm = new AtomicBoolean(false);

  private static final Properties agentProps = new Properties();
  private static Path agentPropPath = null;

  public static void main(String[] args) {
    try {
      osIsWindows.set(System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win"));
    } catch (Exception e) {
      throw new RuntimeException("Could not determine OS name! This Java seems to be broken!", e);
    }

    try {
      jvmIsOracle.set(System.getProperty("java.vendor").toLowerCase(Locale.ROOT).contains("oracle"));
    } catch (Exception e) {
      throw new RuntimeException("Could not determine Java vendor! This Java seems to be broken!", e);
    }

    String javaVersion = null;
    String[] javaVersionElements = null;
    try {
      javaVersion = System.getProperty("java.version");
      /* If this property isn't set, attempting the split will throw NPE. */
      javaVersionElements = javaVersion.split("\\.|_");
    } catch (Exception e) {
      throw new RuntimeException("Could not determine Java version! This Java seems to be broken!", e);
    }

    int majorVersion;
    try {
      /* Java versions before 9 are shown as a 1.x version */
      if (javaVersionElements[0].equals("1")) {
        majorVersion = Integer.parseInt(javaVersionElements[1]);
      } else {
        majorVersion = Integer.parseInt(javaVersionElements[0]);
      }
    } catch (Exception e) {
      /* We should never reach this point if the java version is null. */
      throw new RuntimeException("Problem parsing java version: " + javaVersion, e);
    }

    /*
     * TODO: Unless the target version of this module is lowered, older versions of Java won't even run this class. This
     * error won't ever be seen.
     */
    if (majorVersion < 8) {
      throw new RuntimeException("Must have at least Java 8!");
    }

    if (majorVersion != 8 && majorVersion != 11 && majorVersion != 17) {
      System.err.println("This is likely not a long-term-support Java version.");
      System.err.println("Major version detected: " + majorVersion);
      System.err.println("Full version: " + javaVersion);
      jvmIsShortTerm.set(true);
    }

    readProperties();
    // TODO: Remove the validate/print command. Just here for debugging.
    validateAndPrintEnvironment();

    if (args.length <= 0) {
      System.err.println("No command!");
      System.exit(1);
    }

    int currentArg = 0;

    String command = args[currentArg].toLowerCase(Locale.ROOT);
    currentArg++;

    switch (command) {
      case "get_env":
        validateAndPrintEnvironment();
        break;

      default:
        System.err.println("Unknown command " + command);
        System.exit(1);
        break;
    }
  }

  private static void validateAndPrintEnvironment() {
    /* Create an object for building a list of env commands. */
    StringJoiner env = new StringJoiner("\n", "", "\n");

    /* If wee have no properties, we can't continue, so abort. */
    if (agentProps.size() <= 0) {
      System.err.println("No properties were loaded!");
      System.exit(1);
    }

    /* Add all of the properties for this agent to the list of env commands. */
    for (Object propKey : agentProps.keySet()) {
      String propVal = agentProps.getProperty((String) propKey);
      env.add(kvToEnvVar((String) propKey, propVal));
    }

    /*
     * TODO: Add checks for specific things that are easy to validate in Java. Add settings to the env commands so the
     * script can take action on what was detected -- abort the script, use special CLI options, etc.
     */
    if (jvmIsShortTerm.get()) {
      env.add(kvToEnvVar(JVM_IS_SHORT_TERM, "1"));
    }

    // Write the full set of env commands to stdout.
    System.out.println(env.toString());
  }

  /**
   * Find the file pointed to by SOLR_AGENT_PROPERTIES and read those properties into the class.
   */
  private static void readProperties() {
    String propFileName = System.getProperty(SOLR_AGENT_PROPERTIES);
    if (propFileName == null) {
      propFileName = System.getenv(SOLR_AGENT_PROPERTIES);
      if (propFileName == null) {
        propFileName = DEFAULT_AGENT_PROPERTIES;
      }
    }
    agentPropPath = Paths.get(propFileName);
    try (InputStream is = Files.newInputStream(agentPropPath);) {
      agentProps.load(is);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert a key/value pair to a command for setting an environment variable.
   * 
   * @param key
   *          A key or variable name.
   * @param value
   *          A value.
   * @return A single string as a shell command to set an environment variable.
   */
  private static String kvToEnvVar(String key, String value) {
    StringBuilder var = new StringBuilder(128);
    if (osIsWindows.get()) {
      var.append("SET ");
    }
    /* TODO: Decide whether we want to add "export" to these lines for bash. */
    var.append(key);
    var.append("=\"");
    var.append(value);
    var.append("\"");
    return var.toString();
  }
}
