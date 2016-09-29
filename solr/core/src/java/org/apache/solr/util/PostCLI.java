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

package org.apache.solr.util;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.exec.OS;
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for bin/post and bin/post.cmd
 */
public class PostCLI {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final boolean isWindows = (OS.isFamilyDOS() || OS.isFamilyWin9x() || OS.isFamilyWindows());
  private static final String THIS_SCRIPT = (isWindows ? "bin\\post.cmd" : "bin/post");

  public static final Option[] POST_OPTIONS = new Option[] {
      OptionBuilder
          .withArgName("collection")
          .hasArg()
          .isRequired(true)
          .withDescription("Collection to post to. Defaults to DEFAULT_SOLR_COLLECTION if not specified")
          .withLongOpt("collection")
          .create('c'),
      OptionBuilder
          .withArgName("url")
          .hasArg()
          .isRequired(false)
          .withDescription("<base Solr update URL> (overrides collection, host, and port)")
          .create("url"),
      OptionBuilder
          .withArgName("host")
          .hasArg()
          .isRequired(false)
          .withDescription("<host> (default: localhost)")
          .create("host"),
      OptionBuilder
          .withArgName("port")
          .hasArg()
          .isRequired(false)
          .withDescription("Number of shards; default is 1")
          .withLongOpt("port")
          .create("p"),
      OptionBuilder
          .withArgName("yes|no")
          .hasArg(true)
          .isRequired(false)
          .withDescription("Whether to commit at end of post (default: yes)")
          .create("commit"),
      OptionBuilder
          .hasArg(false)
          .isRequired(false)
          .withDescription("prints tool help")
          .withLongOpt("help")
          .create('h'),
      OptionBuilder
          .withArgName("depth")
          .hasArg(true)
          .isRequired(false)
          .withDescription("default: 1")
          .create("recursive"),
      OptionBuilder
          .withArgName("seconds")
          .hasArg(true)
          .isRequired(false)
          .withDescription("default: 10 for web, 0 for files")
          .create("delay"),
      OptionBuilder
          .withArgName("content-type")
          .hasArg(true)
          .isRequired(false)
          .withDescription("default: application/xml")
          .create("type"),
      OptionBuilder
          .withArgName("<type>[,<type>,...]")
          .hasArg(true)
          .isRequired(false)
          .withDescription("default: xml,json,jsonl,csv,pdf,doc,docx,ppt,pptx,xls,xlsx,odt,odp,ods,ott,otp,ots,rtf,htm,html,txt,log")
          .create("filetypes"),
      OptionBuilder
          .withArgName("<key>=<value>[&<key>=<value>...]")
          .hasArg(true)
          .isRequired(false)
          .withDescription("values must be URL-encoded; these pass through to Solr update request")
          .create("params"),
      OptionBuilder
          .withArgName("yes|no")
          .hasArg(true)
          .isRequired(false)
          .withDescription("default: no; yes outputs Solr response to console")
          .create("out"),
      OptionBuilder
          .withArgName("solr")
          .hasArg(true)
          .isRequired(false)
          .withDescription("sends application/json content as Solr commands to /update instead of /update/json/docs")
          .create("format")
  };

  private static void displayToolOptions() {
    // Could use HelpFormatter, but want more control over formatting...
    System.out.println(
        "\n" +
            "Usage: post -c <collection> [OPTIONS] <files|directories|urls|-d [\"...\",...]\n" +
            "    or post -hel\n" +
            "\n" +
            "   collection name defaults to DEFAULT_SOLR_COLLECTION if not specifie\n" +
            "\n" +
            "OPTION\n" +
            "======\n" +
            "  Solr options\n" +
            "    -url <base Solr update URL> (overrides collection, host, and port\n" +
            "    -host <host> (default: localhost\n" +
            "    -p or -port <port> (default: 8983\n" +
            "    -commit yes|no (default: yes\n" +
            "\n" +
            "  Web crawl options\n" +
            "    -recursive <depth> (default: 1\n" +
            "    -delay <seconds> (default: 10\n" +
            "\n" +
            "  Directory crawl options\n" +
            "    -delay <seconds> (default: 0\n" +
            "\n" +
            "  stdin/args options\n" +
            "    -type <content/type> (default: application/xml\n" +
            "\n" +
            "  Other options\n" +
            "    -filetypes <type>[,<type>,...] (default: xml,json,jsonl,csv,pdf,doc,docx,ppt,pptx,xls,xlsx,odt,odp,ods,ott,otp,ots,rtf,htm,html,txt,log\n" +
            "    -params \"<key>=<value>[&<key>=<value>...]\" (values must be URL-encoded; these pass through to Solr update request\n" +
            "    -out yes|no (default: no; yes outputs Solr response to console\n" +
            "    -format solr (sends application/json content as Solr commands to /update instead of /update/json/docs\n" +
            "\n" +
            "\n" +
            "Examples\n" +
            "\n" +
            "* JSON file:\n" + THIS_SCRIPT + " -c wizbang events.json" +
            "* XML files:\n" + THIS_SCRIPT + " -c records article*.xm\n" +
            "* CSV file:\n" + THIS_SCRIPT + " -c signals LATEST-signals.cs\n" +
            "* Directory of files:\n" + THIS_SCRIPT + " -c myfiles ~/Document\n" +
            "* Web crawl:\n" + THIS_SCRIPT + " -c gettingstarted http://lucene.apache.org/solr -recursive 1 -delay \n" +
            "* Standard input (stdin): echo '{commit: {}}' |\n" + THIS_SCRIPT + " -c my_collection -type application/json -out yes -\n" +
            "* Data as string:\n" + THIS_SCRIPT + " -c signals -type text/csv -out yes -d $'id,value\\n1,0.47'"
    );
  }

  public Option[] getOptions() {
    return POST_OPTIONS;
  }

  public int runTool(CommandLine cli) throws Exception {

    String solrUrl = cli.getOptionValue("url", SolrCLI.DEFAULT_SOLR_URL);
    String collection = cli.getOptionValue("c", System.getenv("DEFAULT_SOLR_COLLECTION"));
    Map<String,String> props = Collections.singletonMap("-Dauto", "yes");
    boolean recursive = false;
    String[] args = cli.getArgs();

    /*
    COLLECTION="$DEFAULT_SOLR_COLLECTION"
    PROPS=('-Dauto=yes')
    RECURSIVE=""
    FILES=()
    URLS=()
    ARGS=()

    while [ $# -gt 0 ]; do
      # TODO: natively handle the optional parameters to SPT
      #       but for now they can be specified as bin/post -c collection-name delay=5 http://lucidworks.com

      if [[ -d "$1" ]]; then
        # Directory
    #    echo "$1: DIRECTORY"
        RECURSIVE=yes
        FILES+=("$1")
      elif [[ -f "$1" ]]; then
        # File
    #    echo "$1: FILE"
        FILES+=("$1")
      elif [[ "$1" == http* ]]; then
        # URL
    #    echo "$1: URL"
        URLS+=("$1")
      else
        if [[ "$1" == -* ]]; then
          if [[ "$1" == "-c" ]]; then
            # Special case, pull out collection name
            shift
            COLLECTION="$1"
          elif [[ "$1" == "-p" ]]; then
            # -p alias for -port for convenience and compatibility with `bin/solr start`
            shift
            PROPS+=("-Dport=$1")
          elif [[ ("$1" == "-d" || "$1" == "--data" || "$1" == "-") ]]; then
            if [[ ! -t 0 ]]; then
              MODE="stdin"
            else
              # when no stdin exists and -d specified, the rest of the arguments
              # are assumed to be strings to post as-is
              MODE="args"
              shift
              if [[ $# -gt 0 ]]; then
                ARGS=("$@")
                shift $#
              else
                # SPT needs a valid args string, useful for 'bin/post -c foo -d' to force a commit
                ARGS+=("<add/>")
              fi
            fi
          else
            key="${1:1}"
            shift
    #       echo "$1: PROP"
            PROPS+=("-D$key=$1")
            if [[ "$key" == "url" ]]; then
              SOLR_URL=$1
            fi
          fi
        else
          echo -e "\nUnrecognized argument: $1\n"
          echo -e "If this was intended to be a data file, it does not exist relative to $PWD\n"
          exit 1
        fi
      fi
      shift
    done

    # Check for errors
    if [[ $COLLECTION == "" && $SOLR_URL == "" ]]; then
      echo -e "\nCollection or URL must be specified.  Use -c <collection name> or set DEFAULT_SOLR_COLLECTION in your environment, or use -url instead.\n"
      echo -e "See '$THIS_SCRIPT -h' for usage instructions.\n"
      exit 1
    fi

    # Unsupported: bin/post -c foo
    if [[ ${#FILES[@]} == 0 && ${#URLS[@]} == 0 && $MODE != "stdin" && $MODE != "args" ]]; then
      echo -e "\nNo files, directories, URLs, -d strings, or stdin were specified.\n"
      echo -e "See '" + THIS_SCRIPT + " -h' for usage instructions.\n"
      exit 1
    fi

    # SPT does not support mixing different data mode types, just files, just URLs, just stdin, or just argument strings.
    # The following are unsupported constructs:
    #    bin/post -c foo existing_file.csv http://example.com
    #    echo '<xml.../>' | bin/post -c foo existing_file.csv
    #    bin/post -c foo existing_file.csv -d 'anything'
    if [[ (${#FILES[@]} != 0 && ${#URLS[@]} != 0 && $MODE != "stdin" && $MODE != "args")
          || ((${#FILES[@]} != 0 || ${#URLS[@]} != 0) && ($MODE == "stdin" || $MODE == "args")) ]]; then
      echo -e "\nCombining files/directories, URLs, stdin, or args is not supported.  Post them separately.\n"
      exit 1
    fi

    PARAMS=""

    # TODO: let's simplify this
    if [[ $MODE != "stdin" && $MODE != "args" ]]; then
      if [[ $FILES != "" ]]; then
        MODE="files"
        PARAMS=("${FILES[@]}")
      fi

      if [[ $URLS != "" ]]; then
        MODE="web"
        PARAMS=("${URLS[@]}")
      fi
    else
      PARAMS=("${ARGS[@]}")
    fi

    PROPS+=("-Dc=$COLLECTION" "-Ddata=$MODE")
    if [[ -n "$RECURSIVE" ]]; then
      PROPS+=('-Drecursive=yes')
    fi

    echo "$JAVA" -classpath "${TOOL_JAR[0]}" "${PROPS[@]}" org.apache.solr.util.SimplePostTool "${PARAMS[@]}"
    "$JAVA" -classpath "${TOOL_JAR[0]}" "${PROPS[@]}" org.apache.solr.util.SimplePostTool "${PARAMS[@]}"
*/
    String[] toolargs = Arrays.asList("-Dc="+collection, "-Dauto=yes", args).toArray(new String[0]);

    SimplePostTool spt = new SimplePostTool(mode, url, auto, type, format, recursive, delay, fileTypes, out, commit, optimize, args);
    spt.
    return 0;
  }

  public static void main(String[] args) throws Exception {
    if (checkEmptyArgs(args)) {
      displayToolOptions();
      exit(1);
    }
    if (askForVersion(args)) {
      System.out.println(Version.LATEST);
      exit(0);
    }
    if (askForHelp(args)) {
      // Simple version tool, no need for its own class
      displayToolOptions();
      exit(0);
    }

    addHttpClientBuilder();

    CommandLine cli = buildCli(options(), args);

    log.info("CLI="+cli.toString());

    exit(new PostCLI().runTool(cli));
  }

  static CommandLine buildCli(Options options, String[] args) throws ParseException {
    return (new GnuParser()).parse(options(), args);
  }

  private static void addHttpClientBuilder() {
    String builderClassName = System.getProperty("solr.authentication.httpclient.builder");
    if (builderClassName!=null) {
      try {
        Class c = Class.forName(builderClassName);
        SolrHttpClientBuilder builder = (SolrHttpClientBuilder)c.newInstance();
        HttpClientUtil.setHttpClientBuilder(builder);
        log.info("Set HttpClientConfigurer from: "+builderClassName);
      } catch (Exception ex) {
        log.error(ex.getMessage());
        throw new RuntimeException("Error during loading of configurer '"+builderClassName+"'.", ex);
      }
    }
  }

  static boolean askForHelp(String[] args) {
    return (args.length == 1 && Arrays.asList("-h","-help","--help").contains(args[0]));
  }

  static boolean askForVersion(String[] args) {
    return (args.length == 1 && Arrays.asList("-v","-version","version").contains(args[0]));
  }

  static boolean checkEmptyArgs(String[] args) {
    return (args == null || args.length == 0 || args[0] == null || args[0].trim().length() == 0);
  }


  static Options options() {
    Options options = new Options();
    for (Option o : POST_OPTIONS) {
      options.addOption(o);
    }
    return options;
  }

  private static void exit(int exitStatus) {
    // TODO: determine if we're running in a test and don't exit
    try {
      System.exit(exitStatus);
    } catch (java.lang.SecurityException secExc) {
      if (exitStatus != 0)
        throw new RuntimeException("SolrCLI failed to exit with status "+exitStatus);
    }
  }

  static class PostToolArgs {
    String mode = "";
    String url;
    boolean auto = true;
    String type;
    String format;
    int recursive;
    int delay;
    String fileTypes;
    boolean out;
    boolean commit;
    boolean optimize;
    List<String> args;
  }
}
