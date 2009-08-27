package lucli;

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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.FileNameCompletor;
import jline.History;
import jline.SimpleCompletor;

import org.apache.lucene.queryParser.ParseException;

/**
 * Main class for lucli: the Lucene Command Line Interface.
 * This class handles mostly the actual CLI part, command names, help, etc.
 */
public class Lucli {

	final static String DEFAULT_INDEX = "index"; //directory "index" under the current directory
	final static String HISTORYFILE = ".lucli"; //history file in user's home directory
	public final static int MAX_TERMS = 100; //Maximum number of terms we're going to show

	// List of commands
	// To add another command, add it in here, in the list of addcomand(), and in the switch statement
	final static int NOCOMMAND = -2;
	final static int UNKOWN = -1;
	final static int INFO = 0;
	final static int SEARCH = 1;
	final static int OPTIMIZE = 2;
	final static int QUIT = 3;
	final static int HELP = 4;
	final static int COUNT = 5;
	final static int TERMS = 6;
	final static int INDEX = 7;
	final static int TOKENS = 8;
	final static int EXPLAIN = 9;
	final static int ANALYZER = 10;

	String historyFile;
	TreeMap commandMap = new TreeMap();
	LuceneMethods luceneMethods; //current cli class we're using
	boolean enableReadline; //false: use plain java. True: shared library readline

	/**
		Main entry point. The first argument can be a filename with an
		application initialization file.
		*/

	public Lucli(String[] args) throws IOException {
		String line;

		historyFile = System.getProperty("user.home") + File.separator	+ HISTORYFILE;

		/*
		 * Initialize the list of commands
		 */
		addCommand("info", INFO, "Display info about the current Lucene index. Example: info");
		addCommand("search", SEARCH, "Search the current index. Example: search foo", 1);
		addCommand("count", COUNT, "Return the number of hits for a search. Example: count foo", 1);
		addCommand("optimize", OPTIMIZE, "Optimize the current index");
		addCommand("quit", QUIT, "Quit/exit the program");
		addCommand("help", HELP, "Display help about commands");
		addCommand("terms", TERMS, "Show the first " + MAX_TERMS + " terms in this index. Supply a field name to only show terms in a specific field. Example: terms");
		addCommand("index", INDEX, "Choose a different lucene index. Example index my_index", 1);
		addCommand("tokens", TOKENS, "Does a search and shows the top 10 tokens for each document. Verbose! Example: tokens foo", 1);
		addCommand("explain", EXPLAIN, "Explanation that describes how the document scored against query. Example: explain foo", 1);
		addCommand("analyzer", ANALYZER, "Specifies the Analyzer class to be used. Example: analyzer org.apache.lucene.analysis.SimpleAnalyzer", 1);

		//parse command line arguments
		parseArgs(args);

		ConsoleReader cr = new ConsoleReader();
		//Readline.readHistoryFile(fullPath);
		cr.setHistory(new History(new File(historyFile)));
		
		// set completer with list of words
    Completor[] comp = new Completor[]{
            new SimpleCompletor(getCommandsAsArray()),
            new FileNameCompletor()
        };
    cr.addCompletor (new ArgumentCompletor(comp));

		// main input loop
		luceneMethods = new LuceneMethods(DEFAULT_INDEX);
		while (true) {
			try {
				line = cr.readLine("lucli> ");
				if (line != null) {
					handleCommand(line, cr);
				}
			} catch (java.io.EOFException eof) {
				System.out.println("");//new line
				exit();
			} catch (UnsupportedEncodingException enc) {
				enc.printStackTrace(System.err);
			} catch (ParseException pe) {
				pe.printStackTrace(System.err);
			} catch (IOException ioe) {
				ioe.printStackTrace(System.err);
			}
		}
	}

	private String[] getCommandsAsArray() {
		Set commandSet = commandMap.keySet();
		String[] commands = new String[commandMap.size()];
		int i = 0;
		for (Iterator iter = commandSet.iterator(); iter.hasNext();) {
			String	cmd = (String) iter.next();
			commands[i++] = cmd;
		}
		return commands;
	}

	public static void main(String[] args) throws IOException {
		new Lucli(args);
	}


	private void handleCommand(String line, ConsoleReader cr) throws IOException, ParseException {
		String [] words = tokenizeCommand(line);
		if (words.length == 0)
			return; //white space
		String query = "";
		if (line.trim().startsWith("#"))		// # = comment
			return;
		//Command name and number of arguments
		switch (getCommandId(words[0], words.length - 1)) {
			case INFO:
				luceneMethods.info();
				break;
			case SEARCH:
				for (int ii = 1; ii < words.length; ii++) {
					query += words[ii] + " ";
				}
				luceneMethods.search(query, false, false, cr);
				break;
			case COUNT:
				for (int ii = 1; ii < words.length; ii++) {
					query += words[ii] + " ";
				}
				luceneMethods.count(query);
				break;
			case QUIT:
				exit();
				break;
			case TERMS:
				if(words.length > 1)
					luceneMethods.terms(words[1]);
				else
					luceneMethods.terms(null);
				break;
			case INDEX:
				LuceneMethods newLm = new LuceneMethods(words[1]);
				try {
					newLm.info(); //will fail if can't open the index
					luceneMethods = newLm; //OK, so we'll use the new one
				} catch (IOException ioe) {
					//problem we'll keep using the old one
					error(ioe.toString());
				}
				break;
			case OPTIMIZE:
				luceneMethods.optimize();
				break;
			case TOKENS:
				for (int ii = 1; ii < words.length; ii++) {
					query += words[ii] + " ";
				}
				luceneMethods.search(query, false, true, cr);
				break;
			case EXPLAIN:
				for (int ii = 1; ii < words.length; ii++) {
					query += words[ii] + " ";
				}
				luceneMethods.search(query, true, false, cr);
				break;
			case ANALYZER:
				luceneMethods.analyzer(words[1]);
				break;
			case HELP:
				help();
				break;
			case NOCOMMAND: //do nothing
				break;
			case UNKOWN:
				System.out.println("Unknown command: " + words[0] + ". Type help to get a list of commands.");
				break;
		}
	}

	private String [] tokenizeCommand(String line) {
		StringTokenizer tokenizer = new StringTokenizer(line, " \t");
		int size = tokenizer.countTokens();
		String [] tokens = new String[size];
		for (int ii = 0; tokenizer.hasMoreTokens(); ii++) {
			tokens[ii]  = tokenizer.nextToken();
		}
		return tokens;
	}

	private void exit() {
		System.exit(0);
	}

	/**
	 * Add a command to the list of commands for the interpreter for a
	 * command that doesn't take any parameters.
	 * @param name  - the name of the command
	 * @param id  - the unique id of the command
	 * @param help  - the help message for this command
	 */
	private void addCommand(String name, int id, String help) {
		addCommand(name, id, help, 0);
	}

	/**
	 * Add a command to the list of commands for the interpreter.
	 * @param name  - the name of the command
	 * @param id  - the unique id of the command
	 * @param help  - the help message for this command
	 * @param params  - the minimum number of required params if any
	 */
	private void addCommand(String name, int id, String help, int params) {
		Command command = new Command(name, id, help, params);
		commandMap.put(name, command);
	}

	private int getCommandId(String name, int params) {
		name = name.toLowerCase(); //treat uppercase and lower case commands the same
		Command command = (Command) commandMap.get(name);
		if (command == null) {
			return(UNKOWN);
		}
		else {
			if(command.params > params) {
				error(command.name + " needs at least " + command.params + " arguments.");
				return (NOCOMMAND);
			}
			return (command.id);
		}
	}

	private void help() {
		Iterator commands = commandMap.keySet().iterator();
		while (commands.hasNext()) {
			Command command = (Command) commandMap.get(commands.next());
			System.out.println("\t" + command.name + ": " + command.help);

		}
	}

	private void error(String message) {
		System.err.println("Error:" + message);
	}

	private void message(String text) {
		System.out.println(text);
	}

	/*
	 * Parse command line arguments (currently none)
	 */
	private void parseArgs(String[] args) {
		if (args.length > 0) {
			usage();
			System.exit(1);
		}
	}

	private void usage() {
		message("Usage: lucli.Lucli");
		message("(currently, no parameters are supported)");
	}

	private class Command {
		String name;
		int id;
		int numberArgs;
		String help;
		int params;

		Command(String name, int id, String help, int params) {
			this.name = name;
			this.id = id;
			this.help = help;
			this.params = params;
		}

		/**
		 * Prints out a usage message for this command.
		 */
		public String commandUsage() {
			return (name + ":" + help + ". Command takes " + params + " params");
		}

	}
}
