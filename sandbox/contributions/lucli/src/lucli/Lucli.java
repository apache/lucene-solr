package lucli;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.*;
import org.gnu.readline.*;
import org.apache.lucene.queryParser.ParseException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Iterator;

/**
 * lucli Main class for lucli: the Lucene Command Line Interface
 * This class handles mostly the actual CLI part, command names, help, etc.
 */

public class Lucli {

	final static String DEFAULT_INDEX = "index"; //directory "index" under the current directory
	final static String HISTORYFILE = ".lucli"; //directory "index" under the current directory
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

	String fullPath;
	TreeMap commandMap = new TreeMap();
	LuceneMethods luceneMethods; //current cli class we're using
	boolean enableReadline; //false: use plain java. True: shared library readline

	/**
		Main entry point. The first argument can be a filename with an
		application initialization file.
		*/

	public Lucli(String[] args) throws ParseException, IOException {
		String line;

		fullPath = System.getProperty("user.home") +  System.getProperty("file.separator")
			+ HISTORYFILE;

		/*
		 * Initialize the list of commands
		 */

		addCommand("info", INFO, "Display info about the current Lucene Index. Example:info");
		addCommand("search", SEARCH, "Search the current index. Example: search foo", 1);
		addCommand("count", COUNT, "Return the number of hits for a search. Example: count foo", 1);
		addCommand("optimize", OPTIMIZE, "Optimize the current index");
		addCommand("quit", QUIT, "Quit/exit the program");
		addCommand("help", HELP, "Display help about commands.");
		addCommand("terms", TERMS, "Show the first " + MAX_TERMS + " terms in this index. Supply a field name to only show terms in a specific field. Example: terms");
		addCommand("index", INDEX, "Choose a different lucene index. Example index my_index", 1);
		addCommand("tokens", TOKENS, "Does a search and shows the top 10 tokens for each document. Verbose! Example: tokens foo", 1);
		addCommand("explain", EXPLAIN, "Explanation that describes how the document scored against query. Example: explain foo", 1);



		//parse command line arguments
		parseArgs(args);

		if (enableReadline)
			org.gnu.readline.Readline.load(ReadlineLibrary.GnuReadline  );
		else
			org.gnu.readline.Readline.load(ReadlineLibrary.PureJava  );

		Readline.initReadline("lucli"); // init, set app name, read inputrc



		Readline.readHistoryFile(fullPath);

		// read history file, if available

		File history = new File(".rltest_history");
		try {
			if (history.exists())
				Readline.readHistoryFile(history.getName());
		} catch (Exception e) {
			System.err.println("Error reading history file!");
		}

		// Set word break characters
		try {
			Readline.setWordBreakCharacters(" \t;");
		}
		catch (UnsupportedEncodingException enc) {
			System.err.println("Could not set word break characters");
			System.exit(0);
		}

		// set completer with list of words

		Readline.setCompleter(new Completer(commandMap));

		// main input loop

		luceneMethods = new LuceneMethods(DEFAULT_INDEX);

		while (true) {
			try {
				line = Readline.readline("lucli> ");
				if (line != null) {
					handleCommand(line);
				}
			} catch (UnsupportedEncodingException enc) {
				System.err.println("caught UnsupportedEncodingException");
				break;
			} catch (java.io.EOFException eof) {
				System.out.println("");//new line
				exit();
			} catch (IOException ioe) {
				ioe.printStackTrace(System.err);
			}
		}

		exit();
	}

	public static void main(String[] args) throws ParseException, IOException {
		new Lucli(args);
	}


	private void handleCommand(String line) throws IOException, ParseException {
		String [] words = tokenizeCommand(line);
		if (words.length == 0)
			return; //white space
		String query = "";
		//Command name and number of arguments
		switch (getCommandId(words[0], words.length - 1)) {
			case INFO:
				luceneMethods.info();
				break;
			case SEARCH:
				for (int ii = 1; ii < words.length; ii++) {
					query += words[ii] + " ";
				}
				luceneMethods.search(query, false, false);
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
				luceneMethods.search(query, false, true);
				break;
			case EXPLAIN:
				for (int ii = 1; ii < words.length; ii++) {
					query += words[ii] + " ";
				}
				luceneMethods.search(query, true, false);
				break;
			case HELP:
				help();
				break;
			case NOCOMMAND: //do nothing
				break;
			case UNKOWN:
				System.out.println("Unknown command:" + words[0] + ". Type help to get a list of commands.");
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

		try {
			Readline.writeHistoryFile(fullPath);
		} catch (IOException ioe) {
			error("while saving history:" + ioe);
		}
		Readline.cleanup();
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
		name.toLowerCase(); //treat uppercase and lower case commands the same
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
	 * Parse command line arguments
	 * Code inspired by http://www.ecs.umass.edu/ece/wireless/people/emmanuel/java/java/cmdLineArgs/parsing.html
	 */
	private void parseArgs(String[] args) {
		for (int ii = 0; ii < args.length; ii++) {
			// a little overkill for now, but foundation
			// for other args
			if (args[ii].startsWith("-")) {
				String arg = args[ii];
				if (arg.equals("-r")) {
					enableReadline = true;
				}
				else {
					usage();
					System.exit(1);
				}
			}
		}
	}

	private void usage() {
		message("Usage: lucli [-j]");
		message("Arguments:");
		message("\t-r: Provide tab completion and history using the GNU readline shared library ");
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
