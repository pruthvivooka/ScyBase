package com.scyllabase.Commands;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.scyllabase.ScyllaBase.displayError;
import static com.scyllabase.ScyllaBase.logMessage;
import static com.scyllabase.ScyllaBase.response;

/**
 * Created by scy11a on 6/21/17.
 */
public class UseCommand implements Command {

	private String command = null;
	private String currentDb;

	public UseCommand(String command) {
		this.command = command;
	}

	public String getCurrentDb() {
		return currentDb;
	}

	@Override
	public boolean execute() {
		if(this.command == null) {
			displayError("Command not initialized");
			return false;
		}
		return parseUseDatabaseString();
	}

	private boolean parseUseDatabaseString() {
		logMessage("Calling create on the query\n" + this.command);
		Pattern createTablePattern = Pattern.compile("^use ([a-z][a-z0-9]*)$");
		Matcher commandMatcher = createTablePattern.matcher(this.command);
		if(commandMatcher.find()) {
			String dbName = commandMatcher.group(1).trim();
			File dbFile = new File("Database/" + dbName);
			if(!dbFile.exists() || !dbFile.isDirectory()) {
				displayError("Database does not exist.");
				return false;
			}
			currentDb = dbName;
			response(dbName + " selected");
			return true;
		} else {
			CommandHelper.wrongSyntax();
			return false;
		}
	}
}
