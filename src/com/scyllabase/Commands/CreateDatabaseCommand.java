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
public class CreateDatabaseCommand implements Command {

	private String command = null;

	public CreateDatabaseCommand(String command) {
		this.command = command;
	}

	@Override
	public boolean execute() {
		if(command == null) {
			displayError("Command not initialized");
			return false;
		}
		return parseCreateDatabaseString();
	}

	private boolean parseCreateDatabaseString() {
		logMessage("Calling create on the query\n" + this.command);
		Pattern createTablePattern = Pattern.compile("^create database ([a-z][a-z0-9]*)$");
		Matcher commandMatcher = createTablePattern.matcher(this.command);
		if(commandMatcher.find()) {
			String dbName = commandMatcher.group(1).trim();
			if(dbName.equalsIgnoreCase("catalog")) {
				displayError("You cannot create catalog database.");
				return false;
			}
			File dbFile = new File("Database/" + dbName);
			if(dbFile.exists() && dbFile.isDirectory()) {
				displayError("Database already exists.");
				return false;
			}
			if(dbFile.mkdir()) {
				response("Database " + dbName + "created");
			} else {
				displayError("Something went wrong making the file.");
				return false;
			}
		} else {
			CommandHelper.wrongSyntax();
			return false;
		}
		return true;
	}
}
