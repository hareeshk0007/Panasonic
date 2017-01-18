/**
*
* *****************************************************************************
*
* Copyright (C) 2016 Biarca. All rights reserved.
* 
* \file		UserConfig.java
* \author	harishk@biarca.com
* 
* Defines UserConfig Class and its operations.
* 
*******************************************************************************
*/

package com.biarca.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.StandardCopyOption;

public class UserConfig
{
	static String configFile = "/opt/mlff/bin/application.properties";
	static String tempConfigFile = "/opt/mlff/bin/application.properties.tmp";

	/*
	 * Default constructor
	 */
	public UserConfig()
	{
	}

	/*
	 * checkConfigFile : Method to check for the presence of the config file
	 *
	 * params : None
	 * returns : true if present, false otherwise
	 */
	boolean checkConfigFile()
	{
		File file = new File(configFile);
			if (!file.exists()) {
				System.out.println("Config file not present");
				return false;
			}
		return true;
	}

	/*
	 * updateUserDetails : Method to update the config file with user details
	 *
	 * params : username
	 * params : password
	 * returns : None
	 */
	static void updateUserDetails(String username, String password)
	{
		try(FileWriter fw = new FileWriter(configFile, true);
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw))
		{
			out.println(username + "=" + password);
		}
		catch (IOException e)
		{
			System.out.println(e.getMessage());
		}
	}

	/*
	 * updateAdminDetails : Method to update the config file with admin details
	 *
	 * params : username
	 * params : password
	 * returns : None
	 */
	static void updateAdminDetails(String userId, String password) 
			throws IOException
	{
		try(FileWriter fw = new FileWriter(configFile, true);
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw))
		{
			out.println("userid" + "=" + userId);
			out.println("password" + "=" + password);

		}
		catch (IOException e)
		{
			System.out.println(e.getMessage());
		}
	}

	/*
	 * deleteUser : Method to delete the user details from the config file
	 *
	 * params : username
	 * returns : None
	 */
	static boolean deleteUser(String username) throws IOException
	{
		File inputFile = new File(configFile);
		File tempFile = new File(tempConfigFile);
		boolean found = false;

		BufferedReader reader = new BufferedReader(new FileReader(inputFile));
		BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));

		String currentLine;

		while((currentLine = reader.readLine()) != null) {
			if(!currentLine.startsWith(username)) {
				writer.write(currentLine + "\n");
			}
			else {
				if (currentLine.startsWith(username)) {
					String parts[] = currentLine.split("=");
					String string = parts[0].trim();
					if (!string.equals(string)) {
						writer.write(currentLine + "\n");
						found = false;
					} else if (string.equals(username)) {
						found = true;
					}
				}
			}
		}
		writer.close();
		reader.close(); 
		java.nio.file.Files.move(tempFile.toPath(), inputFile.toPath(), 
				StandardCopyOption.REPLACE_EXISTING);
		if (found)
			return true;
		else
			return false;
	}

	/*
	 * deleteAdminUser : Method to delete the admin user details from config file
	 *
	 * params : username
	 * returns : None
	 */
	static boolean deleteAdminUser(String username) throws IOException
	{
		File inputFile = new File(configFile);
		File tempFile = new File(tempConfigFile);
		boolean found = false;
		boolean match = false;

		BufferedReader reader = new BufferedReader(new FileReader(inputFile));
		BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
		String currentLine;

		while((currentLine = reader.readLine()) != null) {
			if(!currentLine.equals("")) {
				if (currentLine.startsWith("#"))
					continue;
					String[] split = currentLine.split("=");
					if (split[1].equals(username)) {
						found = true;
						continue;
					} else if (found) {
						match = true;
						found = false;
						continue;
					} else
						writer.write(currentLine + "\n");
			}
		}
		writer.close();
		reader.close(); 
		java.nio.file.Files.move(tempFile.toPath(), inputFile.toPath(), 
			StandardCopyOption.REPLACE_EXISTING);
		if (match)
			return true;
		else
			return false;
	}
}
