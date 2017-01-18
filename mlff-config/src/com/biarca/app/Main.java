/**
*
* *****************************************************************************
*
* Copyright (C) 2016 Biarca. All rights reserved.
* 
* \file		Main.java
* \author	harishk@biarca.com
* 
* This is the entry point of the MLFF config tool
* 
******************************************************************************
*/

package com.biarca.app;

/*
 * Main : Main class to implement the MLLF config tool
 *
 * params : args
 * returns : None
 *
 */
public class Main
{
	public static void main(String[] args) throws Exception 
	{
		UserConfig config = new UserConfig();
		boolean isPresent = config.checkConfigFile();
		if(!isPresent) {
			return;
		}

		if (args.length < 1 || args.length > 7)
			usage();
		else if (args.length == 5 && args[0].equals("--add-user")) {
			if (args[1].equals("--user-id") && args[3].equals("--password"))
			{
				String encrypt = Utils.aesEncrypt(args[4]);
				UserConfig.updateUserDetails(args[2], encrypt);
				System.out.println("Added user "+ args[2]);
			}
			else
				usage();
		} else if (args.length == 7 && args[0].equals("--add-user")) {
			if (args[1].equals("--user-id") && args[3].equals("--password") && 
					args[5].equals("--is-admin") && args[6].equalsIgnoreCase(
					"true"))
			{
				String encrypt = Utils.aesEncrypt(args[4]);
				UserConfig.updateAdminDetails(args[2], encrypt);
				System.out.println("Added admin user "+ args[2]);

			}
			else if (args[1].equals("--user-id") && args[3].equals("--password") 
					&& args[5].equals("--is-admin") && args[6].equalsIgnoreCase(
					"false"))
			{
				String encrypt = Utils.aesEncrypt(args[4]);
				UserConfig.updateUserDetails(args[2], encrypt);
				System.out.println("Added user "+ args[2]);

			}
			else
				usage();
			} else if (args.length == 3 && args[0].equals("--delete-user")) {
				if (args[1].equals("--user-id"))
				{
					boolean  found = UserConfig.deleteUser(args[2]);
					if (found)
						System.out.println("Removed user "+ args[2]);
					else
						System.out.println("Unable to find the user "+ args[2]);
				}
				else
					usage();
			}
			else if (args.length == 5 && args[0].equals("--delete-user")) {
				if (args[1].equals("--user-id") && args[3].equals("--is-admin") 
						&& args[4].equalsIgnoreCase("false"))
				{
					boolean  found = UserConfig.deleteUser(args[2]);
					if (found)
						System.out.println("Removed user "+ args[2]);
					else
						System.out.println("Unable to find the user "+ args[2]);
				}
				else if (args[1].equals("--user-id") && args[3].equals(
						"--is-admin") && args[4].equalsIgnoreCase("true"))
				{
					boolean  found = UserConfig.deleteAdminUser(args[2]);
					if (found)
						System.out.println("Removed user "+ args[2]);
					else
						System.out.println("Unable to find the user "+ args[2]);
				}
				else
					usage();
			}
			else
				usage();
	}

	static void usage()
	{
		System.out.println("Usage : java -jar program_name command "
				+ "mandatory_parameters options");
		System.out.println("Examples");
		System.out.println("java -jar jarName --add-user --user-id userID "
				+ "--password password");
		System.out.println("java -jar jarName --add-user --user-id userID "
				+ "--password password --is-admin true");
		System.out.println("java -jar jarName --add-user --user-id userID "
				+ "--password password --is-admin false");
		System.out.println("java -jar jarName --delete-user --user-id userID");
		System.out.println("java -jar jarName --delete-user --user-id userID "
				+ "--is-admin true");
		System.out.println("java -jar jarName --delete-user --user-id userID "
				+ "--is-admin false");
		System.out.println("--help Print this help");
	}
}
