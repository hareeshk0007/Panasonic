/**
*
* *****************************************************************************
*
* Copyright (C) 2016 Biarca. All rights reserved.
* 
* \file		Keystone.java
* \author	harishk@biarca.com
* 
* Implements the Main Method for the MLFF software application
* 
******************************************************************************
*/
package com.biarca.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.ParseException;
import java.util.Properties;
import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import org.apache.http.client.ClientProtocolException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.biarca.app.web.api.Keystone;
import com.biarca.app.web.api.Swift;
import com.biarca.app.web.api.Utils.StatusCode;

/*
 * Main : Main class to implement the MLLF software application
 *
 * params : args
 * returns : None
 * 
 */
@SpringBootApplication
public class Main {
	public static String configFile = "";
	public static SocketFactory factory = null;
	public static SSLSocketFactory sslFactory = null;

	public static int port = 0;
	public static String protocol = "";
	public static String host = "";

	final static int maxBufferSize = 2147483645;

	public static void main(String... args) {
		try {
			if (args.length == 0) {
				System.out.println("Invalid number of arguments");
				return;
			}

			configFile = args[0];
			StatusCode status = readConfigFile(args[0]);
			if (status != StatusCode.SUCCESS)
				return;
			URL mURL = new URL(Keystone.s3URL);
			protocol = mURL.getProtocol();
			port = mURL.getPort();
			host = mURL.getHost();

			if(protocol.equals("http"))
				factory = SocketFactory.getDefault();
			else if(protocol.equals("https"))
				sslFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();

		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		SpringApplication.run(Main.class, args);
	}
	
	/*
	 * readConfigFile
	 * params : confFileName
	 * returns : StatusCode
	 * 
	 */
	public static StatusCode readConfigFile(String confFileName)
			throws ParseException,  ClientProtocolException,
			IllegalStateException, IOException
	{
		StatusCode status = StatusCode.SUCCESS;
		File file = new File(confFileName);
		{
			if (!file.exists()) {
				System.out.println("Config file not present");
				return StatusCode.OBJECT_NOT_FOUND;
			}
		}
		Properties prop = new Properties();
		InputStream is = new FileInputStream(confFileName);

		prop.load(is);
		Keystone.mAuthURL = prop.getProperty("auth_url");
		Keystone.mAdminUserName = prop.getProperty("username");
		Keystone.mAdminUserId = prop.getProperty("userid");
		Keystone.mAdminDomainName = prop.getProperty("domainname");
		Keystone.mAdminDomainId = prop.getProperty("domainid");
		Keystone.mAdminProjectId = prop.getProperty("projectid");
		Keystone.s3URL = prop.getProperty("s3_url");
		if (prop.getProperty("chunk_size") != null) {
			Swift.mSplitFileSize = (int) Long.parseLong(
					prop.getProperty("chunk_size"));
			if (Swift.mSplitFileSize > maxBufferSize ||
					Swift.mSplitFileSize < 0) {
					System.out.println("Maximum allowed chunk size is " +
							maxBufferSize);
					status = StatusCode.INVALID_PARAMETERS;
			}
		}
		String password = prop.getProperty("password");
		try {
			Keystone.mAdminPassword = Keystone.decryptPassword(password);
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally
		{
			is.close();
		}

		return status;
	}
}
