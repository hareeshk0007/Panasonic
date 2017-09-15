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
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.embedded.jetty.JettyEmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.jetty.JettyServerCustomizer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.biarca.app.web.api.FileUploadController;
import com.biarca.app.web.api.Keystone;
import com.biarca.app.web.api.Utils.StatusCode;

/*
 * Main : Main class to implement the MLLF software application
 *
 * params : args
 * returns : None
 * 
 */
@SpringBootApplication
@ConfigurationProperties
public class Main {
	public static String configFile = "";
	public static SocketFactory factory = null;
	public static SSLSocketFactory sslFactory = null;
	public static int retryInterval = 2000;

	public static int port = 0;
	public static String protocol = "";
	public static String host = "";

	private static final Logger LOGGER = LoggerFactory.getLogger(
			FileUploadController.class);

	public static void main(String... args) {
		try {
			if (args.length == 0) {
				System.out.println("Invalid number of arguments");
				return;
			}

			configFile = args[0];
			StatusCode status = readConfigFile(args[0]);
			if (status == StatusCode.OBJECT_NOT_FOUND) {
				System.out.println("Some of the mandatory fields are missing");
				return;
			}
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

	@Bean
	public JettyEmbeddedServletContainerFactory
		jettyEmbeddedServletContainerFactory(
		@Value("${server.port:9999}") final String port,
		@Value("${jetty.threadPool.maxThreads:200}") final String maxThreads,
		@Value("${jetty.threadPool.minThreads:8}") final String minThreads,
		@Value("${jetty.threadPool.idleTimeout:30000}")
			final String idleTimeout) {
		LOGGER.info("Intializing Jetty Server ....");
		LOGGER.info("port => " + port + " , maxThreads => " + maxThreads +
				" , minThreads => " + minThreads + ", idleTimeout => "+ idleTimeout );
		final JettyEmbeddedServletContainerFactory factory =
				new JettyEmbeddedServletContainerFactory(Integer.valueOf(port));
		factory.addServerCustomizers(new JettyServerCustomizer() {
			@Override
			public void customize(final Server server) {
				final QueuedThreadPool threadPool = server.getBean(
						QueuedThreadPool.class);
				threadPool.setMaxThreads(Integer.valueOf(maxThreads));
				threadPool.setMinThreads(Integer.valueOf(minThreads));
				threadPool.setIdleTimeout(Integer.valueOf(idleTimeout));
			}
		});
		return factory;
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
		retryInterval = Integer.parseInt(
				prop.getProperty("retry_interval"));
		Keystone.mAuthURL = prop.getProperty("auth_url");
		Keystone.mAdminUserName = prop.getProperty("username");
		Keystone.mAdminUserId = prop.getProperty("userid");
		Keystone.mAdminDomainName = prop.getProperty("domainname");
		Keystone.mAdminDomainId = prop.getProperty("domainid");
		Keystone.mAdminProjectId = prop.getProperty("projectid");
		Keystone.s3URL = prop.getProperty("s3_url");
		String password = prop.getProperty("password");
		try {
			if ((Keystone.mAdminUserId == null
				|| (Keystone.mAdminUserId.equals("")))
				|| (Keystone.mAdminProjectId == null)
				|| (Keystone.mAdminProjectId.equals("")
				|| password == null) || password.equals("")) {
				return StatusCode.OBJECT_NOT_FOUND;
			}
			else
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
