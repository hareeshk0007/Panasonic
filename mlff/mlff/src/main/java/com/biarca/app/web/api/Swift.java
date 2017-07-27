/**
*
* *****************************************************************************
*
* Copyright (C) 2016 Biarca. All rights reserved.
* 
* \file		FileUploadController.java
* \author	harishk@biarca.com
* 
* Defines Swift Class and its operations.
* 
******************************************************************************
*/

package com.biarca.app.web.api;


import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import com.biarca.app.Main;
import com.biarca.app.web.api.Utils.StatusCode;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.twmacinta.util.MD5;

public class Swift
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Swift.class);

	private static ArrayList<NameValuePair> headers =
			new ArrayList<NameValuePair>();
	String object_path = "";
	int mChunkSize = 1024 * 1024 * 512; // 512 MB
	final static int mFileMaxSizeInGB = 5;
	final static int maxChunkSize = 2147483645;
	String enableChunkMD5 = "false";
	int part_counter = 0;
	String path = "";
	Socket socket = null;

	/*
	 * Default Constructor
	 *
	 * params : None
	 * returns : None
	 * 
	 */
	Swift()
	{
	}

	/*
	 * prepareUploadFileList : Method initialize the Swift url, and object path
	 *
	 * params : keystone
	 * params : bucket
	 * params : fileName
	 * params : contentLength
	 * params : instream
	 * params : etag
	 * params : lastModified
	 * params : transId
	 * params : swiftDate
	 * returns : StatusCode
	 * 
	 */
	public HttpStatus prepareUploadFile(Keystone keystone, String bucket,
			String fileName, String contentLength, InputStream instream,
			StringBuilder etag, StringBuilder lastModified,
			StringBuilder transId, StringBuilder swiftDate)
					throws ClientProtocolException,
	IllegalStateException, IOException, ParseException
	{
		HttpStatus status = null;
		try {
			int index = 0;
			String url = keystone.mSwiftURL;
			index = url.indexOf("/v1");
			object_path = url.substring(index);

			status = uploadFile(keystone, bucket, fileName, contentLength,
					instream, etag, lastModified, transId, swiftDate);
			return status;
		}
		catch(Exception e) {
			LOGGER.error(e.getMessage());
		}

		return status;
	}

	/*
	 * getFileLengthInGb : Method to get the given file size in GB
	 *
	 * params : mFileLength, file length in Bytes
	 * returns : file size in Giga Bytes
	 */
	public static double getFileLengthInGb(long mFileLength)
	{
		double kiloBytes = mFileLength / 1024;
		double megaBytes = kiloBytes / 1024;
		double gigaBytes = megaBytes / 1024;
		return gigaBytes;
	}

	/*
	 * uploadFile : Method to perform the upload operation to upload object
	 *
	 * params : keystone
	 * params : bucket
	 * params : objectName
	 * params : contentLength
	 * params : instream
	 * params : etag
	 * params : lastModified
	 * params : transId
	 * params : date
	 * returns : StatusCode
	 *
	 */
	public HttpStatus uploadFile(Keystone keystone, String bucket,
			String objectName, String contentLength, InputStream instream,
			StringBuilder etag,StringBuilder lastModified, StringBuilder transId,
			StringBuilder date) throws Exception
	{
		HttpPut putRequest = null;
		String url = "";
		int status = 0;
		int partCounter = 0;
		String fileName = objectName;
		DataOutputStream dos = null;
		BufferedReader br = null;
		String formatted = "";
		byte[] destBuffer = null;
		long totalLength = 0;
		int n = 0;
		int prevReadBytes = 0;
		MD5 md5 = new MD5();
		long now = Instant.now().toEpochMilli();
		int retryCount = 0;
		int maxRetries = 0;
		boolean uploaded = false;

		long modulus = Long.valueOf(contentLength) % mChunkSize;
		if (modulus == 0)
			LOGGER.info("Chunks count " +
				(((long) Long.valueOf(contentLength) / mChunkSize)));
		else
			LOGGER.info("Chunks count " +
					(((long) Long.valueOf(contentLength) / mChunkSize) + 1));
		destBuffer = new byte[mChunkSize];
		while ((n = instream.read(destBuffer, prevReadBytes,
				(mChunkSize - prevReadBytes))) != -1)
		{
			uploaded = false;
			maxRetries = 0;
			md5.Update(destBuffer, prevReadBytes, n);
			totalLength = totalLength + n;
			prevReadBytes += n;
			if ((prevReadBytes >= mChunkSize) ||
					totalLength >= Long.valueOf(contentLength)) {
				formatted = String.format("%08d", partCounter);
				path = object_path + "/" + bucket + "/" + objectName + "/" +
						now + "/" + formatted;
				try {
					while (maxRetries < 3) {
						socket = Main.factory.createSocket(Main.host, Main.port);
						dos = new DataOutputStream(socket.getOutputStream());
						br = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));
						LOGGER.info("Chunk : "  + fileName + "/" + now + "/"
							+ formatted + " is uploading, size : " +
							prevReadBytes);

						dos.write(("PUT " + path + " HTTP/1.0\r\n").getBytes());
						dos.write(("X-Auth-Token: " + keystone.mSwiftToken
								+ "\r\n").getBytes());
						dos.write(("Content-Length: "+ prevReadBytes + "\r\n")
								.getBytes());
						if (enableChunkMD5.equalsIgnoreCase("true")) {
							HashCode hash = Hashing.md5().hashBytes(
									destBuffer, 0, prevReadBytes);
							dos.write(("ETag: " + hash + "\r\n").getBytes());
						}
						dos.write(("\n").getBytes());
						dos.write(destBuffer, 0, prevReadBytes);
						dos.flush();

						String line = "";
						while((line = br.readLine()) != null) {
							if (line.contains("HTTP/1.1")) {
								status = Integer.parseInt(line.substring(9, 12));
								if(line.contains("HTTP/1.1 201")) {
									uploaded = true;
									LOGGER.info(fileName + "/" + now + "/" +
									formatted + " uploaded");
									break;
								}
								else if(line.contains("HTTP/1.1 401")) {
									LOGGER.info("Invalid credentials for "+
										fileName + "/" + now + "/" + formatted);
									keystone.getAuthenticationToken();
									break;
								} else if(line.contains("HTTP/1.1 500")) {
									LOGGER.info("Internal Error "+
									fileName + "/" + now + "/" + formatted);
									LOGGER.info("Sleeping for "+ Main.retryInterval);
									Thread.sleep(Main.retryInterval);
									LOGGER.info(fileName + "/" + now + "/"
										+ formatted + " retry count : "+ maxRetries);
									break;
								} else if(line.contains("HTTP/1.1 503")) {
									LOGGER.info("Service unavailable "+
										fileName + "/" + now + "/" + formatted);
									LOGGER.info("Sleeping for "+ Main.retryInterval);
									LOGGER.info(fileName + "/" + now + "/"
										+ formatted + " retry count : "+ maxRetries);
									Thread.sleep(Main.retryInterval);
									break;
								} else {
									LOGGER.info("Error in uploading "+
										fileName + "/" + now + "/" + formatted);
									LOGGER.info("Error code : " + line);
									break;
								}
							}
						}
						br.close();
						dos.close();
						maxRetries++;
						if (uploaded)
							break;
					}
				}
				catch(Exception e) {
					try {
						LOGGER.info("Exception occured in uploading: "
								+ e.toString() + " " + fileName + "/" +
								now + "/" + formatted);

						keystone.getAuthenticationToken();
						socket = Main.factory.createSocket(Main.host,
								Main.port);
						dos = new DataOutputStream(socket.getOutputStream());
						br = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));
						LOGGER.info("Chunk : "  + fileName + "/" + now + "/"
								+ formatted + " is uploading, size : "+
								prevReadBytes);

						dos.write(("PUT " + path + " HTTP/1.0\r\n").getBytes());
						dos.write(("X-Auth-Token: " + keystone.mSwiftToken
								+ "\r\n").getBytes());
						dos.write(("Content-Length: "+ prevReadBytes + "\r\n")
								.getBytes());
						if (enableChunkMD5.equalsIgnoreCase("true")) {
							HashCode hash = Hashing.md5().hashBytes(
									destBuffer, 0, prevReadBytes);
							dos.write(("ETag: " + hash + "\r\n").getBytes());
						}
						dos.write(("\n").getBytes());
						dos.write(destBuffer, 0, prevReadBytes);
						dos.flush();

						String line = "";
						while((line = br.readLine()) != null) {
							if (line.contains("HTTP/1.1")) {
								status = Integer.parseInt(line.substring(9, 12));
								if(line.contains("HTTP/1.1 201")) {
									LOGGER.info(fileName + "/" + now + "/" +
									formatted + " uploaded");
									uploaded = true;
									break;
								} else if(line.contains("HTTP/1.1 500")) {
									LOGGER.info("Internal Error in the inner "
										+ "exception block " + fileName + "/" +
										now + "/" + formatted);
									LOGGER.info("Sleeping for "+ Main.retryInterval);
									LOGGER.info(fileName + "/" + now + "/"
										+ formatted + " retry count : "+ maxRetries);
									Thread.sleep(Main.retryInterval);
									break;
								} else if(line.contains("HTTP/1.1 503")) {
									LOGGER.info("Service unavailable in the inner "
										+ "exception block " + fileName + "/" +
										now + "/" + formatted);
									LOGGER.info("Sleeping for "+ Main.retryInterval);
									LOGGER.info(fileName + "/" + now + "/"
										+ formatted + " retry count : "+ maxRetries);
									Thread.sleep(Main.retryInterval);
									break;
								} else {
									LOGGER.info("Error in uploading in the inner "
										+ "exception block " + fileName + "/" +
										now + "/" + formatted);
									break;
								}
							}
						}
					}
					catch(ConnectException ex) {
						LOGGER.info("Connection Exception occured in the inner "
							+ "exception block for  uploading: " + e.getMessage()
							+ " " + fileName + "/" + now + "/" + formatted);
						status = HttpStatus.SERVICE_UNAVAILABLE.value();
					}
					catch(Exception ex) {
						LOGGER.info("Exception occured in the inner exception block"
							+ " for  uploading: " + e.getMessage()
							+ " " + fileName + "/" + now + "/" + formatted);
					}
				}
				finally {
					if (br != null)
						br.close();
					if (dos != null)
						dos.close();
				}
				partCounter++;
				prevReadBytes = 0;

				if (uploaded == false)
					return HttpStatus.valueOf(status);
			}
		}

		etag.append(md5.asHex());
		destBuffer = null;

		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		CloseableHttpResponse httpResponse = null;
		try {
			while (retryCount < 3) {
				// Sending the Manifest
				url = keystone.mSwiftURL + "/" + bucket + "/" + objectName;
				LOGGER.info("Manifest file sending : "+ url);
				putRequest = new HttpPut(url);
				headers.clear();
				headers.add(new BasicNameValuePair("X-Auth-Token",
						keystone.mSwiftToken));
				headers.add(new BasicNameValuePair("X-Object-Manifest",  bucket
						+ "/" + objectName + "/" + now));
				headers.add(new BasicNameValuePair("X-Object-Meta-ETag",
						etag.toString()));

				for(NameValuePair h : headers)
					putRequest.addHeader(h.getName(), h.getValue());

				httpResponse = httpClient.execute(putRequest);
				status = httpResponse.getStatusLine().getStatusCode();
				LOGGER.info(bucket + "/" + fileName + " Manifest file "
						+ "status : "+ status);
				Header[] headers = httpResponse.getAllHeaders();
				for (Header header : headers) {
					if (header.getName().equals("Last-Modified"))
						lastModified.append(header.getValue());
					else if (header.getName().equals("X-Trans-Id"))
						transId.append(header.getValue());
					else if (header.getName().equals("Date"))
						date.append(header.getValue());
				}
				if (status == 201) {
					break;
				}
				else if (status == 404) {
					break;
				}
				else if (status == 401) {
					lastModified.setLength(0);
					transId.setLength(0);
					date.setLength(0);
					keystone.getAuthenticationToken();
				}
				else if (status == 403) {
					break;
				} else if (status == 500) {
					lastModified.setLength(0);
					transId.setLength(0);
					date.setLength(0);
					LOGGER.info(fileName + " retry count : "+ maxRetries);
					LOGGER.info("Sleeping for "+ Main.retryInterval);
					Thread.sleep(Main.retryInterval);
				} else if (status == 503) {
					lastModified.setLength(0);
					transId.setLength(0);
					date.setLength(0);
					LOGGER.info(fileName + " retry count : "+ maxRetries);
					LOGGER.info("Sleeping for "+ Main.retryInterval);
					Thread.sleep(Main.retryInterval);
				} else {
					break;
				}
				retryCount++;
			}
		} catch(Exception e) {
			LOGGER.error(e.getMessage());
		}
		finally {
			instream.close();
			httpClient.close();
			httpResponse.close();
			socket.close();
			putRequest = null;
		}

		return HttpStatus.valueOf(status);
	}

	/*
	 * getChunkConfigDetails : Method to get the Chunksize and enableChunkMD5
	 * config values
	 *
	 * params : none
	 * returns : StatusCode
	 */
	public StatusCode getChunkConfigDetails() throws  IOException
	{
		Properties prop = new Properties();
		InputStream is = null;
		try {
			is = new FileInputStream(Main.configFile);
			prop.load(is);
			if (prop.getProperty("chunk_size") != null &&
					!prop.getProperty("chunk_size").equals("")) {
				if (Long.parseLong(
						prop.getProperty("chunk_size")) > maxChunkSize ||
						Long.parseLong(prop.getProperty("chunk_size")) < 0) {
						System.out.println("Chunk size limit is 0-2147483645");
						return StatusCode.INVALID_PARAMETERS;
				}
				mChunkSize = Integer.valueOf(prop.getProperty("chunk_size"));
			}
			if ((prop.getProperty("enable_chunks_md5")) != null ) {
				if (!prop.getProperty("enable_chunks_md5").equals("")) {
					enableChunkMD5 = prop.getProperty("enable_chunks_md5");
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		finally {
			is.close();
			prop.clear();
		}
		return StatusCode.SUCCESS;
	}
}
