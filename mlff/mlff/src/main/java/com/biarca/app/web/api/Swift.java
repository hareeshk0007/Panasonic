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
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

import com.biarca.app.Main;
import com.biarca.app.web.api.Utils.StatusCode;

public class Swift
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Swift.class);

	private static ArrayList<NameValuePair> headers =
			new ArrayList<NameValuePair>();
	String object_path = "";
	int mSplitFileSize = 1024 * 1024 * 512; // 512 MB
	public final static int mFileMaxSizeInGB = 5;
	int part_counter = 0;
	String path = "";
	public static Socket socket = null;

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
	public StatusCode prepareUploadFile(Keystone keystone, String bucket,
			String fileName, String contentLength, InputStream instream,
			StringBuilder etag, StringBuilder lastModified,
			StringBuilder transId, StringBuilder swiftDate)
					throws ClientProtocolException,
	IllegalStateException, IOException, ParseException
	{
		StatusCode status = StatusCode.UNKNOWN;
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
	public StatusCode uploadFile(Keystone keystone, String bucket,
			String objectName, String contentLength, InputStream instream,
			StringBuilder etag,StringBuilder lastModified, StringBuilder transId,
			StringBuilder date) throws Exception
	{
		StatusCode statusCode = StatusCode.UNKNOWN;
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		CloseableHttpResponse httpResponse = null;
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
		MessageDigest md = MessageDigest.getInstance("MD5");
		long now = Instant.now().toEpochMilli();

		LOGGER.info("Chunks count " + 
				(((long) Long.valueOf(contentLength) / mSplitFileSize) + 1));
		destBuffer = new byte[mSplitFileSize];
		while ((n = instream.read(destBuffer, prevReadBytes,
				(mSplitFileSize - prevReadBytes))) != -1)
		{
			md.update(destBuffer, prevReadBytes, n);
			totalLength = totalLength + n;
			prevReadBytes += n;
			if ((prevReadBytes >= mSplitFileSize) ||
					totalLength >= Long.valueOf(contentLength)) {
				formatted = String.format("%08d", partCounter);
				path = object_path + "/" + bucket + "/" + objectName + "/" +
						now + "/" + formatted;
				try {
					socket = Main.factory.createSocket(Main.host, Main.port);
					dos = new DataOutputStream(socket.getOutputStream());
					br = new BufferedReader(
							new InputStreamReader(socket.getInputStream()));

					String checksum = calculateMD5CheckSum(destBuffer,
							prevReadBytes);
					LOGGER.info("Chunk : "  + fileName + "/" + now + "/"
							+ formatted + " is uploading, size : "+
							prevReadBytes);

					dos.write(("PUT " + path + " HTTP/1.0\r\n").getBytes());
					dos.write(("Content-Length: "+ prevReadBytes + "\r\n")
							.getBytes());
					dos.write(("X-Auth-Token: " + keystone.mSwiftToken
							+ "\r\n").getBytes());
					dos.write(("ETag: " + checksum + "\r\n").getBytes());
					dos.write(("\n").getBytes());
					dos.write(destBuffer, 0, prevReadBytes);
					dos.flush();

					prevReadBytes = 0;
					String line = "";

					while((line = br.readLine()) != null)
					{
						if(line.contains("HTTP/1.1 201")) {
							LOGGER.info(fileName + "/" + now + "/" + formatted
							+ " uploaded");
							break;
						}						
						else if(line.contains("HTTP/1.1 401")) {
							LOGGER.info("Invalid credentials : " + 
									fileName + "/" + now + "/" + formatted);
							break;
						}
						else if(line.contains("HTTP/1.1 403")) {
							LOGGER.info("Permission denied : " + 
									fileName + "/" + now + "/" + formatted);
							break;
						}
						else if(line.contains("HTTP/1.1 404")) {
							statusCode = StatusCode.OBJECT_NOT_FOUND;
							LOGGER.info("Bucket not found : "+ 
									fileName + "/" + now + "/" + formatted);
							break;
						}
						else if(line.contains(
								"HTTP/1.1 422 Unprocessable Entity")) {
							LOGGER.info("HTTP/1.1 422 Unprocessable Entity : " + 
									fileName + "/" + now + "/" + formatted);
							break;
						}
						else {
							statusCode = StatusCode.UNKNOWN;
							LOGGER.info("Error Code : " + line + " : " +
									fileName + "/" + now + "/" + formatted);
							break;
						}
					}
				}
				catch(Exception e){
					LOGGER.error(e.getMessage());
				}
				finally {
					br.close();
					dos.close();
					System.gc();
				}
				partCounter++;
			}
			if (statusCode == StatusCode.OBJECT_NOT_FOUND) {
				instream.close();
				httpClient.close();
				socket.close();
				putRequest = null;
				return statusCode;
			}
		}

		byte[] mdbytes = md.digest();
		for(int j = 0; j < mdbytes.length; j++)
			etag.append(Integer.toString((
					mdbytes[j] & 0xff) + 0x100, 16).substring(1));
		destBuffer = null;

		try {
			// Sending the Manifest
			url = keystone.mSwiftURL + "/" + bucket + "/" + objectName;
			LOGGER.info("Manifest file sending : "+ url);
			putRequest = new HttpPut(url);
			headers.clear();
			headers.add(new BasicNameValuePair("X-Auth-Token",
					keystone.mSwiftToken));
			headers.add(new BasicNameValuePair("X-Object-Manifest",  bucket +
					"/" + objectName + "/" + now));
			headers.add(new BasicNameValuePair("X-Object-Meta-ETag", 
					etag.toString()));

			for(NameValuePair h : headers)
				putRequest.addHeader(h.getName(), h.getValue());
	
			httpResponse = httpClient.execute(putRequest);
			status = httpResponse.getStatusLine().getStatusCode();
			LOGGER.info(bucket + "/" + fileName + " Manifest file "
					+ "status : "+ status);
			if (status == 201)
				statusCode = StatusCode.SUCCESS;
			else if (status == 404)
				statusCode = StatusCode.OBJECT_NOT_FOUND;

			Header[] headers = httpResponse.getAllHeaders();
			for (Header header : headers) {			
				if (header.getName().equals("Last-Modified"))
					lastModified.append(header.getValue());
				else if (header.getName().equals("X-Trans-Id"))
					transId.append(header.getValue());
				else if (header.getName().equals("Date"))
					date.append(header.getValue());
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

		return statusCode;
	}
	
	/*
	 * calculateMD5CheckSum : Method calculates and returns the MD5 checksum
	 * of a given buffer.
	 *
	 * params : buffer
	 * params : save
	 * returns : MD5 checksum string
	 */
	public String calculateMD5CheckSum(byte[] buffer, int save)
			throws NoSuchAlgorithmException, IOException
	{
		MessageDigest md = MessageDigest.getInstance("MD5");
		md.update(buffer, 0, save);
		byte[] mdbytes = md.digest();

		StringBuffer checksum = new StringBuffer();
		for(int i = 0; i < mdbytes.length; i++)
			checksum.append(Integer.toString((
					mdbytes[i] & 0xff) + 0x100, 16).substring(1));
		return checksum.toString();
	}

	/*
	 * getChunkSize : Get Chunk size
	 *
	 * params : none
	 * returns : StatusCode
	 */
	public StatusCode getChunkSize() throws  IOException
	{
		Properties prop = new Properties();
		InputStream is = null;
		try {
			is = new FileInputStream(Main.configFile);
			prop.load(is);
			if (prop.getProperty("chunk_size") != null) {
				if (!prop.getProperty("chunk_size").equals(""))
					mSplitFileSize = Integer.valueOf(
							prop.getProperty("chunk_size"));
			} else
				return StatusCode.INVALID_PARAMETERS;
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
