/**
*
* *****************************************************************************
*
* Copyright (C) 2016 Biarca. All rights reserved.
* 
* \file		FileUploadController.java
* \author	harishk@biarca.com
* 
* Defines Swift3 Class and its operations.
* 
******************************************************************************
*/

package com.biarca.app.web.api;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;

import javax.net.ssl.SSLSocket;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import com.biarca.app.Main;

public class Swift3 {

	private static final Logger LOGGER = LoggerFactory.getLogger(Swift3.class);
	
	Socket socket = null;
	SSLSocket sslSocket = null;
	String postData = "";

	/*
	 * listBuckets : Method to list all the buckets for the given user
	 * params : date
	 * params : signature
	 * params : contentType
	 * params : bucketList
	 * params : servletResponse
	 * returns : StatusCode
	 * 
	 */
	public static HttpStatus listBuckets(String date, String signature,
			String contentType, StringBuilder bucketList,
			HttpServletResponse servletResponse) throws IOException {

		String strResponse = "";
		int responseCode = 0;
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		CloseableHttpResponse httpResponse = null;

		try {

			HttpGet httpGet = new HttpGet(Keystone.s3URL);
			httpGet.setHeader("Date", date);
			httpGet.setHeader("Authorization", signature);
			if (contentType != null)
				httpGet.setHeader("Content-Type", 
						"application/x-www-form-urlencoded; charset=utf-8");
			try {
				httpResponse = httpClient.execute(httpGet);
				responseCode = httpResponse.getStatusLine().getStatusCode();
				LOGGER.info("In listAllBuckets status : "+ responseCode);

					if (responseCode == 200) {
					Header[] headers = httpResponse.getAllHeaders();
					for (Header header : headers) {
						if (!header.getName().equals("Content-Length"))
							servletResponse.setHeader(header.getName(),
									header.getValue());
					}
					HttpEntity httpEntity = httpResponse.getEntity();
					if(httpEntity != null)
					{
						InputStream instream = httpEntity.getContent();
						strResponse = convertStreamToString(instream);
						bucketList.append(strResponse);
						instream.close();
					}
				}
			}
			catch (ConnectException e) {
				responseCode = HttpStatus.SERVICE_UNAVAILABLE.value();
				e.printStackTrace();
				LOGGER.warn(e.getMessage());
			}
			catch (Exception e) {
				e.printStackTrace();
				LOGGER.warn(e.getMessage());
			}
			finally
			{
				if (httpResponse != null)
					httpResponse.close();
			}
		}
		catch(Exception e)
		{
			LOGGER.warn(e.getMessage());
		}
		finally
		{
			httpClient.close();
		}

		return HttpStatus.valueOf(responseCode);
	}

	/*
	 * createBucket : Method to create the bucket
	 * params : date
	 * params : signature
	 * params : contentType
	 * params : bucketName
	 * params : result
	 * params : servletResponse
	 * returns : StatusCode
	 * 
	 */
	public static HttpStatus createBucket(String date, String signature,
			String contentType, String bucketName, StringBuilder result,
			HttpServletResponse servletResponse) throws IOException {
		int status = 0;
		HttpPut httpPut = null;
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		CloseableHttpResponse httpResponse = null;

		try {
	
			httpPut = new HttpPut(Keystone.s3URL + bucketName);
			httpPut.setHeader("Date", date);
			httpPut.setHeader("Authorization", signature);
			if (contentType != null)
				httpPut.setHeader("Content-type", 
						"application/x-www-form-urlencoded; charset=utf-8");
			try {
				httpResponse = httpClient.execute(httpPut);
				status = httpResponse.getStatusLine().getStatusCode();
				LOGGER.info("In Create bucket status : "+ status);
				
				Header[] headers = httpResponse.getAllHeaders();
				for (Header header : headers) {
					if (header.getName().startsWith("x-amz-id"))
						servletResponse.setHeader(header.getName(),
								header.getValue());
					if (header.getName().startsWith("x-amz-request-id"))
						servletResponse.setHeader(header.getName(),
								header.getValue());
					if (header.getName().startsWith("Content-Type"))
						servletResponse.setHeader(header.getName(),
								header.getValue());
					if (header.getName().startsWith("X-Trans-Id"))
						servletResponse.setHeader(header.getName(),
								header.getValue());
					if (header.getName().startsWith("Date"))
						servletResponse.setHeader(header.getName(),
								header.getValue());
					if (header.getName().startsWith("Location"))
						servletResponse.setHeader(header.getName(),
								header.getValue());
				}

				InputStream in = httpResponse.getEntity().getContent();
				result.append(convertStreamToString(in));
				if (in != null)
					in.close();
			} catch (ConnectException e) {
				status = HttpStatus.SERVICE_UNAVAILABLE.value();
				e.printStackTrace();
				LOGGER.warn(e.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.warn(e.getMessage());
			}
			finally
			{
				if (httpResponse != null)
					httpResponse.close();
			}
		}
		catch(Exception e)
		{
			LOGGER.warn(e.getMessage());
		}
		finally
		{
			httpClient.close();
		}

		return HttpStatus.valueOf(status);
	}

	/*
	 * deleteBucket : Method to delete the bucket
	 * params : date
	 * params : signature
	 * params : bucketName
	 * params : result
	 * params : servletResponse
	 * returns : StatusCode
	 * 
	 */
	public static HttpStatus deleteBucket(String date, String signature,
			String bucketname, StringBuilder result,
			HttpServletResponse servletResponse) throws IOException
	{
		String strResponse = "";
		int status = 0;
		HttpDelete httpDelete = null;
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		CloseableHttpResponse httpResponse = null;

		try {

			httpDelete = new HttpDelete(Keystone.s3URL + bucketname);
			httpDelete.setHeader("Date", date);
			httpDelete.setHeader("Authorization", signature);

			try {
				httpResponse = httpClient.execute(httpDelete);
				status = httpResponse.getStatusLine().getStatusCode();
				LOGGER.info("In DeleteBucket status : "+ status);

				HttpEntity httpEntity = httpResponse.getEntity();
				if(httpEntity != null)
				{
					InputStream instream = httpEntity.getContent();
					strResponse = convertStreamToString(instream);
					result.append(strResponse);
					instream.close();
				}

				Header[] headers = httpResponse.getAllHeaders();
				for (Header header : headers) {		
					if (!header.getName().equals("Transfer-Encoding"))
							servletResponse.setHeader(header.getName(), 
									header.getValue());
				}
			} catch (ConnectException e) {
				status = HttpStatus.SERVICE_UNAVAILABLE.value();
				e.printStackTrace();
				LOGGER.warn(e.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.warn(e.getMessage());
			}
			finally
			{
				if (httpResponse != null)
					httpResponse.close();
			}
		}
		catch(Exception e)
		{
			LOGGER.warn(e.getMessage());
		}
		finally
		{
			httpClient.close();
		}

		return HttpStatus.valueOf(status);
	}
	
	/*
	 * uploadObject : Method to upload the file which is lesser than 5GB
	 * params : date
	 * params : signature
	 * params : bucketName
	 * params : objectName
	 * params : contentLength
	 * params : contentType
	 * params : content_MD5
	 * params : instream
	 * params : result
	 * params : response
	 * returns : StatusCode
	 * 
	 */
	public HttpStatus uploadObject(String date, String signature, String bucketName,
			String objectName, String contentLength, String contentType,
			String content_MD5, InputStream instream,
			HttpServletResponse response) throws Exception
	{
		String path = "";
		int status = 0;
		DataOutputStream dos = null;
		BufferedReader br = null;

		try
		{
			path = "/" + objectName;
			if(Main.protocol.equals("http"))
			{
				socket = Main.factory.createSocket(Main.host, Main.port);
				dos = new DataOutputStream(socket.getOutputStream());
				br = new BufferedReader(
						new InputStreamReader(socket.getInputStream()));
			}
			else if(Main.protocol.equals("https"))
			{
				sslSocket = (SSLSocket) Main.sslFactory.createSocket(
						Main.host, Main.port);
				dos = new DataOutputStream(sslSocket.getOutputStream());
				br = new BufferedReader(new InputStreamReader(
						sslSocket.getInputStream()));
			}

			dos.write(("PUT " + path + " HTTP/1.0\r\n").getBytes());
			dos.write(("Date: "+ date + "\r\n").getBytes());
			dos.write(("Authorization: " + signature + "\n").getBytes());
			if (contentType != null) {
				dos.write(("Content-type: " + contentType + "\n").getBytes());
			}
			if (content_MD5 != null)
				dos.write(("Content-MD5: " + content_MD5 + "\n").getBytes());
			dos.write(("Content-length: " + contentLength + "\n").getBytes());
			dos.write(("\n").getBytes());

			IOUtils.copy(instream, dos);

			dos.write(postData.getBytes());
			dos.flush();

			String line = "";
			while((line = br.readLine()) != null)
			{
				String[] key_value = line.split(" ");
				String value = line.substring(line.indexOf(' ') + 1,
						line.length());
				if(!key_value[0].equals("HTTP/1.1")) {
					String key = key_value[0].substring(0, 
							key_value[0].length() -1);
					response.setHeader(key, value);
				}
				if (line.contains("HTTP/1.1")) {
					status = Integer.parseInt(line.substring(9, 12));
					if(line.contains("HTTP/1.1 200")) {
						LOGGER.info(objectName+ " uploaded to bucket ");
					} else
						LOGGER.info("Unable to upload object " + objectName +
							" : Status " + status);
				}
			}
		} catch (SocketException e) {
			e.printStackTrace();
			LOGGER.warn(e.getMessage());
			status = HttpStatus.SERVICE_UNAVAILABLE.value();
		}
		catch(Exception e)
		{
			LOGGER.warn(e.getMessage());
		}
		finally
		{
			instream.close();
			if (dos != null)
				dos.close();
			if (br != null)
				br.close();
		}

		return HttpStatus.valueOf(status);
	}

	/*
	 * deleteObject : Method to delete the object
	 * params : date
	 * params : signature
	 * params : contentType
	 * params : objectName
	 * params : result
	 * params : servletResponse
	 * returns : StatusCode
	 * 
	 */
	public static HttpStatus deleteObject(String date, String signature,
			String contentType, String objectName, StringBuilder result,
			HttpServletResponse servletResponse) throws IOException
	{
		int status = 0;
		String strResponse = "";
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		CloseableHttpResponse httpResponse = null;

		try
		{

			HttpDelete httpDelete = new HttpDelete(Keystone.s3URL + objectName);
			LOGGER.info("In DeleteObject : "+ objectName);

			httpDelete.setHeader("Date", date);
			httpDelete.setHeader("Authorization", signature);
			
			if (contentType != null)
				httpDelete.setHeader("Content-Type", 
						"application/x-www-form-urlencoded; charset=utf-8");
			try {
				httpResponse = httpClient.execute(httpDelete);
				status = httpResponse.getStatusLine().getStatusCode();
				LOGGER.info("In DeleteObject status : "+ status);
				
				HttpEntity httpEntity = httpResponse.getEntity();
				if(httpEntity != null)
				{
					InputStream instream = httpEntity.getContent();
					strResponse = convertStreamToString(instream);
					result.append(strResponse);
					instream.close();			
				}
				Header[] headers = httpResponse.getAllHeaders();
				for (Header header : headers) {
					if (!header.getName().equals("Transfer-Encoding"))
						servletResponse.setHeader(header.getName(), 
								header.getValue());
				}
			} catch (ConnectException e) {
				status = HttpStatus.SERVICE_UNAVAILABLE.value();
				e.printStackTrace();
				LOGGER.warn(e.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.warn(e.getMessage());
			}
			finally
			{
				if (httpResponse != null)
					httpResponse.close();
			}
		}
		catch(Exception e)
		{
			LOGGER.warn(e.getMessage());
		}
		finally
		{
			httpClient.close();
		}

		return HttpStatus.valueOf(status);
	}
	
	/*
	 * headObject : Method to get the headers of the object
	 * params : date
	 * params : signature
	 * params : contentType
	 * params : objectName
	 * params : servletResponse
	 * returns : StatusCode
	 * 
	 */
	public static HttpStatus headObject(String date, String signature,
			String contentType, String objectName, 
			HttpServletResponse servletResponse) throws IOException
	{
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		CloseableHttpResponse httpResponse = null;
		int status = 0;

		try
		{
			HttpHead httpHead = new HttpHead(Keystone.s3URL + "/"+ objectName);

			httpHead.setHeader("Date", date);
			httpHead.setHeader("Authorization", signature);

			if (contentType != null)
				httpHead.setHeader("Content-type", 
						"application/x-www-form-urlencoded; charset=utf-8");
			try {
				httpResponse = httpClient.execute(httpHead);
				status = httpResponse.getStatusLine().getStatusCode();
				LOGGER.info("In HeadObject status : "+ status);
				
				Header[] headers = httpResponse.getAllHeaders();
				for (Header header : headers) {
					servletResponse.setHeader(header.getName(),
							header.getValue());
				}
			
			} catch (ConnectException e) {
				status = HttpStatus.SERVICE_UNAVAILABLE.value();
				e.printStackTrace();
				LOGGER.warn(e.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.warn(e.getMessage());
			}
			finally
			{
				if (httpResponse != null)
					httpResponse.close();
			}
		}
		catch(Exception e)
		{
			LOGGER.warn(e.getMessage());
		}
		finally
		{
			httpClient.close();
		}

		return HttpStatus.valueOf(status);
	}
	
	/*
	 * convertStreamToString : Method to convert the instream to String
	 * params : inputStream
	 * returns : output string
	 */
	public static String convertStreamToString(InputStream inputStream)
	{
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(inputStream));
		StringBuilder sb = new StringBuilder();
		String line = null;
		try
		{
			while((line = reader.readLine()) != null)
				sb.append(line);
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				inputStream.close();
				reader.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}

		return sb.toString();
		
	}

	/*
	 * downloadObject : Method to download the object from s3
	 * params : date
	 * params : signature
	 * params : contentType
	 * params : objectName
	 * params : statusCode
	 * returns : servletResponse
	 */
	public static InputStream downloadObjectFromSwift(String date, String signature,
			String contentType, String objectName, MutableInt statusCode,
			HttpServletResponse servletResponse) throws Exception
	{
		int status = 0;
		boolean isMetaETagFound = false;
		String etag = "";
		String metaEtag = "";
		InputStream instream = null;
		int retryCount = 0;
		boolean isSuccess = false;

		while ((isSuccess== false) && retryCount <= 2)
		{
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			try
			{
				HttpGet httpGet = new HttpGet(Keystone.s3URL + objectName);

				httpGet.setHeader("Date", date);
				httpGet.setHeader("Authorization", signature);
				if (contentType != null)
					httpGet.setHeader("Content-type",
							"application/x-www-form-urlencoded; charset=utf-8");

				CloseableHttpResponse httpResponse = httpClient.execute(httpGet);
				status = httpResponse.getStatusLine().getStatusCode();
				LOGGER.info("listOrDownloadObject status : "+ status);

				statusCode.setValue(status);

				Header[] headers = httpResponse.getAllHeaders();
				for (Header header : headers) {
					if (header.getName().equals("ETag")) {
						etag = header.getValue();
						continue;
					}
					if (header.getName().equals("x-amz-meta-etag")) {
						isMetaETagFound = true;
						metaEtag = header.getValue();
					}
					if (!header.getName().equals("Transfer-Encoding"))
						servletResponse.setHeader(header.getName(), 
								header.getValue());
				}
				if (isMetaETagFound)
					servletResponse.setHeader("ETag", metaEtag);
				else
					servletResponse.setHeader("ETag", etag);

				HttpEntity httpEntity = httpResponse.getEntity();
				if(status == 200)
				{
					instream = null;
					if(httpEntity != null)
					{
						instream = httpEntity.getContent();
						isSuccess = true;
					}
				}
				else if(status == 500 || status == 503)
				{
					LOGGER.info("Sleeping for "+ Main.retryInterval);
					Thread.sleep(Main.retryInterval);
					LOGGER.info("Retrying download for object : "+ objectName);
					retryCount++;
					LOGGER.info("Retry count : "+ retryCount);
				}
				else
				{
					LOGGER.info("Unable to download object " +
						objectName + " : " + status);
					isSuccess = true;
				}
			}
			catch(Exception e)
			{
				LOGGER.warn(e.getMessage());
				status = HttpStatus.SERVICE_UNAVAILABLE.value();
				statusCode.setValue(status);
				break;
			}
			finally
			{
				httpClient = null;
			}
		}

		return instream;
	}
}
