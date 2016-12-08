/**
*
* *****************************************************************************
*
* Copyright (C) 2016 Biarca. All rights reserved.
* 
* \file		Keystone.java
* \author	harishk@biarca.com
* 
* Defines Keystone Class and its operations.
* 
******************************************************************************
*/

package com.biarca.app.web.api;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Properties;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.biarca.app.Main;
import com.biarca.app.web.api.Utils.StatusCode;

public class Keystone
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Keystone.class);
	
	static StringEntity entity = null;

	public String mAdminAuthToken = "";
	public static String mAdminUserName = "";
	public static String mAdminUserId = "";
	public static String mAdminPassword = "";
	public static String mAdminDomainId = "";
	public static String mAdminDomainName = "";
	public static String mAdminProjectId = "";
	public static String mAdminTenantName = "";
	public static String mAuthURL = "";
	public static String s3URL = "";

	public String mSwiftProjectId = "";
	public String mSwiftUserId = "";
	public String mSwiftPassword = "";
	public String mSwiftTenantName = "";
	public String mSwiftURL = "";
	public String mSwiftToken = "";

	private static String response = "";
	private final static String urlSuffix = "/auth/tokens";

	/*
	 * getAdminAuthenticationToken
	 * params : None
	 * returns : Status code
	 * 
	 */
	public StatusCode getAdminAuthenticationToken()
	{
		StatusCode status = StatusCode.UNKNOWN;
		int responseCode = 0;
		try {
			ArrayList<NameValuePair> headers = new ArrayList<NameValuePair>();
			String strURL = mAuthURL + urlSuffix ;
			
			HttpPost postRequest = new HttpPost(strURL);
			if(mAuthURL.contains("v2.0"))
				entity = new StringEntity("{\"auth\":{\"" +
					"passwordCredentials\":{\"username\":\""+
					mAdminUserName+"\",\"password\":\"" +
					mAdminPassword+"\"},\"tenantName\":\"" +
					mAdminTenantName +"\"}}");			
			else if(mAuthURL.contains("v3")) {
				if (!mAdminUserId.equals("") && !mAdminProjectId.equals("")) {
					entity = new StringEntity("{\"auth\": {" + "\"identity\": {"
							+ "\"methods\": [" + "\"password\"],\"password\": "
							+ "{\"user\": {\"id\": \""+ mAdminUserId
							+ "\",\"password\": \"" + mAdminPassword + "\"}}}," 
							+ "\"scope\": {\"project\": {\"id\": \"" 
							+ mAdminProjectId + "\"}}}}");
				} else if (!mAdminUserName.equals("") && 
						!mAdminDomainName.equals("")) {
					entity = new StringEntity("{\"auth\":{\"identity\":"
							 + "{\"methods\":" + "[\"password\"],\"password\": "
							 + "{\"user\":{\"name\":\"" + mAdminUserName 
							 + "\",\"domain\":" + "{ \"name\":\"" 
							 + mAdminDomainName + "\"},\"password\":\"" + 
							 mAdminPassword + "\"}}}," + "\"scope\":{\"project\""
							 + ":{\"name\":\"" + mAdminUserName + "\",\"domain\":"
							 + "{ \"name\":\"" + mAdminDomainName + "\"}}}}}");
				} else if (!mAdminUserName.equals("") && 
						!mAdminDomainId.equals("")) {
					entity = new StringEntity("{\"auth\":{\"identity\":"
							 + "{\"methods\":" + "[\"password\"],\"password\": "
							 + "{\"user\":{\"name\":\"" + mAdminUserName 
							 + "\",\"domain\":" + "{ \"id\":\"" + mAdminDomainId 
							 + "\"},\"password\":\"" + mAdminPassword + "\"}}}," 
							 + "\"scope\":{\"project\":{\"name\":\"" + mAdminUserName 
							 + "\",\"domain\":" + "{ \"id\":\"" + mAdminDomainId 
							 + "\"}}}}}");
				}
			}

			headers.add(new BasicNameValuePair("Content-Type","application/json"));

			for(NameValuePair h : headers)
				postRequest.addHeader(h.getName(), h.getValue());
			postRequest.setEntity(entity);

			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			CloseableHttpResponse httpResponse = null;

			try {
				httpResponse = httpClient.execute(postRequest);
				responseCode = httpResponse.getStatusLine().getStatusCode();
				LOGGER.info("Admin Authentication status "+ responseCode);
				if(responseCode == 201)
				{
					status = StatusCode.SUCCESS;
					Header[] headerss = httpResponse.getAllHeaders();
					for (Header header : headerss)
						if(header.getName().equals("X-Subject-Token"))
							mAdminAuthToken = header.getValue();
					LOGGER.info("Retrieved Admin Token : "+ mAdminAuthToken);
				}
				else if (responseCode == 401)
				{
					status = StatusCode.INVALID_CREDENTIALS;
					LOGGER.info("Admin authentication failed with : "+ responseCode);
				}
				else
				{
					LOGGER.info("Admin authentication failed with : "+ status);
				}
			}
			catch(Exception e) {
				LOGGER.error(e.toString());
			}
			finally {
				headers = null;
				postRequest = null;
				httpResponse.close();
				httpClient.close();
			}			
		}
		catch(Exception ex) {
			LOGGER.error(ex.toString());
		}

		return status;
	}
	
	/*
	 * getAuthenticationToken
	 * params : None
	 * returns : Status code
	 * 
	 */
	public StatusCode getAuthenticationToken()
	{
		StatusCode err = StatusCode.UNKNOWN;
		int responseCode = 0;

		try {
			ArrayList<NameValuePair> headers =
					new ArrayList<NameValuePair>();
			String strURL = mAuthURL + urlSuffix ;
			HttpPost postRequest = new HttpPost(strURL);

			if(mAuthURL.contains("v2.0"))
				entity = new StringEntity("{\"auth\":{\"" +
					"passwordCredentials\":{\"username\":\""+
					mSwiftUserId+"\",\"password\":\"" +
					mSwiftPassword+"\"},\"tenantName\":\"" +
					mSwiftTenantName +"\"}}");
			else if(mAuthURL.contains("v3")) {
				entity = new StringEntity("{\"auth\": {" + "\"identity\": {"
						+ "\"methods\": [" + "\"password\"],\"password\": "
						+ "{\"user\": {\"id\": \""+ mSwiftUserId + "\","
						+ "\"password\": \"" + mSwiftPassword + "\"}}},"+ 
						"\"scope\": {\"project\": {\"id\": \"" 
						+ mSwiftProjectId + "\"}}}}");
			}

			headers.add(new BasicNameValuePair("Content-Type","application/json"));
			for(NameValuePair h : headers)
				postRequest.addHeader(h.getName(), h.getValue());
			postRequest.setEntity(entity);

			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			CloseableHttpResponse httpResponse = null;

			try {				
				httpResponse = httpClient.execute(postRequest);
				responseCode = httpResponse.getStatusLine().getStatusCode();
				LOGGER.info("User Authentication status "+ responseCode);
				if (responseCode == 201) {
					err = StatusCode.SUCCESS;
					Header[] headerss = httpResponse.getAllHeaders();
					for (Header header : headerss)
						if(header.getName().equals("X-Subject-Token"))
							mSwiftToken = header.getValue();
					LOGGER.info("Retrieved Swift Token : "+ mSwiftToken);
					HttpEntity httpEntity = httpResponse.getEntity();
					if(httpEntity != null) {
						InputStream instream = httpEntity.getContent();
						response = convertStreamToString(instream);
						parseJSONNode(response);
						instream.close();
					}
				}
				else if (responseCode == 401) {
					err = StatusCode.INVALID_CREDENTIALS;
					LOGGER.info("User authentication failed with : "+ 
							responseCode);
				}
			}
			catch(Exception e) {
				LOGGER.warn(e.toString());
			}
			finally {
				httpResponse.close();
				httpClient.close();
			}
		}
		catch(Exception ex) {
			LOGGER.error(ex.toString());
		}

		return err;
	}
	
	/*
	 * getEC2Details
	 * params : ec2
	 * returns : Status code
	 * 
	 */
	public StatusCode getEC2Details(String ec2)
	{
		StatusCode status = StatusCode.UNKNOWN;
		int responseCode; 
		CloseableHttpClient httpClient = HttpClients.createDefault();
		CloseableHttpResponse httpResponse = null;
		InputStream instream = null;
		try {
			ArrayList<NameValuePair> headers =
					new ArrayList<NameValuePair>();
			String strURL = mAuthURL + "/credentials";
			HttpGet getRequest = new HttpGet(strURL);
			
			headers.add(new BasicNameValuePair("X-Auth-Token", mAdminAuthToken));
			for(NameValuePair h : headers)
				getRequest.addHeader(h.getName(), h.getValue());

			try {
				httpResponse = httpClient.execute(getRequest);
				responseCode = httpResponse.getStatusLine().getStatusCode();
				if(responseCode == 200)
				{
					status = StatusCode.SUCCESS;				
					HttpEntity httpEntity = httpResponse.getEntity();
					if (httpEntity != null) {
						instream = httpEntity.getContent();
						response = convertStreamToString(instream);
						status = parseJSONForEC2(response, ec2);
					}
				}
				else if(responseCode == 401) {
					LOGGER.error("Invalid crdentials");
					status = StatusCode.INVALID_CREDENTIALS;
				}
				else if(responseCode == 403) {
					LOGGER.error("Permission denied");
					status = StatusCode.PERMISSION_DENIED;
				}
				else {
					status = StatusCode.FAILED;
				}
			}
			catch(Exception e) {
				LOGGER.error(e.getMessage());
			}
			finally {				
				httpResponse.close();
				if (instream != null)
					instream.close();				
			}	
			
			if(status != StatusCode.SUCCESS)
				LOGGER.info("Unable to get EC2 details, Error Code : " + status);
		}
		catch(Exception ex) {
			LOGGER.info(ex.toString());
		}
		finally
		{
			try {
				httpClient.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return status;
	}

	/*
	 * convertStreamToString
	 * params : inputStream
	 * returns : output string
	 * 
	 */
	public static String convertStreamToString(InputStream inputStream)
			throws JSONException
	{
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(inputStream));
		StringBuilder sb = new StringBuilder();
		String line = null;
		try {
			while((line = reader.readLine()) != null)
				sb.append(line + "\n");
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			try {
				inputStream.close();
				reader.close();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sb.toString();
	}

	/*
	 * parseJSONNode
	 * params : response stream
	 * returns : StatusCode
	 * 
	 */
	public StatusCode parseJSONNode(String response) throws JSONException
	{
		int i = 0;
		int j = 0;
		int len;
		int len2;
		JSONObject urlObject = null;
		StatusCode err = StatusCode.UNKNOWN;
		JSONObject url = null;

		JSONObject access_object = new JSONObject(response);
		String token_str = access_object.getString("token");

		JSONObject token_object = new JSONObject(token_str);
		JSONArray catalog_array = token_object.getJSONArray("catalog");

		len = catalog_array.length();
		for(i = 0; i < len; i++) {
			urlObject = catalog_array.getJSONObject(i);
			String type = urlObject.getString("type");
			if (type.equals("object-store")) {
				JSONArray endpoints_array = urlObject.getJSONArray("endpoints");
				len2 = endpoints_array.length();
				for(j = 0; j < len2; ++j) {
					url = endpoints_array.getJSONObject(j);
					String interfaces = url.getString("interface");
					if (interfaces.equals("public")) {
						mSwiftURL = url.getString("url");
						LOGGER.info("Retrieved Swift URL : " + mSwiftURL);
						err = StatusCode.SUCCESS;
						break;
					}
				}
				break;
			}
		}
		if(mSwiftURL.equals(""))
			LOGGER.error("Unable to retrieved Swift URL");
		return err;
	}

	/*
	 * parseJSONForEC2
	 * params : response stream
	 * returns : StatusCode
	 * 
	 */
	public StatusCode parseJSONForEC2(String response, String ec2)
			throws Exception
	{
		int i = 0;
		int len;
		JSONObject urlObject = null;
		StatusCode err = StatusCode.UNKNOWN;

		JSONObject credentials_object = new JSONObject(response);
		JSONArray catalog_array = credentials_object.getJSONArray("credentials");

		len = catalog_array.length();
		for(i = 0; i < len; i++) {
			urlObject = catalog_array.getJSONObject(i);

			String blob = urlObject.getString("blob");
			JSONObject access_object = new JSONObject(blob);
			String access_token = access_object.getString("access");
			if (access_token.equals(ec2)) {
				String user_token = urlObject.getString("user_id");
				String project = urlObject.getString("project_id");

				mSwiftUserId = user_token;
				mSwiftProjectId = project;
				try {
					String encPassword = getPassword(mSwiftUserId);
					if (encPassword == null || encPassword.equals(""))
						return StatusCode.OBJECT_NOT_FOUND;
					mSwiftPassword = decryptPassword(encPassword);
					if (mSwiftPassword.equals(""))
						err = StatusCode.OBJECT_NOT_FOUND;
					else
						err = StatusCode.SUCCESS;
				} catch (IOException e) {
					e.printStackTrace();
				}
				break;
			}
		}
		if(mSwiftUserId.equals("") || mSwiftProjectId.equals("")) {
			LOGGER.info("Unable to get the userID or project ID "
					+ "from the response");
			err = StatusCode.OBJECT_NOT_FOUND;
		}

		return err;
	}
	
	/*
	 * getPassword
	 * params : response stream
	 * returns : output string
	 */
	String getPassword(String userId) throws IOException
	{
		Properties prop = new Properties();
		String password = "";
		InputStream is = null;
		try {
			is = new FileInputStream(Main.configFile);
			prop.load(is);
			password = prop.getProperty(mSwiftUserId);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		finally {
			is.close();
			prop.clear();
		}
		return password;
	}

	/*
	 * decryptPassword : Method to decrypt the password
	 *
	 * params : encrypt
	 * returns : String
	 */
	public static String decryptPassword(String encrypt) throws Exception
	{
		String key = "Bar12345Bar12345";
		String initVector = "RandomInitVector";
		try {
			IvParameterSpec iv = new IvParameterSpec(
					initVector.getBytes("UTF-8"));
			SecretKeySpec skeySpec = new SecretKeySpec(
					key.getBytes("UTF-8"), "AES");

			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
			cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);

			byte[] original = cipher.doFinal(Base64.decodeBase64(encrypt));

			return new String(original);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return null;
	}
}
