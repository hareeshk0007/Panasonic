/**
*
* *****************************************************************************
*
* Copyright (C) 2016 Biarca. All rights reserved.
* 
* \file		FileUploadController.java
* \author	harishk@biarca.com
* 
* Defines Utils Class and its operations.
* 
******************************************************************************
*/
package com.biarca.app.web.api;

import java.net.URLEncoder;

public class Utils
{
	public enum StatusCode
	{
		SUCCESS,
		FAILED,
		INTERNAL_SERVER_ERROR,
		SERVICE_UNAVAILABLE,
		RESOURCE_CREATED,
		INVALID_CREDENTIALS,
		INVALID_PARAMETERS,
		PERMISSION_DENIED,
		OBJECT_NOT_FOUND,
		ALREADY_EXISTS,
		CONNECTION_ERROR,
		MAX_CONNECTIONS_REACHED,
		FILE_NOT_FOUND,		
		UNKNOWN,
		CONTENT_MISMATCH,
		BAD_REQUEST,
	};

	enum RequestType
	{
		eGet,
		ePost,
		ePut,
		eDelete
	};

	/*
	 * convertWhiteSpaceToValidString
	 * params : string
	 * returns : resultant string
	 * 
	 */
	public static String convertWhiteSpaceToValidString(String string)
	{
		String result = string;
		try
		{
			result = URLEncoder.encode(string, "UTF-8").replaceAll(
					"\\+", "%20");
		}
		catch(Exception e)
		{
		}
		return result;
	}
}