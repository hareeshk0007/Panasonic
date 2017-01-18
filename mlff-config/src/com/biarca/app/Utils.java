/**
*
* *****************************************************************************
*
* Copyright (C) 2016 Biarca. All rights reserved.
* 
* \file		Utils.java
* \author	harishk@biarca.com
* 
* Defines Utils Class and its operations.
* 
******************************************************************************
*/
package com.biarca.app;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

public class Utils
{
	public enum StatusCode
	{
		SUCCESS,
		FAILED,
		FILE_NOT_FOUND,
	};

	/*
	 * aesEncrypt : Method to encrypt the given file using the AES algorithm
	 *
	 * params : None
	 * returns : None
	 */
	static String aesEncrypt(String password) throws Exception
	{
		try {
			String key = "Bar12345Bar12345";
			String initVector = "RandomInitVector";
			IvParameterSpec iv = new IvParameterSpec(
					initVector.getBytes("UTF-8"));
			SecretKeySpec skeySpec = new SecretKeySpec(
					key.getBytes("UTF-8"), "AES");

			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
			cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
			byte[] encrypted = cipher.doFinal(password.getBytes());

			return Base64.encodeBase64String(encrypted);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return null;
	}
}
