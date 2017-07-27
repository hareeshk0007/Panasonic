/**
*
* *****************************************************************************
*
* Copyright (C) 2016 Biarca. All rights reserved.
* 
* \file		FileUploadController.java
* \author	harishk@biarca.com
* 
* Defines FileUploadController Class and its operations.
* 
******************************************************************************
*/
package com.biarca.app.web.api;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.HandlerMapping;

import com.biarca.app.web.api.Utils.StatusCode;

@RestController
public class FileUploadController {

	private static final Logger LOGGER = LoggerFactory.getLogger(
			FileUploadController.class);

	public static ServerSocket serverSocket = null;
	public static Socket socket = null;

	@RequestMapping(value = "/", method = RequestMethod.GET,
			  produces = MediaType.APPLICATION_XML_VALUE)
	  public ResponseEntity<?> listAllBuckets(HttpServletRequest req,
			  HttpServletResponse response) throws IOException {

		LOGGER.info("In listAllBuckets : ");

		String date = req.getHeader("Date");
		String authorization = req.getHeader("Authorization");
		String contentType = req.getHeader("Content-type");

		StringBuilder bucketList = new StringBuilder();
		  
		HttpStatus statusCode = Swift3.listBuckets(date, authorization,
				contentType, bucketList, response);
		if (statusCode == HttpStatus.OK)
			return new ResponseEntity<StringBuilder>(bucketList, HttpStatus.OK);
		else
			return new ResponseEntity<String>(statusCode);
  }

	/*
	 * createBucket : Web Service Method to create the bucket
	 *
	 * params : bucket
	 * params : req
	 * params : response
	 * 
	 */
	@RequestMapping(value = "/{bucket}", method = RequestMethod.PUT)
	public ResponseEntity<?> createBucket(@PathVariable("bucket") String bucket,
		  HttpServletRequest req, HttpServletResponse response) throws IOException {

	  LOGGER.info("In createBucket : " + req.getRequestURI());

	  StringBuilder result = new StringBuilder();
	  String date = req.getHeader("Date");
	  String authorization = req.getHeader("Authorization");
	  String contentType = req.getHeader("Content-type");
	  String contentLength = req.getHeader("Content-length");
	  if(!contentLength.equals("0")) {		  
		  return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);
	  }
	  HttpStatus statusCode = Swift3.createBucket(date, authorization,
			  contentType, req.getRequestURI(), result, response);
	  if (statusCode == HttpStatus.OK) {
		  return new ResponseEntity<String>(HttpStatus.OK);
	  } else
		  return ResponseEntity.status(statusCode).body(result);
	}

	/*
	 * deleteBucket : Web Service Method to delete the bucket
	 *
	 * params : bucket
	 * params : req
	 * params : response
	 * 
	 */
	@RequestMapping(value = "/{bucket}", method = RequestMethod.DELETE)
	public ResponseEntity<?> deleteBucket(@PathVariable("bucket") String bucket,
		  HttpServletRequest req, HttpServletResponse response) throws IOException {

	  LOGGER.info("In deleteBucket :" + req.getRequestURI());

	  String date = req.getHeader("Date");
	  String authorization = req.getHeader("Authorization");

	  StringBuilder result = new StringBuilder();

	  HttpStatus statusCode = Swift3.deleteBucket(date, authorization,
			  req.getRequestURI(), result, response);
	  if (statusCode == HttpStatus.NO_CONTENT)
		  return new ResponseEntity<String>(HttpStatus.NO_CONTENT);
	  else
		  return new ResponseEntity<StringBuilder>(result, statusCode);
	}

	/*
	 * uploadObject : Web Service Method to upload the object
	 *
	 * params : bucket
	 * params : req
	 * params : response
	 * 
	 */
	@RequestMapping(value = "/{bucket}/**", method = RequestMethod.PUT)
	public ResponseEntity<?> uploadObject(@PathVariable("bucket") String bucket,
			HttpServletRequest req, HttpServletResponse response) throws Exception {

	  StatusCode statusCode = null;
	  HttpStatus httpStatus = null;
	  String requestURI = req.getRequestURI();
	  String pattern = (String) req.getAttribute(
			  HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);

	  String fileName = new AntPathMatcher().extractPathWithinPattern(pattern,
			  req.getServletPath());
	  String contentLength = req.getHeader("Content-length");
	  if (contentLength == null || contentLength.equals("")) {
		  LOGGER.info("In uploadObject, Unable to get Content Length for "+
				  fileName);
		  return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);
	  }

	  String authorization = req.getHeader("Authorization");
	  String contentType = req.getHeader("Content-type");

	  Double length = Swift.getFileLengthInGb(Long.parseLong(contentLength));
	  if (length > Swift.mFileMaxSizeInGB) {
		  LOGGER.info("In uploadObject => "+ fileName + " : Size : "+ 
				  contentLength);
		  StringBuilder etag = new StringBuilder();
		  StringBuilder lastModified = new StringBuilder();
		  StringBuilder transId = new StringBuilder();
		  StringBuilder swiftDate = new StringBuilder();
		  Keystone keystone = new Keystone();

		  try
		  {
			  Swift swift = new Swift();
			  statusCode = swift.getChunkConfigDetails();
			  if (statusCode == StatusCode.INVALID_PARAMETERS) {
				  LOGGER.info("Unable to get chunk size for file "+  fileName
						  + ", Chunk size limit is 0-2147483645");
				  return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);
			  }

			  httpStatus = keystone.getAdminAuthenticationToken();
			  if (httpStatus != HttpStatus.CREATED) {
					  return new ResponseEntity<String>(httpStatus);
			  }
			  String ec2_tmp[] = authorization.split(":");
			  String ec2[] = ec2_tmp[0].split(" ");
			  statusCode = keystone.getEC2Details(ec2[1]);
			  if (statusCode != StatusCode.SUCCESS) {
				  if (statusCode == StatusCode.INVALID_CREDENTIALS)
					  return new ResponseEntity<String>(HttpStatus.UNAUTHORIZED);
				  else if (statusCode == StatusCode.OBJECT_NOT_FOUND)
					  return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
				  else
					  return new ResponseEntity<String>(
							  HttpStatus.INTERNAL_SERVER_ERROR);
			  }
			  statusCode = keystone.getAuthenticationToken();
			  if (statusCode != StatusCode.SUCCESS) {
				  if (statusCode == StatusCode.INVALID_CREDENTIALS)
					  return new ResponseEntity<String>(HttpStatus.UNAUTHORIZED);
				  else if (statusCode == StatusCode.OBJECT_NOT_FOUND)
					  return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
				  else
					  return new ResponseEntity<String>(
							  HttpStatus.INTERNAL_SERVER_ERROR);
			  }
			  httpStatus = swift.prepareUploadFile(keystone, bucket,
					  fileName, contentLength, req.getInputStream(),
					  etag, lastModified, transId, swiftDate);
			  response.addHeader("Content-type", "text/html; charset=UTF-16");
			  response.addHeader("Date", swiftDate.toString());
			  response.addHeader("x-amz-id-2", transId.toString());
			  response.addHeader("Last-Modified", lastModified.toString());
			  response.addHeader("x-amz-request-id", transId.toString());
			  response.addHeader("X-Trans-Id", transId.toString());
			  if (httpStatus == HttpStatus.CREATED) {
				  response.setContentLength(0);
				  response.addHeader("ETag", "\""+ etag.toString() + "\"");
				  return new ResponseEntity<String>(HttpStatus.OK);
			  }
			  else
				  return new ResponseEntity<String>(httpStatus);
		  }
		  catch (Exception e)
		  {
			LOGGER.error(e.toString());	
		  }
		  return new ResponseEntity<String>(
				  HttpStatus.INTERNAL_SERVER_ERROR);
	  }
	  else
	  {
		  String date = req.getHeader("Date");
		  String content_MD5 = req.getHeader("Content-MD5");
		  StringBuilder objectName =
				  new StringBuilder(requestURI).deleteCharAt(0);
		  LOGGER.info("In uploadObject => "+ objectName + " : Size : "+
				  contentLength);
		  Swift3 swift3 = new Swift3();
		  httpStatus = swift3.uploadObject(date, authorization, bucket,
				  objectName.toString(), contentLength, contentType,
				  content_MD5, req.getInputStream(), response);

		  if (httpStatus == HttpStatus.OK) {
			  response.setContentType("text/html; charset=UTF-16");
			  return new ResponseEntity<String>(HttpStatus.OK);
		  }
		  else
			  return new ResponseEntity<String>(httpStatus);
	  }
	}

	/*
	 * listOrDownloadObject : Web Service Method to list or download the object
	 *
	 * params : bucket
	 * params : req
	 * params : response
	 * 
	 */
	@RequestMapping(value = "/{bucket}/**", method = {RequestMethod.GET})
	public ResponseEntity<?> listOrDownloadObject(
			@PathVariable("bucket") String bucket, HttpServletRequest req,
			HttpServletResponse response) throws Exception {

		ResponseEntity<?> respEntity = null;
		MutableInt statusCode = new MutableInt();
		String date = req.getHeader("Date");
		String authorization = req.getHeader("Authorization");
		String contentType = req.getHeader("Content-type");

		String requestURI = req.getRequestURI();
		String marker = req.getParameter("marker");
		InputStream inputStream = null;
		if (marker == null) {
			LOGGER.info("In listOrDownloadObject : " + req.getRequestURI());
			inputStream = Swift3.downloadObjectFromSwift(date, authorization,
				contentType, requestURI, statusCode, response);
		}
		else {
			String markerRequest =  requestURI + "?marker=" + marker;
			LOGGER.info("In listOrDownloadObject : " + markerRequest);
			inputStream = Swift3.downloadObjectFromSwift(date, authorization,
				contentType, markerRequest, statusCode, response);
		}
		HttpStatus httpStatus = HttpStatus.valueOf(statusCode.intValue());
		if (httpStatus == HttpStatus.OK) {
		  	InputStreamResource inputStreamResource = new 
		  			InputStreamResource(inputStream);
		  	inputStream = null;
		  	return new ResponseEntity<Object>(inputStreamResource, null,
		  			HttpStatus.OK);
		} else
			respEntity = new ResponseEntity<Object>(null, null, httpStatus);

	  	if (inputStream != null) {
	  		inputStream.close();
	  		inputStream = null;
	  	}

	  	return respEntity;
	}

	/*
	 * headObject : Web Service Method to get the headers of the object
	 *
	 * params : bucket
	 * params : req
	 * params : response
	 * 
	 */
	@RequestMapping(value = "/{bucket}/**", method = RequestMethod.HEAD)
	public ResponseEntity<?> headObject(@PathVariable("bucket") String bucket,
		  HttpServletRequest req, HttpServletResponse response) throws IOException {

		  LOGGER.info("In headObject : " + req.getRequestURI());

		  String date = req.getHeader("Date");
		  String authorization = req.getHeader("Authorization");
		  String contentType = req.getHeader("Content-type");

		  String requestURI = req.getRequestURI();
		  StringBuilder sb = new StringBuilder(requestURI).deleteCharAt(0);

		  HttpStatus status = Swift3.headObject(
				  date, authorization, contentType, sb.toString(), response);
			  return new ResponseEntity<String>(status);
  }

	/*
	 * headObject : Web Service Method to delete the object
	 *
	 * params : bucket
	 * params : req
	 * params : response
	 * 
	 */
	@RequestMapping(value = "/{bucket}/**", method = RequestMethod.DELETE)
	public ResponseEntity<?> deleteObject(@PathVariable("bucket") String bucket,
		  HttpServletRequest req, HttpServletResponse response) throws IOException {

		  LOGGER.info("In deleteObject : " + req.getRequestURI());

		  String date = req.getHeader("Date");
		  String authorization = req.getHeader("Authorization");
		  String contentType = req.getHeader("Content-type");

		  StringBuilder result = new StringBuilder();
		  String requestURI = req.getRequestURI();

		  HttpStatus status = Swift3.deleteObject(date, authorization,
				  contentType, requestURI, result, response);
		  if (status == HttpStatus.NO_CONTENT)
			  return new ResponseEntity<String>(HttpStatus.NO_CONTENT);
		  else
			  return new ResponseEntity<StringBuilder>(result, status);
	}
}
