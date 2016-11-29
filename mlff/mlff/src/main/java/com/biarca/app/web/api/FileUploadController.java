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
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.client.ClientProtocolException;
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
			  HttpServletResponse response) {

		LOGGER.info("In listAllBuckets : ");

		String date = req.getHeader("Date");
		String authorization = req.getHeader("Authorization");
		String contentType = req.getHeader("Content-type");

		StringBuilder bucketList = new StringBuilder();
		  
		StatusCode statusCode = Swift3.listBuckets(date, authorization,
				contentType, bucketList, response);
		if (statusCode == StatusCode.SUCCESS)
			return new ResponseEntity<StringBuilder>(bucketList, HttpStatus.OK);
		else if (statusCode == StatusCode.PERMISSION_DENIED) {
			return new ResponseEntity<String>(HttpStatus.FORBIDDEN);
		}
		else
			return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
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
		  HttpServletRequest req, HttpServletResponse response) {

	  LOGGER.info("In createBucket : " + req.getRequestURI());

	  StringBuilder result = new StringBuilder();
	  String date = req.getHeader("Date");
	  String authorization = req.getHeader("Authorization");
	  String contentType = req.getHeader("Content-type");
	  String contentLength = req.getHeader("Content-length");
	  if(!contentLength.equals("0")) {		  
		  return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);
	  }
	  StatusCode statusCode = Swift3.createBucket(date, authorization, 
			  contentType, req.getRequestURI(), result, response);
	  if (statusCode == StatusCode.SUCCESS) {
		  return new ResponseEntity<String>(HttpStatus.OK);
	  }
	  else if (statusCode == StatusCode.ALREADY_EXISTS) {
		  return ResponseEntity
				  .status(HttpStatus.CONFLICT)
				  .body(result.toString());
	  }
	  else if (statusCode == StatusCode.PERMISSION_DENIED)
		  return new ResponseEntity<String>(HttpStatus.FORBIDDEN);
	  else if (statusCode == StatusCode.BAD_REQUEST)
		  return new ResponseEntity<StringBuilder>(result, null,
				  HttpStatus.BAD_REQUEST);
	  else
		  return ResponseEntity
				  .status(HttpStatus.INTERNAL_SERVER_ERROR)
				  .body("Error Message");
  }

	/*
	 * listBucketObjects : Web Service Method to list the bucket objects
	 *
	 * params : bucket
	 * params : req
	 * params : response
	 * 
	 */
	@RequestMapping(value = "/{bucket}", method = RequestMethod.GET,
		  produces = "application/xml")
	public ResponseEntity<?> listBucketObjects(@PathVariable("bucket")
		String bucket, HttpServletRequest req, HttpServletResponse response) {

	  LOGGER.info("In listBucketObjects : " + req.getRequestURI());

	  String date = req.getHeader("Date");
	  String authorization = req.getHeader("Authorization");
	  String contentType = req.getHeader("Content-type");

	  StringBuilder objectList = new StringBuilder();
	  StatusCode statusCode = Swift3.listBucketObjects(
			  date, authorization, contentType, req.getRequestURI(),
			  response, objectList);
	  if (statusCode == StatusCode.SUCCESS)
		  return new ResponseEntity<StringBuilder>(objectList, HttpStatus.OK);
	  else if (statusCode == StatusCode.PERMISSION_DENIED)
		  return new ResponseEntity<String>(HttpStatus.FORBIDDEN);
	  else if (statusCode == StatusCode.OBJECT_NOT_FOUND)
		  return new ResponseEntity<StringBuilder>(objectList,
				  HttpStatus.NOT_FOUND);
	  else if (statusCode == StatusCode.BAD_REQUEST)
		  return new ResponseEntity<StringBuilder>(objectList,
				  HttpStatus.BAD_REQUEST);
	  else
		  return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
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
		  HttpServletRequest req, HttpServletResponse response) {

	  LOGGER.info("In deleteBucket :" + req.getRequestURI());

	  String date = req.getHeader("Date");
	  String authorization = req.getHeader("Authorization");

	  StringBuilder result = new StringBuilder();

	  StatusCode statusCode = Swift3.deleteBucket(date, authorization,
			  req.getRequestURI(), result, response);
	  if (statusCode == StatusCode.SUCCESS)
		  return new ResponseEntity<String>(HttpStatus.NO_CONTENT);
	  else if (statusCode == StatusCode.OBJECT_NOT_FOUND)
		  return new ResponseEntity<StringBuilder>(result,
				  HttpStatus.NOT_FOUND);
	  else if (statusCode == StatusCode.PERMISSION_DENIED)
		  return new ResponseEntity<String>(HttpStatus.FORBIDDEN);
	  else if (statusCode == StatusCode.ALREADY_EXISTS)
		  return new ResponseEntity<StringBuilder>(result, 
				  HttpStatus.CONFLICT);
	  else
		  return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);
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
			HttpServletRequest req, HttpServletResponse response) throws
			ClientProtocolException, IllegalStateException, IOException,
			ServletException {

	  StatusCode statusCode = null;
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
			  statusCode = swift.getChunkSize();
			  if (statusCode == StatusCode.INVALID_PARAMETERS) {
				  LOGGER.info("Unable to get chunk size for file "+  fileName
						  + ", Chunk size limit is 0-2147483645");
				  return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);
			  }

			  statusCode = keystone.getAdminAuthenticationToken();
			  if (statusCode != StatusCode.SUCCESS) {
				  if (statusCode == StatusCode.INVALID_CREDENTIALS)
					  return new ResponseEntity<String>(HttpStatus.UNAUTHORIZED);
				  else if (statusCode == StatusCode.OBJECT_NOT_FOUND)
					  return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
				  else
					  return new ResponseEntity<String>(
							  HttpStatus.INTERNAL_SERVER_ERROR);
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
			  if (statusCode != StatusCode.SUCCESS)
				  return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
			  statusCode = swift.prepareUploadFile(keystone, bucket,
					  fileName, contentLength, req.getInputStream(),
					  etag, lastModified, transId, swiftDate);
			  response.addHeader("Content-type", "text/html; charset=UTF-16");
			  response.addHeader("Date", swiftDate.toString());
			  response.addHeader("x-amz-id-2", transId.toString());
			  response.addHeader("Last-Modified", lastModified.toString());
			  response.addHeader("x-amz-request-id", transId.toString());
			  response.addHeader("X-Trans-Id", transId.toString());
			  if (statusCode == StatusCode.SUCCESS) {
				  response.setContentLength(0);
				  response.addHeader("ETag", "\""+ etag.toString() + "\"");
				  return new ResponseEntity<String>(HttpStatus.OK);
			  }
			  else if (statusCode == StatusCode.PERMISSION_DENIED)
				  return new ResponseEntity<String>(HttpStatus.FORBIDDEN);
			  else if (statusCode == StatusCode.OBJECT_NOT_FOUND)
				  return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
			  else if (statusCode == StatusCode.INVALID_PARAMETERS)
				  return new ResponseEntity<StringBuilder>(
					  HttpStatus.BAD_REQUEST);
			  else if (statusCode == StatusCode.CONTENT_MISMATCH)
				  return new ResponseEntity<String>(
						  HttpStatus.PRECONDITION_FAILED);
			  else
				  return new ResponseEntity<String>(
						  HttpStatus.INTERNAL_SERVER_ERROR);
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
		  try
		  {
			  StringBuilder objectName =
					  	new StringBuilder(requestURI).deleteCharAt(0);
			  LOGGER.info("In uploadObject => "+ objectName + " : Size : "+
					  contentLength);
			  Swift3 swift3 = new Swift3();
			  statusCode = swift3.uploadObject(date, authorization, bucket,
					  objectName.toString(), contentLength, contentType,
					  content_MD5, req.getInputStream(), response);
		  }
		  catch (Exception e)
		  {
			LOGGER.error(e.toString());
		  }
		  if (statusCode == StatusCode.SUCCESS) {
			  response.setContentType("text/html; charset=UTF-16");
			  return new ResponseEntity<String>(HttpStatus.OK);
		  }
		  else if (statusCode == StatusCode.PERMISSION_DENIED)
			  return new ResponseEntity<String>(HttpStatus.FORBIDDEN);
		  else if (statusCode == StatusCode.OBJECT_NOT_FOUND)
			  return new ResponseEntity<String>("Bucket Not Found", 
					  HttpStatus.NOT_FOUND);
		  else if (statusCode == StatusCode.INVALID_PARAMETERS)
			  return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);
		  else if (statusCode == StatusCode.CONTENT_MISMATCH)
			  return new ResponseEntity<String>(HttpStatus.PRECONDITION_FAILED);
		  else
			  return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);
	  }
	}

	/*
	 * downloadObject : Web Service Method to download the object
	 *
	 * params : bucket
	 * params : req
	 * params : response
	 * 
	 */
	@RequestMapping(value = "/{bucket}/**", method = {RequestMethod.GET})
	public ResponseEntity<?> downloadObject(
			@PathVariable("bucket") String bucket, HttpServletRequest req,
			HttpServletResponse response) throws Exception {

	  	LOGGER.info("In downloadObject : " + req.getRequestURI());

	  	ResponseEntity<?> respEntity = null;
	  	StringBuilder statusCode = new StringBuilder();

		String date = req.getHeader("Date");
		String authorization = req.getHeader("Authorization");
		String contentType = req.getHeader("Content-type");

		String requestURI = req.getRequestURI();

	  	InputStream inputStream = Swift3.downloadObjectFromSwift(date,
	  			authorization, contentType, requestURI, statusCode, response);
	  	if (statusCode.toString().equals("SUCCESS")) {
		  	InputStreamResource inputStreamResource = new 
		  			InputStreamResource(inputStream);
		  	inputStream = null;
		  	return new ResponseEntity<Object>(inputStreamResource, null,
		  			HttpStatus.OK);
	  	}
	  	else if (statusCode.toString().equals("INVALID_CREDENTIALS"))
	  	  	respEntity = new ResponseEntity<Object>(null, null,
	  	  			HttpStatus.FORBIDDEN);
	  	else if (statusCode.toString().equals("PERMISSION_DENIED"))
	  	  	respEntity = new ResponseEntity<Object>(null, null, 
	  	  			HttpStatus.FORBIDDEN);
	  	else if (statusCode.toString().equals("NOT_FOUND")) {
	  		InputStreamResource inputStreamResource = new
	  				InputStreamResource(inputStream);
	  		inputStream = null;
	  	  	respEntity = new ResponseEntity<Object>(inputStreamResource, null,
	  	  			HttpStatus.NOT_FOUND);
	  	}
	  	else if (statusCode.toString().equals("BAD_REQUEST")) {
	  		InputStreamResource inputStreamResource = new 
	  				InputStreamResource(inputStream);
	  	  	respEntity = new ResponseEntity<Object>(inputStreamResource, null, 
	  	  			HttpStatus.BAD_REQUEST);
	  	}

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
		  HttpServletRequest req, HttpServletResponse response) {

		  LOGGER.info("In headObject : " + req.getRequestURI());

		  String date = req.getHeader("Date");
		  String authorization = req.getHeader("Authorization");
		  String contentType = req.getHeader("Content-type");

		  String requestURI = req.getRequestURI();
		  StringBuilder sb = new StringBuilder(requestURI).deleteCharAt(0);

		  StatusCode statusCode = Swift3.headObject(
				  date, authorization, contentType, sb.toString(), response);
		  if (statusCode == StatusCode.SUCCESS)
			  return new ResponseEntity<String>(HttpStatus.OK);
		  else if (statusCode == StatusCode.PERMISSION_DENIED)
			  return new ResponseEntity<String>(HttpStatus.FORBIDDEN);
		  else if (statusCode == StatusCode.OBJECT_NOT_FOUND)
			  return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
		  else
			  return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
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
		  HttpServletRequest req, HttpServletResponse response) {

		  LOGGER.info("In deleteObject : " + req.getRequestURI());

		  String date = req.getHeader("Date");
		  String authorization = req.getHeader("Authorization");
		  String contentType = req.getHeader("Content-type");

		  StringBuilder objectList = new StringBuilder();
		  String requestURI = req.getRequestURI();

		  StatusCode statusCode = Swift3.deleteObject(date, authorization,
				  contentType, requestURI, objectList, response);
		  if (statusCode == StatusCode.SUCCESS)
			  return new ResponseEntity<String>(HttpStatus.NO_CONTENT);
		  else if (statusCode == StatusCode.PERMISSION_DENIED)
			  return new ResponseEntity<String>(HttpStatus.FORBIDDEN);
		  else if (statusCode == StatusCode.OBJECT_NOT_FOUND)
			  return new ResponseEntity<StringBuilder>(objectList, 
					  HttpStatus.NOT_FOUND);
		  else
			  return new ResponseEntity<StringBuilder>(objectList, 
					  HttpStatus.NOT_FOUND);
	}
}
