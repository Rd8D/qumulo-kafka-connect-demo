// Kafka is licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
// Please note that any Qumulo's RESTful API used in this demo is copyrighted to Qumulo, Inc.
// You can read more about Qumulo's API here: https://qumulo.github.io/
// CAUTION: THIS DEMO CODE IS NOT PRODUCTION READY

package org.demo.kafka.utils.qf2.crypto;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RSAUtils {
	
	private static final Logger log = LoggerFactory.getLogger(RSAUtils.class);

	private static RSAUtils instance = null;

	private static HashMap<String, String> props;

	public static final String KEY_CONFIG = "rsa.key";

	private static Key key = null;

	private static byte[] digest = null;

	protected RSAUtils() throws Exception {
		if (props == null)
			throw new Exception(
					"Cannot perform RestApiUtil() before completing a call to getInstance(Map<String, String> props)");
		digest = Files.readAllBytes(Paths.get(props.get(KEY_CONFIG)));
	}

	public static RSAUtils getInstance() throws Exception {

		if (instance == null)
			instance = new RSAUtils();

		return instance;
	}

	public static synchronized RSAUtils getInstance(Map<String, String> props) throws Exception {

		RSAUtils.props = new HashMap<String, String>(props);
		return getInstance();
	}

	public PublicKey getPublicKey() throws NoSuchAlgorithmException, InvalidKeySpecException {

		if (key == null) {
			X509EncodedKeySpec ks = new X509EncodedKeySpec(Base64.getDecoder().decode(digest));
			KeyFactory kf = KeyFactory.getInstance("RSA");
			key = (PublicKey) kf.generatePublic(ks);
		}
		return (PublicKey) key;
	}

	public PrivateKey getPrivateKey() throws NoSuchAlgorithmException, InvalidKeySpecException {

		if (key == null) {
			PKCS8EncodedKeySpec ks = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(digest));
			KeyFactory kf = KeyFactory.getInstance("RSA");
			key = (PrivateKey) kf.generatePrivate(ks);
		}
		return (PrivateKey) key;
	}

	public String encrypt(byte[] data) throws BadPaddingException, IllegalBlockSizeException, InvalidKeyException,
			NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeySpecException {

		KeyGenerator kgen = KeyGenerator.getInstance("DES");
		kgen.init(56);
		SecretKey sk = kgen.generateKey();
		Cipher cipher = Cipher.getInstance("DES");
		cipher.init(Cipher.ENCRYPT_MODE, sk);
		byte[] encData = cipher.doFinal(data);
		cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
		cipher.init(Cipher.ENCRYPT_MODE, getPublicKey());		
		byte[] encKey = cipher.doFinal(sk.getEncoded());
		log.trace(DatatypeConverter.printHexBinary(MessageDigest.getInstance("MD5").digest(encKey)));
		log.trace(DatatypeConverter.printHexBinary(MessageDigest.getInstance("MD5").digest(encData)));
		String strKey = Base64.getEncoder().encodeToString(encKey);
		String strData = Base64.getEncoder().encodeToString(encData);

		return strKey + "\t" + strData;
	}

	public String decrypt(String data) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException,
			BadPaddingException, IllegalBlockSizeException, InvalidKeySpecException {

		String[] digest = data.split("\t");
		byte[] encKey = Base64.getDecoder().decode(digest[0]);
		log.trace(DatatypeConverter.printHexBinary(MessageDigest.getInstance("MD5").digest(encKey)));
		byte[] encData = Base64.getDecoder().decode(digest[1]);
		log.trace(DatatypeConverter.printHexBinary(MessageDigest.getInstance("MD5").digest(encData)));
		Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
		cipher.init(Cipher.DECRYPT_MODE, getPrivateKey());
		byte[] decKey = cipher.doFinal(encKey);
		SecretKey sk = new SecretKeySpec(decKey, 0, decKey.length, "DES");
		Cipher cipher2 = Cipher.getInstance("DES");
		cipher2.init(Cipher.DECRYPT_MODE, sk);

		return new String(cipher2.doFinal(encData));
	}

	public static void clear() {
		key = null;
		props = null;
		digest = null;
		instance = null;
	}

}
