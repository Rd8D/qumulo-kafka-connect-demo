// Kafka is licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
// Please note that any Qumulo Core REST API used in this demo is copyrighted to Qumulo, Inc.
// You can read more about Qumulo's RESTful API here: https://qumulo.github.io/
// CAUTION: THIS DEMO CODE IS NOT PRODUCTION READY

package org.demo.kafka.utils.qf2.crypto;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Base64;

public class RSAKeyPairGenerator {

	private PrivateKey id_rsa;
	private PublicKey id_rsa_pub;

	public RSAKeyPairGenerator() throws NoSuchAlgorithmException {
		KeyPairGenerator kgen = KeyPairGenerator.getInstance("RSA");
		kgen.initialize(1024);
		KeyPair kpair = kgen.generateKeyPair();
		this.id_rsa = kpair.getPrivate();
		this.id_rsa_pub = kpair.getPublic();
	}

	public void writeToFile(String path, byte[] key) throws IOException {
		File f = new File(path);
		f.getParentFile().mkdirs();

		FileOutputStream fos = new FileOutputStream(f);
		fos.write(key);
		fos.flush();
		fos.close();
	}

	public PrivateKey getPrivateKey() {
		return id_rsa;
	}

	public PublicKey getPublicKey() {
		return id_rsa_pub;
	}

	public static void main(String[] args) throws NoSuchAlgorithmException, IOException {

		RSAKeyPairGenerator kgen = new RSAKeyPairGenerator();
		kgen.writeToFile("conf/.id_rsa.pub",
				Base64.getEncoder().encodeToString(kgen.getPublicKey().getEncoded()).getBytes());
		System.out.println(".id_rsa.pub:\n" + Base64.getEncoder().encodeToString(kgen.getPublicKey().getEncoded()));
		kgen.writeToFile("conf/.id_rsa",
				Base64.getEncoder().encodeToString(kgen.getPrivateKey().getEncoded()).getBytes());
		System.out.println(".id_rsa:\n" + Base64.getEncoder().encodeToString(kgen.getPrivateKey().getEncoded()));

	}
}
