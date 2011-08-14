package com.spotify.hadoop.openpgp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.security.SecureRandom;

import org.apache.hadoop.conf.Configuration;

import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.bcpg.CompressionAlgorithmTags;
import org.bouncycastle.bcpg.SymmetricKeyAlgorithmTags;

public class OpenPgpCompressor extends StreamCompressor {
	public static final Map<String, Integer> COMPRESSION_ALGORITHMS = EnumUtils.getStaticFinalFieldMapping(CompressionAlgorithmTags.class);
	public static final Map<String, Integer> ENCRYPTION_ALGORITHMS = EnumUtils.getStaticFinalFieldMapping(SymmetricKeyAlgorithmTags.class);

	public OpenPgpCompressor(Configuration conf) throws IOException {
		super(conf);
	}

	protected OutputStream createOutputStream(OutputStream out) throws IOException {
		return createOutputStream(
			out,
			getKey(),
			getEncryptionAlgorithm(),
			wantsIntegrity(),
			getCompressionAlgorithm(),
			getFormat(),
			"",
			PGPLiteralDataGenerator.NOW,
			getBufferSize());
	}

	// Default protection, for unit tests.
	static OutputStream createOutputStream(OutputStream out, Object key, int encryption, boolean signed, int compression, int format, String name, Date mtime, int bufferSize) throws IOException {
		try {
			if (encryption != PGPEncryptedDataGenerator.NULL || signed) {
				PGPEncryptedDataGenerator edg = new PGPEncryptedDataGenerator(
					encryption,
					signed,
					new SecureRandom(),
					"BC");

				if (key instanceof PGPPublicKey)
					edg.addMethod((PGPPublicKey) key);
				else if (key instanceof String)
					edg.addMethod(((String) key).toCharArray());
				else
					throw new IOException("Encryption was requested but not key was specified");

				out = edg.open(out, bufferSize);
			}

			if (compression != PGPCompressedDataGenerator.UNCOMPRESSED) {
				PGPCompressedDataGenerator cdg = new PGPCompressedDataGenerator(
					compression);

				out = cdg.open(out);
			}

			PGPLiteralDataGenerator ldg = new PGPLiteralDataGenerator();

			out = ldg.open(
				out,
				(char) format,
				name,
				mtime,
				new byte[bufferSize]);

			return out;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	private File getPubringFile() {
		String path = getConf().get("spotify.hadoop.openpgp.pubring.path");

		if (path != null)
			return new File(path);

		return GnuPgUtils.getDefaultPubringFile();
	}

	private Object getKey() {
		PGPPublicKey pubKey = getPublicKey();

		if (pubKey != null) return pubKey;

		return getEncryptionPassPhrase();
	}

	private String getEncryptionPassPhrase() {
		return getConf().get("spotify.hadoop.openpgp.encrypt.passPhrase");
	}

	private String getPublicKeyId() {
		return getConf().get("spotify.hadoop.openpgp.encrypt.keyId");
	}

	private PGPPublicKey getPublicKey() {
		return getPublicKey(getPublicKeyId());
	}

	private PGPPublicKey getPublicKey(String id) {
		if (id == null) return null;

		try {
			PGPPublicKeyRingCollection col = GnuPgUtils.createPublicKeyRingCollection(getPubringFile());

			return GnuPgUtils.getPublicKey(col, id);
		} catch (Exception ex) {
			throw new KeyNotFoundException(ex);
		}
	}

	private int getFormat() {
		String format = getConf().get("spotify.hadoop.openpgp.format", "binary");

		if (format.equals("binary")) return PGPLiteralDataGenerator.BINARY;
		else if (format.equals("text")) return PGPLiteralDataGenerator.TEXT;
		else if (format.equals("utf-8")) return PGPLiteralDataGenerator.UTF8;

		throw new RuntimeException("unknown format");
	}

	private int getCompressionAlgorithm() {
		String algo = getConf().get("spotify.hadoop.openpgp.compression", "uncompressed");

		return COMPRESSION_ALGORITHMS.get(algo.toUpperCase());
	}

	private int getEncryptionAlgorithm() {
		String algo = getConf().get("spotify.hadoop.openpgp.encryption");

		if (algo == null) {
			if (getPublicKey() != null || getEncryptionPassPhrase() != null)
				algo = "cast5";
			else
				algo = "null";
		}

		return ENCRYPTION_ALGORITHMS.get(algo.toUpperCase());
	}

	private boolean wantsIntegrity() {
		String b = getConf().get("spotify.hadoop.openpgp.integrity.sign");

		if (b == null) {
			if (getPublicKey() != null || getEncryptionPassPhrase() != null)
				b = "true";
			else
				b = "false";
		}

		return Boolean.valueOf(b);
	}

	private int getBufferSize() {
		return getConf().getInt("spotify.hadoop.openpgp.buffersize", 1 << 14);
	}
}
