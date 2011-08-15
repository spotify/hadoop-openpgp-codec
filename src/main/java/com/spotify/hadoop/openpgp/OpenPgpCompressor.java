package com.spotify.hadoop.openpgp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

/**
 * A Hadoop compressor that runs an OpenPGP encryption.
 *
 * The word "compressor" is wrong. Think of it more like a transformer.
 *
 * Configuration entries used:
 *
 * * spotify.hadoop.openpgp.buffersize
 * * spotify.hadoop.openpgp.compression
 * * spotify.hadoop.openpgp.encryption
 * * spotify.hadoop.openpgp.encrypt.keyId
 * * spotify.hadoop.openpgp.encrypt.passPhrase
 * * spotify.hadoop.openpgp.format
 * * spotify.hadoop.openpgp.integrity.sign
 * * spotify.hadoop.openpgp.pubring.path
 *
 * Note that the default settings has no encryption and no compression,
 * thus just creating an OpenPGP literal data packet.
**/
public class OpenPgpCompressor extends StreamCompressor {
	/// Compression algorithm name to value mapping.
	public static final Map<String, Integer> COMPRESSION_ALGORITHMS = EnumUtils.getStaticFinalFieldMapping(CompressionAlgorithmTags.class);

	/// Encryption algorithm name to value mapping.
	public static final Map<String, Integer> ENCRYPTION_ALGORITHMS = EnumUtils.getStaticFinalFieldMapping(SymmetricKeyAlgorithmTags.class);

	/**
	 * Construct a new compressor object.
	 *
	 * @param conf a valid configuration.
	**/
	public OpenPgpCompressor(Configuration conf) throws IOException {
		super(conf);
	}

	/**
	 * Overridden function to create the output stream chain.
	 *
	 * This version reads its settings from the configuration object.
	 * See the class help for more information.
	 *
	 * Note that the OpenPGP literal data file name is set to the empty
	 * string (which is what GnuPG does for stdin,) and the modification
	 * time is set to 0 (indicating something like "unknown".)
	**/
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

	/**
	 * Create the ouput stream chain.
	 *
	 * This method is static to ensure it is purely functional.
	 * Uses default protection, for unit tests.
	 *
	 * @param out the final stream to write to.
	 * @param key the encryption key (PGPPublicKey) or pass phrase (String.)
	 * @param encryption the encryption algorithm.
	 * @param signed whether to sign the stream or not.
	 * @param compression the compression algorithm.
	 * @param format the format of the literal data.
	 * @param name the file name of the input file, usually the empty string.
	 * @param mtime the last-modification-time to record, usually PGPLiteralDataGenerator.NOW.
	 * @param bufferSize the size of the Bouncy Castle buffers.
	 *
	 * @see org.bouncycastle.openpgp.PGPCompressedData
	 * @see org.bouncycastle.openpgp.PGPEncryptedData
	 * @see org.bouncycastle.openpgp.PGPLiteralData
	**/
	static OutputStream createOutputStream(OutputStream out, Object key, int encryption, boolean signed, int compression, int format, String name, Date mtime, int bufferSize) throws IOException {
		try {
			List<OutputStream> streams = new ArrayList<OutputStream>();

			streams.add(out);

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

				out = edg.open(out, new byte[bufferSize]);
				streams.add(out);
			}

			if (compression != PGPCompressedDataGenerator.UNCOMPRESSED) {
				PGPCompressedDataGenerator cdg = new PGPCompressedDataGenerator(
					compression);

				out = cdg.open(out);
				streams.add(out);
			}

			PGPLiteralDataGenerator ldg = new PGPLiteralDataGenerator();

			out = ldg.open(
				out,
				(char) format,
				name,
				mtime,
				new byte[bufferSize]);

			streams.add(out);

			return new MultipleClosingOutputStream(streams);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Return the public key ring file, as specified in configuration.
	 *
	 * Falls back to the GnuPG default.
	**/
	private File getPubringFile() {
		String path = getConf().get("spotify.hadoop.openpgp.pubring.path");

		if (path != null)
			return new File(path);

		return GnuPgUtils.getDefaultPubringFile();
	}

	/**
	 * Return the encryption key to be used.
	 *
	 * If a public key is found, that is returned, else a pass phrase
	 * String is returned, or null.
	 *
	 * @return a PGPPublicKey, String, or null.
	**/
	private Object getKey() {
		PGPPublicKey pubKey = getPublicKey();

		if (pubKey != null) return pubKey;

		return getEncryptionPassPhrase();
	}

	/**
	 * Return the encryption pass-phrase to use for symmetric-cipher only.
	 *
	 * This is like using "gpg -c".
	 *
	 * @return a string, or null.
	**/
	private String getEncryptionPassPhrase() {
		return getConf().get("spotify.hadoop.openpgp.encrypt.passPhrase");
	}

	/**
	 * Return the ID of the public key to use for signing and encrypting.
	 *
	 * @return a string, or null.
	**/
	private String getPublicKeyId() {
		return getConf().get("spotify.hadoop.openpgp.encrypt.keyId");
	}

	/**
	 * Return the public key to use for signing and encrypting.
	 *
	 * @return an object, or null.
	**/
	private PGPPublicKey getPublicKey() {
		return getPublicKey(getPublicKeyId());
	}

	/**
	 * Look up the given key ID in the default pubring file.
	 *
	 * @return an object, or null.
	**/
	private PGPPublicKey getPublicKey(String id) {
		if (id == null) return null;

		try {
			PGPPublicKeyRingCollection col = GnuPgUtils.createPublicKeyRingCollection(getPubringFile());

			return GnuPgUtils.getPublicKey(col, id);
		} catch (Exception ex) {
			throw new KeyNotFoundException(ex);
		}
	}

	/**
	 * Return an identifier of the format of the literal data.
	 *
	 * @return a constant from PGPLiteralData, defaulting to "binary."
	 *
	 * @see PGPLiteralData#BINARY
	 * @see PGPLiteralData#TEXT
	 * @see PGPLiteralData#UTF8
	**/
	private int getFormat() {
		String format = getConf().get("spotify.hadoop.openpgp.format", "binary");

		if (format.equals("binary")) return PGPLiteralDataGenerator.BINARY;
		else if (format.equals("text")) return PGPLiteralDataGenerator.TEXT;
		else if (format.equals("utf-8")) return PGPLiteralDataGenerator.UTF8;

		throw new RuntimeException("unknown format");
	}

	/**
	 * Return an identifier of the compression algorithm to use.
	 *
	 * Defaults to "uncompressed."
	**/
	private int getCompressionAlgorithm() {
		String algo = getConf().get("spotify.hadoop.openpgp.compression", "uncompressed");

		return COMPRESSION_ALGORITHMS.get(algo.toUpperCase());
	}

	/**
	 * Return an identifier of the encryption algorithm to use.
	 *
	 * This defaults to "cast5" if a key could be found, else "null."
	**/
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

	/**
	 * Return true if the stream should be signed.
	 *
	 * Defaults to true iff a key could be found.
	**/
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

	/**
	 * Return the default buffer size for Bouncy Castle buffers.
	 *
	 * Defaults to 16 kB.
	**/
	private int getBufferSize() {
		return getConf().getInt("spotify.hadoop.openpgp.buffersize", 1 << 14);
	}
}
