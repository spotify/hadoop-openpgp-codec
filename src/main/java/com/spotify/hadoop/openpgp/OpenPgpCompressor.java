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

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.bcpg.CompressionAlgorithmTags;
import org.bouncycastle.bcpg.SymmetricKeyAlgorithmTags;

public class OpenPgpCompressor extends StreamCompressor {
	public static final String PUBRING_FILE_NAME = "pubring.gpg";
	public static final Map<String, Integer> COMPRESSION_ALGORITHMS = EnumUtils.getStaticFinalFieldMapping(CompressionAlgorithmTags.class);
	public static final Map<String, Integer> ENCRYPTION_ALGORITHMS = EnumUtils.getStaticFinalFieldMapping(SymmetricKeyAlgorithmTags.class);

	public OpenPgpCompressor(Configuration conf) {
		super(conf);
	}

	protected OutputStream createOutputStream(OutputStream out) {
		return createOutputStream(
			out,
			getPublicKey(),
			getEncryptionAlgorithm(),
			wantsSignature(),
			getCompressionAlgorithm(),
			getFormat(),
			"",
			PGPLiteralDataGenerator.NOW,
			getBufferSize());
	}

	// Default protection, for unit tests.
	static OutputStream createOutputStream(OutputStream out, PGPPublicKey key, int encryption, boolean signed, int compression, int format, String name, Date mtime, int bufferSize) {
		try {
			if (encryption != PGPEncryptedDataGenerator.NULL || signed) {
				PGPEncryptedDataGenerator edg = new PGPEncryptedDataGenerator(
					encryption,
					signed,
					new SecureRandom(),
					(String) null);

				edg.addMethod(key);
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

		path = System.getenv("GNUPGHOME");

		if (path != null)
			return new File(path, PUBRING_FILE_NAME);

		return new File(System.getProperty("user.home") + File.separator + ".gnupg", PUBRING_FILE_NAME);
	}

	private String getPublicKeyId() {
		return getConf().get("spotify.hadoop.openpgp.pubkey");
	}

	private PGPPublicKey getPublicKey() {
		return getPublicKey(getPublicKeyId());
	}

	private PGPPublicKey getPublicKey(String id) {
		if (id == null) return null;

		try {
			FileInputStream in = new FileInputStream(getPubringFile());

			try {
				PGPPublicKeyRingCollection col = new PGPPublicKeyRingCollection(PGPUtil.getDecoderStream(in));

				long lid = Long.parseLong(id, 0x10);
				long mask = (1L << (4 * id.length())) - 1;

				for (Iterator<PGPPublicKeyRing> rit = col.getKeyRings(); rit.hasNext();) {
					for (Iterator<PGPPublicKey> kit = rit.next().getPublicKeys(); kit.hasNext();) {
						PGPPublicKey key = kit.next();

						if ((key.getKeyID() & mask) == lid) {
							if (!key.isEncryptionKey())
								throw new IncompatibleKeyException("not an encryption key: " + id);

							if (key.isRevoked())
								throw new IncompatibleKeyException("key is revoked: " + id);

							return key;
						}
					}
				}

				throw new KeyNotFoundException("key not found: " + id);
			} finally {
				in.close();
			}
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
		String algo = getConf().get("spotify.hadoop.openpgp.encryption", "null");

		return ENCRYPTION_ALGORITHMS.get(algo.toUpperCase());
	}

	private boolean wantsSignature() {
		return getConf().getBoolean("spotify.hadoop.openpgp.signed", true);
	}

	private int getBufferSize() {
		return getConf().getInt("spotify.hadoop.openpgp.buffersize", 1 << 14);
	}
}
