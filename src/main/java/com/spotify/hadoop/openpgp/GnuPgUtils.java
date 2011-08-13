package com.spotify.hadoop.openpgp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPUtil;


public class GnuPgUtils {
	public static final String PUBRING_FILE_NAME = "pubring.gpg";

	public static PGPPublicKeyRingCollection createPublicKeyRingCollection() throws IOException, PGPException {
		return createPublicKeyRingCollection(getDefaultPubringFile());
	}

	public static PGPPublicKeyRingCollection createPublicKeyRingCollection(File pubring) throws IOException, PGPException {
		FileInputStream in = new FileInputStream(pubring);

		try {
			return new PGPPublicKeyRingCollection(PGPUtil.getDecoderStream(in));
		} finally {
			in.close();
		}
	}

	public static File getDefaultPubringFile() {
		String path = System.getenv("GNUPGHOME");

		if (path != null)
			return new File(path, PUBRING_FILE_NAME);

		return new File(System.getProperty("user.home") + File.separator + ".gnupg", PUBRING_FILE_NAME);
	}

	public static PGPPublicKey getPublicKey(PGPPublicKeyRingCollection col, String id) {
		if (id == null) return null;

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
	}
}
