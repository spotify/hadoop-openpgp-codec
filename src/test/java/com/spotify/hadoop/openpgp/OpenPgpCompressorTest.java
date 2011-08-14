package com.spotify.hadoop.openpgp;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;

import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;


public class OpenPgpCompressorTest {
	static {
		java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
	}

	@Test
	public void createPlainOutputStream() throws Exception {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		OutputStream cout = OpenPgpCompressor.createOutputStream(
			bout,
			getPublicKey(),
			PGPEncryptedData.NULL,
			false,
			PGPCompressedData.UNCOMPRESSED,
			PGPLiteralData.UTF8,
			"",
			PGPLiteralData.NOW,
			1 << 14);

		final byte[] DATA = "Hello World!".getBytes("UTF-8");

		cout.write(DATA);
		cout.close();

		final byte[] HEADER = { (byte) 0xCB, 0x12, 0x75, 0, 0, 0, 0, 0 };

		byte[] both = new byte[HEADER.length + DATA.length];

		System.arraycopy(HEADER, 0, both, 0, HEADER.length);
		System.arraycopy(DATA, 0, both, HEADER.length, DATA.length);

		assertEquals(both, bout.toByteArray());
	}

	public static PGPPublicKey getPublicKey() {
		try {
			PGPPublicKeyRingCollection col = GnuPgUtils.createPublicKeyRingCollection(new File("etc", GnuPgUtils.PUBRING_FILE_NAME));

			return GnuPgUtils.getPublicKey(col, "75FAD0E0");
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		} catch (PGPException ex) {
			throw new RuntimeException(ex);
		}
	}
}
