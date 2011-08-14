package com.spotify.hadoop.openpgp;

import java.io.FileInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.security.NoSuchProviderException;

import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;

import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPSecretKeyRingCollection;


public class OpenPgpDecompressorTest {
	static {
		java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
	}

	@Test
	public void createInputStream() throws Exception {
		InputStream fin = ClassLoader.getSystemResourceAsStream("hello.txt.gpg");
		InputStream din = OpenPgpDecompressor.createInputStream(
			fin,
			false,
			new OpenPgpDecompressor.PrivateKeyFactory() {
				public PGPPrivateKey getPrivateKey(long id) {
					return OpenPgpDecompressorTest.this.getPrivateKey(id);
				}
			});

		byte[] buffer = new byte[1024];
		int n = din.read(buffer);
		String data = new String(buffer, 0, n, "UTF-8");

		assertEquals("Hello world!\n", data);
	}

	public static PGPPrivateKey getPrivateKey(long id) {
		try {
			PGPSecretKeyRingCollection col = GnuPgUtils.createSecretKeyRingCollection(new File("etc", GnuPgUtils.SECRING_FILE_NAME));

			return GnuPgUtils.getPrivateKey(col, id, "");
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		} catch (NoSuchProviderException ex) {
			throw new RuntimeException(ex);
		} catch (PGPException ex) {
			throw new RuntimeException(ex);
		}
	}
}
