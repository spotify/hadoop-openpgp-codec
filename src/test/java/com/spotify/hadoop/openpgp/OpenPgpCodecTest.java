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


public class OpenPgpCodecTest {
	static {
		java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
	}

	@Test
	public void create() throws Exception {
		new OpenPgpCodec();
	}

	@Test
	public void createCompressor() throws Exception {
		OpenPgpCodec codec = new OpenPgpCodec();

		assertTrue(codec.getCompressorType().isInstance(codec.createCompressor()));
	}

	@Test
	public void createDecompressor() throws Exception {
		OpenPgpCodec codec = new OpenPgpCodec();

		assertTrue(codec.getDecompressorType().isInstance(codec.createDecompressor()));
	}
}
