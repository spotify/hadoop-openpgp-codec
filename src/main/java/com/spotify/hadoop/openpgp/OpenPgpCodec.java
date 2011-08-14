package com.spotify.hadoop.openpgp;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.Security;

import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

public class OpenPgpCodec extends AbstractCompressionCodec {
	public OpenPgpCodec() {
		super();
		ensureBouncyCastleProvider();
	}

	public Class<? extends Compressor> getCompressorType() {
		return OpenPgpCompressor.class;
	}

	public Compressor createCompressor() {
		try {
			return new OpenPgpCompressor(getConf());
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public Class<? extends Decompressor> getDecompressorType() {
		return OpenPgpDecompressor.class;
	}

	public Decompressor createDecompressor() {
		return new OpenPgpDecompressor(getConf());
	}

	public String getDefaultExtension() {
		return ".gpg";
	}

	protected void ensureBouncyCastleProvider() {
		if (Security.getProvider("BC") == null)
			Security.addProvider(new BouncyCastleProvider());
	}
}
