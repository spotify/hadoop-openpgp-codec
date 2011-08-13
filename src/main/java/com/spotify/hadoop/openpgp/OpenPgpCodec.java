package com.spotify.hadoop.openpgp;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

public class OpenPgpCodec extends AbstractCompressionCodec {
	public Class<? extends Compressor> getCompressorType() {
		return OpenPgpCompressor.class;
	}

	public Compressor createCompressor() {
		return new OpenPgpCompressor(getConf());
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
}
