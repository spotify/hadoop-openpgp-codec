package com.spotify.hadoop.openpgp;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;

public abstract class AbstractCompressionCodec implements CompressionCodec, Configurable {
	private Configuration conf;

	public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
		return createOutputStream(out, createCompressor());
	}

	public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
		return new CompressorStream(out, compressor);
	}

	public CompressionInputStream createInputStream(InputStream in) throws IOException {
		return createInputStream(in, createDecompressor());
	}

	public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
		return new DecompressorStream(in, decompressor);
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return conf;
	}
}
