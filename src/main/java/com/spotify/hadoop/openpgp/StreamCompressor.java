package com.spotify.hadoop.openpgp;

import java.io.IOException;
import java.io.OutputStream;
import static java.lang.Math.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

/**
 * This is a Hadoop IO compressor backed by an OutputStream.
 *
 * This class is abstract, and you have to override the createOutputStream()
 * function. Use the supplied OutputStream as the final output of the stream
 * chain.
**/
public abstract class StreamCompressor implements Compressor {
	private OutputStream stream;
	private Configuration conf;

	private byte[] inputBytes;
	private int inputOff;
	private int inputLen;

	private byte[] outputBytes;
	private int outputOff;
	private int outputLen;

	private byte[] bufferBytes;
	private int bufferOff;
	private int bufferLen;

	private int numBytesRead;
	private int numBytesWritten;
	private boolean hasFinished;
	private boolean streamClosed;

	public StreamCompressor(Configuration conf) throws IOException {
		this(conf, conf.getInt("spotify.hadoop.openpgp.streamCompressor.initialBufferSize", 1024));
	}

	public StreamCompressor(Configuration conf, int initialBufferSize) throws IOException {
		reinit(conf);
		bufferBytes = new byte[initialBufferSize];
		this.stream = createOutputStream(new SelfOutputStream());
	}

	public void setInput(byte[] b, int off, int len) {
		inputBytes = b;
		inputOff = off;
		inputLen = len;
	}

	public boolean needsInput() {
		return inputLen == 0 && !hasFinished;
	}

	public void setDictionary(byte[] b, int off, int len) {
		throw new UnsupportedOperationException();
	}

	public long getBytesRead() {
		return numBytesRead;
	}

	public long getBytesWritten() {
		return numBytesWritten;
	}

	public void finish() {
		hasFinished = true;
	}

	public boolean finished() {
		return hasFinished && bufferLen == 0 && inputLen == 0 && streamClosed;
	}

	public int compress(byte[] b, int off, int len) throws IOException {
		int n = min(len, bufferLen);

		System.arraycopy(bufferBytes, bufferOff, b, off, n);
		bufferLen -= n;
		bufferOff += n;
		len -= n;
		off += n;

		if (len == 0) return n;

		outputBytes = b;
		outputOff = off;
		outputLen = len;

		stream.write(inputBytes, inputOff, inputLen);
		numBytesRead += inputLen;
		inputLen = 0;

		if (hasFinished) {
			streamClosed = true;
			stream.close();
		}

		outputBytes = null;
		numBytesWritten += n + len - outputLen;

		return n + len - outputLen;
	}

	public void reinit(Configuration conf) {
		this.conf = conf;
		inputLen = 0;
	}

	public void reset() {
		reinit(conf);
	}

	public void end() {
		try {
			if (!streamClosed) {
				streamClosed = true;
				stream.close();
			}
		} catch (IOException ex) {
		}
	}

	protected Configuration getConf() {
		return conf;
	}

	protected abstract OutputStream createOutputStream(OutputStream out) throws IOException;

	private class SelfOutputStream extends OutputStream {
		public void close() throws IOException {
			finish();
		}

		public void flush() throws IOException {
			// It's a pull-API, so we can't do anything here.
		}

		public void write(byte[] b, int off, int len) throws IOException {
			if (bufferLen == 0 && outputBytes != null && outputLen > 0) {
				// We're in a compress() call and can write directly to output.
				int n = min(len, outputLen);

				System.arraycopy(b, off, outputBytes, outputOff, n);
				outputOff += n;
				outputLen -= n;
				off += n;
				len -= n;

				if (len == 0) return;
			}

			if (bufferLen + len > bufferBytes.length) {
				// Allocate larger buffer.
				int n = bufferBytes.length * 2;

				while (n < bufferLen + len)
					n *= 2;

				byte[] newBytes = new byte[n];

				System.arraycopy(bufferBytes, bufferOff, newBytes, 0, bufferLen);
				bufferBytes = newBytes;
			} else if (bufferOff > 0) {
				// Shift buffer to beginning.
				System.arraycopy(bufferBytes, bufferOff, bufferBytes, 0, bufferLen);
			}

			bufferOff = 0;
			System.arraycopy(b, off, bufferBytes, bufferLen, len);
			bufferLen += len;
		}

		public void write(int b) throws IOException {
			if (bufferLen == 0 && outputBytes != null && outputLen > 0) {
				// We're in a compress() call and can write directly to output.
				outputBytes[outputOff++] = (byte) b;
				--outputLen;
				return;
			}

			if (bufferLen == bufferBytes.length) {
				// Allocate larger buffer.
				byte[] newBytes = new byte[bufferBytes.length * 2];

				System.arraycopy(bufferBytes, bufferOff, newBytes, 0, bufferLen);
				bufferBytes = newBytes;
			} else if (bufferOff > 0) {
				// Shift buffer to beginning.
				System.arraycopy(bufferBytes, bufferOff, bufferBytes, 0, bufferLen);
			}

			bufferOff = 0;
			bufferBytes[bufferLen++] = (byte) b;
		}
	}
}
