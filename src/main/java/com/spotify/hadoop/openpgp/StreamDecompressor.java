package com.spotify.hadoop.openpgp;

import java.io.IOException;
import java.io.InputStream;
import static java.lang.Math.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * This is a Hadoop IO decompressor backed by an InputStream.
 *
 * This class is abstract, and you have to override the createInputStream()
 * function. Use the supplied InputStream as the source of the stream
 * chain.
 *
 * Note that unlike the StreamCompressor, this class drives the InputStream
 * in a separate thread. createInputStream() is called in that thread, and it
 * is thus safe for blocking calls to InputStream#read() to occur during the
 * createInputStream() call. Your input stream does not have to be thread-safe.
 *
 * There is probably nowhere around this. For an OutputStream, we can always
 * allocate a separate buffer for storing additional data not consumed by the
 * parameters to compress(). For the InputStream case, we cannot predict what
 * the next read() will return...
**/
public abstract class StreamDecompressor implements Decompressor {
	private Configuration conf;
	private Thread streamThread;

	private byte[] inputBytes;
	private int inputOff;
	private int inputLen;

	private BlockingQueue<Action> actionQueue = new LinkedBlockingQueue<Action>();

	private int numBytesRead;
	private int numBytesWritten;
	private volatile boolean hasFinished;

	public StreamDecompressor(Configuration conf) {
		this.conf = conf;
		streamThread = new Thread() {
			public void run() {
				pump();
			}
		};

		streamThread.setDaemon(true);
		streamThread.start();
	}

	public void setInput(byte[] b, int off, int len) {
		synchronized (streamThread) {
			inputBytes = b;
			inputOff = off;
			inputLen = len;
		}
	}

	public boolean needsInput() {
		return inputLen == 0 && !hasFinished;
	}

	public boolean needsDictionary() {
		return false;
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

	public int getRemaining() {
		return inputLen;
	}

	public void finish() {
		hasFinished = true;
	}

	public boolean finished() {
		return hasFinished && inputLen == 0;
	}

	public int decompress(byte[] b, int off, int len) throws IOException {
		try {
			return (int) (Integer) execute(new Action(b, off, len, false));
		} catch (InterruptedException ex) {
			throw new IOException(ex);
		}
	}

	public void reinit(Configuration conf) {
		this.conf = conf;

		try {
			execute(new Action(null, 0, 0, true));
		} catch (InterruptedException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void reset() {
		reinit(conf);
	}

	public void end() {
		try {
			actionQueue.put(new Action(null, -1, -1, false));
			streamThread.join();
			streamThread = null;
		} catch (InterruptedException ex) {
			throw new RuntimeException(ex);
		}
	}

	protected Configuration getConf() {
		return conf;
	}

	protected abstract InputStream createInputStream(InputStream out) throws IOException;

	private Object execute(Action a) throws InterruptedException {
		actionQueue.put(a);

		return a.waitForResult();
	}

	private void pump() {
		InputStream stream = null;

		try {
			for (;;) {
				Action a = actionQueue.take();

				if (a.isEnd()) break;

				if (stream == null || a.wantsNewStream()) {
					if (stream != null)
						stream.close();

					stream = createInputStream(new SelfInputStream());
				}

				if (a.hasBuffer())
					a.setResult(Integer.valueOf(stream.read(a.getBytes(), a.getOffset(), a.getLength())));
				else
					a.setResult();
			}
		} catch (InterruptedException ex) {
			ex.printStackTrace();
			// This will cause setInput() to fail.
			streamThread = null;
		} catch (IOException ex) {
			ex.printStackTrace();
			// This will cause setInput() to fail.
			streamThread = null;
		} finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (IOException ex) {
				}
			}
		}
	}

	private class SelfInputStream extends InputStream {
		public void close() throws IOException {
			finish();
		}

		public int available() throws IOException {
			return getRemaining();
		}

		public int read(byte[] b, int off, int len) throws IOException {
			if (inputLen == 0 && hasFinished)
				return -1;

			int n = min(len, inputLen);

			System.arraycopy(inputBytes, inputOff, b, off, n);
			inputLen -= n;
			inputOff += n;

			return n;
		}

		public int read() throws IOException {
			if (inputLen == 0)
				return -1;

			byte b = inputBytes[inputOff + inputLen--];
			inputOff += 1;

			return (b < 0 ? 256 + b : b);
		}
	}

	private static class Action {
		private byte[] bytes;
		private int off;
		private int len;
		private boolean recreateStream;
		private Object result;

		public Action(byte[] bytes, int off, int len, boolean recreateStream) {
			this.bytes = bytes;
			this.off = off;
			this.len = len;
			this.recreateStream = recreateStream;
		}

		public String toString() {
			return "[" + getClass().getName() +
				": bytes=" + bytes +
				", off=" + off +
				", len=" + len +
				", recreateStream=" + recreateStream +
				", result=" + result +
				"]";
		}

		public byte[] getBytes() {
			return bytes;
		}

		public int getOffset() {
			return off;
		}

		public int getLength() {
			return len;
		}

		public boolean hasBuffer() {
			return bytes != null;
		}

		public boolean isEnd() {
			return len == -1;
		}

		public boolean wantsNewStream() {
			return recreateStream;
		}

		public synchronized Object waitForResult() throws InterruptedException {
			while (result == null)
				wait();

			return result;
		}

		public synchronized void setResult() {
			setResult(this);
		}

		public synchronized void setResult(Object result) {
			this.result = result;
			notifyAll();
		}
	}
}
