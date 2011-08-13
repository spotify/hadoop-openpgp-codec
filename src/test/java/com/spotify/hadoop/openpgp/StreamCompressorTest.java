package com.spotify.hadoop.openpgp;

import java.io.OutputStream;

import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;


public class StreamCompressorTest {
	@Test
	public void create() {
		StreamCompressor c = createIdentity();

		assert c.needsInput();
		assertEquals(0, c.getBytesRead());
		assertEquals(0, c.getBytesWritten());
		assert !c.finished();
		c.end();
	}

	@Test
	public void setInput() throws Exception {
		StreamCompressor c = createIdentity();
		byte[] b = "Hello World!".getBytes("UTF-8");

		c.setInput(b, 0, b.length);
		assert !c.needsInput();
		c.end();
	}

	@Test
	public void finish() throws Exception {
		StreamCompressor c = createIdentity();
		byte[] b = "Hello World!".getBytes("UTF-8");

		c.setInput(b, 0, b.length);
		c.finish();
		assert !c.finished();
		c.end();
	}

	@Test
	public void compress() throws Exception {
		StreamCompressor c = createIdentity();
		byte[] b = "Hello World!".getBytes("UTF-8");

		c.setInput(b, 0, b.length);
		c.finish();

		// One byte larger to see we don't get garbage.
		byte[] buf = new byte[b.length + 1];

		assertEquals(b.length, c.compress(buf, 0, buf.length));
		assertEquals("Hello World!", new String(buf, 0, b.length, "UTF-8"));
		assert c.finished();
		c.end();
	}

	@Test
	public void compressTwice() throws Exception {
		StreamCompressor c = createIdentity();
		byte[] b = "Hello World!".getBytes("UTF-8");

		c.setInput(b, 0, b.length);
		c.finish();

		// One byte larger to see we don't get garbage.
		byte[] buf = new byte[b.length + 1];

		assertEquals(6, c.compress(buf, 0, 6));
		assert !c.finished();
		assertEquals(6, c.compress(buf, 6, 7));
		assertEquals("Hello World!", new String(buf, 0, b.length, "UTF-8"));
		assert c.finished();
		c.end();
	}

	@Test
	public void setInputTwice() throws Exception {
		StreamCompressor c = createIdentity();
		byte[] b = "Hello World!".getBytes("UTF-8");

		c.setInput(b, 0, 6);

		// One byte larger to see we don't get garbage.
		byte[] buf = new byte[b.length + 1];

		assertEquals(6, c.compress(buf, 0, buf.length));

		c.setInput(b, 6, 6);
		c.finish();

		assertEquals(6, c.compress(buf, 6, 7));
		assertEquals("Hello World!", new String(buf, 0, b.length, "UTF-8"));
		c.end();
	}

	StreamCompressor createIdentity() {
		return new StreamCompressor(null) {
			protected OutputStream createOutputStream(OutputStream out) {
				return out;
			}
		};
	}
}
