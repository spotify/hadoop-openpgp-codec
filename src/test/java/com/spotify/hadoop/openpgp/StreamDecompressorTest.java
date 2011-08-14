package com.spotify.hadoop.openpgp;

import java.io.InputStream;

import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;


public class StreamDecompressorTest {
	@Test
	public void create() {
		StreamDecompressor c = createIdentity();

		assert c.needsInput();
		assertEquals(0, c.getBytesRead());
		assertEquals(0, c.getBytesWritten());
		assertEquals(0, c.getRemaining());
		assert !c.finished();
		c.end();
	}

	@Test
	public void setInput() throws Exception {
		StreamDecompressor c = createIdentity();
		byte[] b = "Hello World!".getBytes("UTF-8");

		c.setInput(b, 0, b.length);
		assert !c.needsInput();
		assertEquals(b.length, c.getRemaining());
		c.end();
	}

	@Test
	public void finish() throws Exception {
		StreamDecompressor c = createIdentity();
		byte[] b = "Hello World!".getBytes("UTF-8");

		c.setInput(b, 0, b.length);
		c.finish();
		assert !c.finished();
		c.end();
	}

	@Test
	public void decompress() throws Exception {
		StreamDecompressor c = createIdentity();
		byte[] b = "Hello World!".getBytes("UTF-8");

		c.setInput(b, 0, b.length);
		c.finish();

		// One byte larger to see we don't get garbage.
		byte[] buf = new byte[b.length + 1];

		assertEquals(b.length, c.decompress(buf, 0, buf.length));
		assertEquals("Hello World!", new String(buf, 0, b.length, "UTF-8"));
		assert c.finished();
		assertEquals(0, c.getRemaining());
		c.end();
	}

	@Test
	public void decompressTwice() throws Exception {
		StreamDecompressor c = createIdentity();
		byte[] b = "Hello World!".getBytes("UTF-8");

		c.setInput(b, 0, b.length);
		c.finish();

		// One byte larger to see we don't get garbage.
		byte[] buf = new byte[b.length + 1];

		assertEquals(6, c.decompress(buf, 0, 6));
		assert !c.finished();
		assertEquals(6, c.decompress(buf, 6, 7));
		assertEquals("Hello World!", new String(buf, 0, b.length, "UTF-8"));
		assert c.finished();
		assertEquals(0, c.getRemaining());
		c.end();
	}

	@Test
	public void setInputTwice() throws Exception {
		StreamDecompressor c = createIdentity();
		byte[] b = "Hello World!".getBytes("UTF-8");

		c.setInput(b, 0, 6);

		// One byte larger to see we don't get garbage.
		byte[] buf = new byte[b.length + 1];

		assertEquals(6, c.decompress(buf, 0, buf.length));

		c.setInput(b, 6, 6);
		c.finish();

		assertEquals(6, c.decompress(buf, 6, 7));
		assertEquals("Hello World!", new String(buf, 0, b.length, "UTF-8"));
		c.end();
	}

	StreamDecompressor createIdentity() {
		return new StreamDecompressor(null) {
			protected InputStream createInputStream(InputStream in) {
				return in;
			}
		};
	}
}
