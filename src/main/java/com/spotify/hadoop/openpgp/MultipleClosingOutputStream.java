package com.spotify.hadoop.openpgp;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;


/**
 * An output stream that additionally takes a list of streams to close
 * when this stream is closed.
 *
 * This is useful for the Bouncy Castle OpenPGP streams that do not close
 * their upstream streams, but still need #close() to be called for final
 * buffer flushing.
 *
 * In a structure like
 *
 *  DataInputStream(BufferedInputStream(FileInputStream()))
 *
 * FileInputStream should be first in the list. Note that this example is
 * useless since the built-in OutputStreams close their upstreams.
**/
public class MultipleClosingOutputStream extends FilterOutputStream {
	private List<OutputStream> streams = new ArrayList<OutputStream>();

	/**
	 * Construct a new stream with a list of upstreams.
	 *
	 * The last element in the streams list is given to the FilterOutputStream
	 * constructor.
	**/
	public MultipleClosingOutputStream(List<OutputStream> streams) {
		super(streams.get(streams.size() - 1));
		this.streams.addAll(streams);
	}

	/**
	 * Construct a new stream with just a single backend.
	 *
	 * This makes the MCOS class work like FilterOutputStream.
	**/
	public MultipleClosingOutputStream(OutputStream out) {
		super(out);
		this.streams.add(out);
	}

	/**
	 * Close this stream.
	 *
	 * Closes all registered streams in reverse order.
	**/
	public void close() throws IOException {
		for (int i = streams.size() - 1; i >= 0; --i)
			streams.get(i).close();
	}
}
