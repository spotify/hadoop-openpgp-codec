package com.spotify.hadoop.openpgp;


public class KeyNotFoundException extends RuntimeException {
	public KeyNotFoundException(String msg) {
		super(msg);
	}

	public KeyNotFoundException(Throwable cause) {
		super(cause);
	}
}
