package com.spotify.hadoop.openpgp;


public class IncompatibleKeyException extends RuntimeException {
	public IncompatibleKeyException(String msg) {
		super(msg);
	}
}
