package com.aerospike.kafka.connect.errors;

public class ConversionError extends Exception {

	private static final long serialVersionUID = -6254341864909680037L;

	public ConversionError() {
	}

	public ConversionError(String message) {
		super(message);
	}

	public ConversionError(Throwable cause) {
		super(cause);
	}

	public ConversionError(String message, Throwable cause) {
		super(message, cause);
	}
}
