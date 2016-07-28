package com.aerospike.kafka.connect.sink;

import java.util.List;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class KeyAndBins {

	private Key key;
	private Bin[] bins;

	public KeyAndBins(Key key, Bin[] bins) {
		this.key = key;
		this.bins = bins;
	}
	
	public KeyAndBins(Key key, List<Bin> bins) {
		this.key = key;
		this.bins = bins.toArray(new Bin[0]);
	}

	public Key getKey() {
		return key;
	}
	
	public Bin[] getBins() {
		return bins;
	}
}
