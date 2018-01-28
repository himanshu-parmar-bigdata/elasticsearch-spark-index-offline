package com.parmarh.elasticsearch;

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

class ESIndexShardSnapshotFunction<K,V> implements Function2<Integer, Iterator<Tuple2<K,V>>,
		Iterator<Void>>{
	public ESIndexShardSnapshotFunction(ESIndexShardSnapshotCreator creator, Supplier<Configuration> configurationSupplier,
										String destination, String indexName, int bulkSize, String indexType, long timeout) {
		this.creator = creator;
		this.configurationSupplier = configurationSupplier;
		this.destination = destination;
		this.indexName = indexName;
		this.bulkSize = bulkSize;
		this.indexType = indexType;
		this.timeout = timeout;
	}

	private final ESIndexShardSnapshotCreator creator;
	private final Supplier<Configuration> configurationSupplier;
	private final String destination;
	private final String indexName;
	private final int bulkSize;
	private final String indexType;
	private final long timeout;

	@Override
	public Iterator<Void> call(Integer partNum, Iterator<Tuple2<K, V>> data) throws Exception {
		FileSystem fs = FileSystem.get(new URI(destination), configurationSupplier.get());
		creator.create(fs, indexName, partNum, bulkSize, indexType, data, timeout);
		return new ArrayList<Void>().iterator();
	}
}