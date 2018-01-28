package com.parmarh.elasticsearch;

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.net.URI;
import java.util.Iterator;


public class ESIndexShardSnapshotPipeline<K,V> {
	private final ESIndexShardSnapshotCreator creator;
	private final Supplier<Configuration> configurationSupplier;
	private final String indexName;
	private final String indexType;
	private final int bulkSize;
	private final long timeout;

	public ESIndexShardSnapshotPipeline(ESIndexShardSnapshotCreator creator, Supplier<Configuration> configurationSupplier,
										String indexName, String indexType, int bulkSize, long timeout) {
		this.creator = creator;
		this.configurationSupplier = configurationSupplier;
		this.indexName = indexName;
		this.indexType = indexType;
		this.bulkSize = bulkSize;
		this.timeout = timeout;
	}

	public void process(JavaPairRDD<K, V> pairRDD) throws Exception {
		URI finalDestURI = new URI(creator.getSnapshotDestination());

		Function2<Integer, Iterator<Tuple2<K, V>>, Iterator<Void>> snapshotFunc =
			new ESIndexShardSnapshotFunction<K,V>(creator, configurationSupplier,
					creator.getSnapshotDestination(), indexName, bulkSize, indexType, timeout);
        //apply snapshot function on each partition of pairRDD
		pairRDD.mapPartitionsWithIndex(snapshotFunc, true).count();

		FileSystem fs = FileSystem.get(finalDestURI, configurationSupplier.get());

		//generating top level manifest files and moving to final destination
		creator.postprocess(fs, indexName, pairRDD.partitions().size(), indexType, timeout);
	}
}