package com.parmarh.elasticsearch.util;

import org.apache.spark.Partitioner;

import java.util.HashMap;

public class ElasticSearchPartitioner extends Partitioner{

    int numPatitions = 0;
    private HashMap<String, Integer> routes;

    public ElasticSearchPartitioner(int numPatitions) {
        this.numPatitions = numPatitions;
        routes = EsUtils.getRouteMap(16);

    }

    @Override
    public int numPartitions() {
        return numPatitions;
    }

    @Override
    public int getPartition(Object key) {
        if ( numPatitions == 0 ) return 0;
        return routes.get( key );

    }
}
