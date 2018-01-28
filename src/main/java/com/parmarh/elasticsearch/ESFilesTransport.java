package com.parmarh.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ESFilesTransport implements Serializable {
	static final String DIR_SEPARATOR = File.separator;


	public ESFilesTransport() {
	}



	/**
	 * Moves prepared locally shards data or index metadata to destination
	 * @param snapshotName
	 * @param index
	 * @param snapshotWorkingLocation - local path
	 * @param destination - remote path(hdfs/s3/local)
	 * @param destShard
	 * @param moveShards - either to move shard data or index metadata
	 * @throws IOException
	 */
	public void move(FileSystem fs, String snapshotName, String index, String snapshotWorkingLocation, String destination, int destShard, boolean moveShards) throws IOException {
		// Figure out which shard has all the data
		String baseIndexShardLocation = snapshotWorkingLocation + DIR_SEPARATOR + "indices" + DIR_SEPARATOR+index;

		if (moveShards) {
			String largestShard = getShardSource(baseIndexShardLocation);

			// Upload shard data
			String shardSource = baseIndexShardLocation + DIR_SEPARATOR + largestShard;

			String shardDestination = destination + DIR_SEPARATOR + "indices" + DIR_SEPARATOR + index + DIR_SEPARATOR;
			transferDir(fs, shardDestination, shardSource, destShard);
			transferFile(fs, shardDestination, "snapshot-" + snapshotName, baseIndexShardLocation);
		} else {
			// Upload top level manifests
			transferFile(fs, destination, "metadata-" + snapshotName, snapshotWorkingLocation);
			transferFile(fs, destination, "snapshot-" + snapshotName, snapshotWorkingLocation);
			transferFile(fs, destination, "index", snapshotWorkingLocation);


			// Upload per-index manifests
			//String indexManifestSource =  baseIndexShardLocation;
			//String indexManifestDestination = destination + DIR_SEPARATOR + "indices" + DIR_SEPARATOR + index;
			//transferFile(fs, indexManifestDestination, "snapshot-" + snapshotName, indexManifestSource);
		}
	}

	/**
	 * We've snapshotted an index with all data routed to a single shard (1 shard)
	 */
	private String getShardSource(String baseIndexLocation) throws IOException {
		// Get a list of shards in the snapshot

		File file = new File(baseIndexLocation);
		String[] shardDirectories = file.list(DirectoryFileFilter.DIRECTORY);

		// Figure out which shard has all the data in it. Since we've routed all data to it, there'll only be one
		Long biggestDirLength = null;
		String biggestDir = null;
		for(String directory : shardDirectories) {
			File curDir = new File(baseIndexLocation +DIR_SEPARATOR+ directory);
			long curDirLength = FileUtils.sizeOfDirectory(curDir);
			if(biggestDirLength == null || biggestDirLength < curDirLength) {
				biggestDir = directory;
				biggestDirLength = curDirLength;
			}
		}

		return biggestDir;
	}


	private void ensurePathExists(FileSystem fs, String destination) throws IOException {
		fs.mkdirs(new Path(destination));
	}

	private void transferFile(FileSystem fs, String destination, String filename, String localDirectory) throws IOException {
		Path source = new Path(localDirectory + DIR_SEPARATOR + filename);
		ensurePathExists(fs, destination);
		fs.copyFromLocalFile(false, true, source, new Path(destination + DIR_SEPARATOR + filename));
	}

	private void transferDir(FileSystem fs, String destination, String localShardPath, int shard) throws IOException {
		destination = destination + shard + DIR_SEPARATOR;
		ensurePathExists(fs, destination);
		File[] files = new File(localShardPath).listFiles();
		for (File file : files) {
			transferFile(fs, destination, file.getName(), localShardPath);
		}
	}
}
