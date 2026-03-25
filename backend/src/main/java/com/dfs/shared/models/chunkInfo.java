package com.dfs.shared.models;

import java.util.UUID;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ChunkInfo {
    private final String chunkId;
    private final int sequenceNumber;
    private final List<String> replicatedNodeIds;

    public ChunkInfo(int sequenceNumber) {
        // Automatically generate a massive, mathematically unique ID
        this.chunkId = UUID.randomUUID().toString();
        this.sequenceNumber = sequenceNumber;
        
        // A thread-safe list to hold the IDs of the nodes storing this chunk
        this.replicatedNodeIds = new CopyOnWriteArrayList<>();
    }

    public String getChunkId() {
        return chunkId;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public List<String> getReplicatedNodeIds() {
        return replicatedNodeIds;
    }

    public void addNodeLocation(String nodeId) {
        if (!replicatedNodeIds.contains(nodeId)) {
            replicatedNodeIds.add(nodeId);
        }
    }

    public void removeNodeLocation(String nodeId) {
        replicatedNodeIds.remove(nodeId);
    }
}
