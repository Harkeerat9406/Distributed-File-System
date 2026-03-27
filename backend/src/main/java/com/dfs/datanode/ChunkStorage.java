package com.dfs.datanode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files; // Added this import so we can read files quickly!

public class ChunkStorage {

    private final String storageDirectory;

    public ChunkStorage(String nodeId) {
        // Creates a simulated hard drive folder for each node
        // e.g., "storage_sim/node_8001/"
        this.storageDirectory = System.getProperty("user.dir") + "/storage_sim/" + nodeId + "/";
        initializeStorage();
    }

    private void initializeStorage() {
        File dir = new File(storageDirectory);
        if (!dir.exists()) {
            if (dir.mkdirs()) {
                System.out.println("Initialized storage volume at: " + storageDirectory);
            } else {
                System.err.println("CRITICAL: Failed to create storage volume.");
            }
        }
    }

    public boolean saveChunk(String chunkId, byte[] data) {
        File chunkFile = new File(storageDirectory + chunkId + ".chunk");
        
        try (FileOutputStream fos = new FileOutputStream(chunkFile)) {
            fos.write(data);
            System.out.println("Successfully stored chunk: " + chunkId + " (" + data.length + " bytes)");
            return true;
        } catch (IOException e) {
            System.err.println("Disk I/O Error while saving chunk " + chunkId + ": " + e.getMessage());
            return false;
        }
    }

    // THE MISSING PIECE: The ability to read the bytes back off the disk!
    public byte[] readChunk(String chunkId) {
        File chunkFile = new File(storageDirectory + chunkId + ".chunk");
        if (chunkFile.exists()) {
            try {
                // We use Java NIO to read the entire chunk off the hard drive instantly
                return Files.readAllBytes(chunkFile.toPath());
            } catch (IOException e) {
                System.err.println("Failed to read chunk from disk: " + e.getMessage());
            }
        } else {
            System.err.println("Chunk not found on disk: " + chunkId);
        }
        return null;
    }
}