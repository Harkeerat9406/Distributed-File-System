package com.dfs.master;

import com.dfs.shared.models.FileMetadata;
import java.util.concurrent.ConcurrentHashMap;

public class NamespaceMap {
    
    // The central registry mapping a file path to its metadata
    private final ConcurrentHashMap<String, FileMetadata> fileSystem;

    public NamespaceMap() {
        this.fileSystem = new ConcurrentHashMap<>();
    }

    
    // Adds a new file to the system. 
    // Returns true if successful, false if the file already exists.

    public boolean addFile(String filePath, FileMetadata metadata) {
        // putIfAbsent is an atomic operation. It prevents two threads 
        // from creating the same file at the exact same millisecond.
        return fileSystem.putIfAbsent(filePath, metadata) == null;
    }


    // Retrieves the metadata for a specific file.
    // Returns null if the file does not exist.
    public FileMetadata getFile(String filePath) {
        return fileSystem.get(filePath);
    }

    // Checks if file is in the system
    public boolean fileExists(String filePath) {
        return fileSystem.containsKey(filePath);
    }
    
    // Deletes file from registry
    public void deleteFile(String filePath) {
        fileSystem.remove(filePath);
    }
}