package com.dfs.master;

import com.dfs.shared.models.ChunkInfo;
import com.dfs.shared.models.FileMetadata;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class MasterApiServer {

    private final int HTTP_PORT = 8080;
    // We will use a smaller chunk size (1 MB) for easy local testing. 
    // In a real system like HDFS, this is usually 64MB or 128MB.
    private final int CHUNK_SIZE = 1024 * 1024; 


    private final HealthMonitor healthMonitor;
    private final NamespaceMap namespaceMap;

    // ADD THIS CONSTRUCTOR
    public MasterApiServer(HealthMonitor healthMonitor, NamespaceMap namespaceMap) {
        this.healthMonitor = healthMonitor;
        this.namespaceMap= namespaceMap;
    }

    public void start() {
        try {
            // Create Java's lightweight built-in HTTP server
            HttpServer server = HttpServer.create(new InetSocketAddress(HTTP_PORT), 0);
            
            // Define the REST API endpoint for frontend uploads
            server.createContext("/upload", new UploadHandler());

            server.createContext("/download", new DownloadHandler());
            
            // Use a thread pool to handle multiple web requests at once
            server.setExecutor(Executors.newFixedThreadPool(10)); 
            server.start();
            System.out.println("Master API (Front Door) listening for web uploads on port " + HTTP_PORT);
            
        } catch (IOException e) {
            System.err.println("Failed to start HTTP API Server: " + e.getMessage());
        }
    }

    /**
     * This inner class handles the actual HTTP request from your React/Vercel frontend.
     */
    class UploadHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // 1. Handle CORS (Cross-Origin Resource Sharing)
            // This is required so your Vercel website is allowed to talk to your local Java server
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "POST, OPTIONS");

            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            // 2. Process the actual File Upload
            if ("POST".equals(exchange.getRequestMethod())) {
                System.out.println("Receiving file upload from frontend...");
                
                String query= exchange.getRequestURI().getQuery();
                String filename= (query!=null && query.contains("filename="))
                        ? query.split("filename=")[1].split("&")[0]
                        : "uploaded_file.bin";  // Fallback name just in case


                try (InputStream is = exchange.getRequestBody()) {
                    // Read the entire incoming file into memory (the buffer)
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    int nRead;
                    byte[] data = new byte[16384]; // Read in 16KB blocks
                    while ((nRead = is.read(data, 0, data.length)) != -1) {
                        buffer.write(data, 0, nRead);
                    }
                    
                    byte[] completeFileBytes = buffer.toByteArray();
                    System.out.println("File received. Total size: " + completeFileBytes.length + " bytes.");

                    // 3. THE CORE ALGORITHM: Split the file into chunks
                    splitIntoChunks(completeFileBytes, filename);

                    // Send a success response back to the browser
                    String response = "Upload and split successful!";
                    exchange.sendResponseHeaders(200, response.length());
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response.getBytes());
                    }
                } catch (Exception e) {
                    System.err.println("Upload failed: " + e.getMessage());
                    exchange.sendResponseHeaders(500, -1);
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }


    /**
     * Chops the large byte array into chunks and replicates them across multiple Data Nodes.
     */
    private void splitIntoChunks(byte[] fileBytes, String filename) throws IOException {
        java.util.List<String> activeNodes = healthMonitor.getActiveNodes();
        if (activeNodes.isEmpty()) {
            throw new IOException("CRITICAL: No Data Nodes are currently online!");
        }

        int totalChunks = (int) Math.ceil((double) fileBytes.length / CHUNK_SIZE);
        System.out.println("Splitting file into " + totalChunks + " chunks...");

        FileMetadata fileMetadata = new FileMetadata(filename, fileBytes.length);
        int offset = 0;
        
        // THE UPGRADE: How many copies of each chunk do we want?
        int REPLICATION_FACTOR = 2; 

        for (int i = 0; i < totalChunks; i++) {
            int length = Math.min(CHUNK_SIZE, fileBytes.length - offset);
            byte[] chunkData = new byte[length];
            System.arraycopy(fileBytes, offset, chunkData, 0, length);
            
            String chunkId = java.util.UUID.randomUUID().toString();
            ChunkInfo chunkInfo = new ChunkInfo(chunkId, i);

            // Shuffle the list of active nodes to randomize distribution
            java.util.Collections.shuffle(activeNodes);
            
            // Make sure we don't try to replicate 2 times if only 1 node is online!
            int nodesToPick = Math.min(REPLICATION_FACTOR, activeNodes.size());
            
            System.out.println("Replicating Chunk " + chunkId + " to " + nodesToPick + " distinct nodes.");

            // Loop through our selected nodes and throw the chunk to ALL of them
            for (int j = 0; j < nodesToPick; j++) {
                String targetNodeAddress = activeNodes.get(j);
                
                // Write it down on the Master's receipt!
                chunkInfo.addNodeLocation(targetNodeAddress); 

                String[] hostAndPort = targetNodeAddress.split(":");
                String ip = hostAndPort[0];
                int port = Integer.parseInt(hostAndPort[1]);

                try (java.net.Socket socket = new java.net.Socket(ip, port);
                    java.io.DataOutputStream out = new java.io.DataOutputStream(socket.getOutputStream())) {
                    
                    out.writeUTF("STORE");
                    out.writeUTF(chunkId); 
                    out.writeInt(length);  
                    out.write(chunkData);  
                    
                } catch (IOException e) {
                    System.err.println("Failed to send replica to " + targetNodeAddress + ": " + e.getMessage());
                }
            }
            
            fileMetadata.addChunk(chunkInfo);
            offset += length;
        }
        
        namespaceMap.put(filename, fileMetadata);
        System.out.println("Successfully recorded " + filename + " with full replication.");
    }


    /**
     * Reconstructs a file from chunks and streams it back to the client.
     */
    class DownloadHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

            if ("GET".equals(exchange.getRequestMethod())) {
                // Look for the filename in the URL (e.g., /download?filename=pom.xml)
                String query = exchange.getRequestURI().getQuery();
                String filename = query != null && query.contains("=") ? query.split("=")[1] : null;
                
                if (filename == null || !namespaceMap.contains(filename)) {
                    String error = "File not found in the cluster!";
                    exchange.sendResponseHeaders(404, error.length());
                    exchange.getResponseBody().write(error.getBytes());
                    exchange.getResponseBody().close();
                    return;
                }

                FileMetadata metadata = namespaceMap.get(filename);
                byte[] completeFile = new byte[(int) metadata.getFileSize()];
                int offset = 0;

                System.out.println("Starting download assembly for: " + filename);

                // Stitch the chunks together sequentially
                for (ChunkInfo chunk : metadata.getChunks()) {
                    boolean chunkRecovered = false;
                    
                    // THE FAULT TOLERANCE MAGIC: 
                    // We loop through the node locations. If Node A is dead, we instantly try Node B!
                    for (String nodeAddress : chunk.getNodeLocations()) {
                        String[] hostAndPort = nodeAddress.split(":");
                        try (java.net.Socket socket = new java.net.Socket(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
                            java.io.DataOutputStream out = new java.io.DataOutputStream(socket.getOutputStream());
                            java.io.DataInputStream in = new java.io.DataInputStream(socket.getInputStream())) {
                            
                            out.writeUTF("READ");
                            out.writeUTF(chunk.getChunkId());
                            out.flush();        // prevent infinite buffer for download
                            
                            int length = in.readInt();
                            if (length > 0) {
                                byte[] chunkData = new byte[length];
                                in.readFully(chunkData);
                                
                                // Paste the chunk bytes into the master file array
                                System.arraycopy(chunkData, 0, completeFile, offset, length);
                                offset += length;
                                chunkRecovered = true;
                                System.out.println("Successfully pulled chunk " + chunk.getChunkId() + " from " + nodeAddress);
                                 break; // We got the chunk, stop asking other nodes!
                            }
                        } catch (Exception e) {
                            System.err.println("Node " + nodeAddress + " failed to provide chunk. Trying backup replica...");
                        }
                    }
                    
                    if (!chunkRecovered) {
                        String error = "CRITICAL: File corrupted. All replicas of a chunk are offline.";
                        exchange.sendResponseHeaders(500, error.length());
                        exchange.getResponseBody().write(error.getBytes());
                        exchange.getResponseBody().close();
                        return;
                    }
                }
                
                // Success! Send the fully assembled file back to the user
                exchange.getResponseHeaders().add("Content-Disposition", "attachment; filename=\"" + filename + "\"");
                exchange.sendResponseHeaders(200, completeFile.length);
                try (java.io.OutputStream os = exchange.getResponseBody()) {
                    os.write(completeFile);
                }
                System.out.println("Download complete!");
            }
        }
    }
}