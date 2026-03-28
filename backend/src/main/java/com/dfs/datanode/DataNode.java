package com.dfs.datanode;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.PrintWriter;

public class DataNode {
    
    private final int myPort; 
    private final String nodeId;
    private final String masterHost = "127.0.0.1";
    private final int masterPort = 9000;
    
    private final ChunkStorage storage; 
    private Socket heartbeatSocket;
    private PrintWriter out;

    public DataNode(String portStr) {
        this.myPort = Integer.parseInt(portStr);
        this.nodeId = "node_" + myPort;
        this.storage = new ChunkStorage(this.nodeId); 
    }

    public void start() {
        System.out.println("Starting Data Node: " + nodeId + " on port " + myPort);
        try {
            heartbeatSocket = new Socket(masterHost, masterPort);
            out = new PrintWriter(heartbeatSocket.getOutputStream(), true);
            startHeartbeat();
            startChunkListener();
            while (!heartbeatSocket.isClosed()) {
                Thread.sleep(1000); 
            }
        } catch (Exception e) {
            System.err.println("Data Node disconnected: " + e.getMessage());
        }
    }

    private void startChunkListener() {
        Thread listenerThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(myPort)) {
                while (true) {
                    try (Socket masterConnection = serverSocket.accept();
                         DataInputStream in = new DataInputStream(masterConnection.getInputStream())) {
                        
                        String command = in.readUTF();
                        
                        if ("STORE".equals(command)) {
                            String chunkId = in.readUTF();
                            int length = in.readInt();
                            byte[] data = new byte[length];
                            in.readFully(data);
                            storage.saveChunk(chunkId, data);
                            
                        } else if ("READ".equals(command)) {
                            String chunkId = in.readUTF();
                            byte[] data = storage.readChunk(chunkId);
                            java.io.DataOutputStream out = new java.io.DataOutputStream(masterConnection.getOutputStream());
                            if (data != null) {
                                out.writeInt(data.length);
                                out.write(data);
                            } else {
                                out.writeInt(-1); 
                            }
                            out.flush(); 
                            
                        } else if ("KILL".equals(command)) {
                            // THE ASSASSINATION PROTOCOL
                            System.out.println("CRITICAL: Received KILL command from Master. Shutting down immediately.");
                            System.exit(0); 
                        }
                        
                    } catch (IOException e) {
                        System.err.println("Error processing Master request: " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                System.err.println("Critical failure starting chunk listener: " + e.getMessage());
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    private void startHeartbeat() {
        Thread heartbeatThread = new Thread(() -> {
            while (!heartbeatSocket.isClosed()) {
                try {
                    out.println("HEARTBEAT " + myPort);
                    Thread.sleep(5000); 
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java DataNode <port_number>");
            System.exit(1);
        }
        DataNode node = new DataNode(args[0]);
        node.start();
    }
}
