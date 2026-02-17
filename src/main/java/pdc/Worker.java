package pdc;

import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    private Socket socket;
    private int workerId;
    private DataInputStream in;
    private DataOutputStream out;
    private ExecutorService threadPool;
    private byte[] buffer;
    private volatile boolean running;

 
    public Worker(){

        this.workerId = 0;
        this.threadPool = Executors.newCachedThreadPool();
        this.buffer = new byte[1024];
        this.running = true;


    }



    public Worker(Socket socket) {
        this.socket = socket;
        this.workerId = 0;
        this.threadPool = Executors.newCachedThreadPool(); 
        this.buffer = new byte[1024]; 
        this.running = true;
        this.in = null;
        this.out = null;
    }
    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port)  throws IOException{
     
        try{
        this.socket = new Socket (masterHost, port);
        this.out = new DataOutputStream(this.socket.getOutputStream());
        this.in = new DataInputStream(this.socket.getInputStream());

        
       Message msg = new Message();
        msg.type = Message.Register;
        msg.sender = String.valueOf(workerId);
        msg.workerId = workerId;

        byte[] packedMsg = msg.pack();
        out.write(packedMsg);
        out.flush();
       
        Message response = Message.unpack(in);
        if (response.type.equals(Message.ack)) {
            System.out.println("Registration successful");
        }
        Thread heartbeat = new Thread(() -> {
            while (running){
                       try {
                    Message hbMsg = new Message();
                    hbMsg.type = Message.Heartbeat;
                    hbMsg.sender = String.valueOf(workerId);
                    hbMsg.workerId = workerId;
                    out.write(hbMsg.pack());
                    out.flush();
                    Thread.sleep(2000); 
                } catch (IOException | InterruptedException e) {
                    System.err.println("Heartbeat failed: " + e.getMessage());
                    running = false;
                }
            }
        });
        heartbeat.setDaemon(true);
        heartbeat.start();
     
        
        while (running) {
            try{
                Message taskMsg =Message.unpack(in);
                if (taskMsg.type.equals(Message.Task)) {
                    threadPool.submit(() -> execute(taskMsg)); 
            } else if (taskMsg.type.equals(Message.Shutdown)) {
                running = false;
                break;
            } 
            
        } catch (IOException e) {
            System.err.println("Lost connection: " + e.getMessage());
            running = false;
            break;
        }
    }
    threadPool.shutdown();
    socket.close();
    } catch (IOException e) {
        System.err.println("Failed to connect to Master: " + e.getMessage());
    }  
}
/**
     * 
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        try{

        } catch (Exception e) {
            System.err.println("Error executing task: " + e.getMessage());
        }
    } 

    public void execute( Message Task) {
        long startTime = System.currentTimeMillis();
        System.out.println("Starting execution of task " + startTime);
        ByteArrayInputStream byteArrayInpStr = new ByteArrayInputStream(Task.payload);
        DataInputStream dataInpStr = new DataInputStream(byteArrayInpStr);

        try{
            int startRow = dataInpStr.readInt();
            int endRow = dataInpStr.readInt();
            int colsA = dataInpStr.readInt();
            int colsB = dataInpStr.readInt();
           

            int rows = endRow -startRow;
            double[][] A = new double[rows][colsA];

            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < colsA; c++) {
                    A[r][c] = dataInpStr.readDouble();
                }
            }

            double[][] B = new double[colsA][colsB];
            for (int r = 0; r < colsA; r++) {
                for (int c = 0; c < colsB; c++) {
                    B[r][c] = dataInpStr.readDouble();
                }
            }
             double [][] result = new double[rows] [colsB];
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < colsB; c++) {
                    for (int k = 0; k < colsA; k++) {
                        result[r][c]+= A[r][k] * B[k][c];
                    }
                }
            }
            long endTime = System.currentTimeMillis();
            System.out.println("Task" + Task.taskId + "done in:" +(endTime- startTime) + "ms");
             Message resultMsg = new Message();
             resultMsg.type = Message.Response;
             resultMsg.sender = String.valueOf(workerId);
             resultMsg.workerId = workerId;
             resultMsg.taskId = Task.taskId;

             ByteArrayOutputStream byteArrayOutStr = new ByteArrayOutputStream();
             DataOutputStream dataOutStr = new DataOutputStream(byteArrayOutStr);

             dataOutStr.writeInt(startRow);
             dataOutStr.writeInt(endRow);
             dataOutStr.writeInt(colsB);

             for (int r = 0; r < rows; r++) {
                for (int c = 0; c < colsB; c++) {
                    dataOutStr.writeDouble(result[r][c]);
                }
            }
            dataOutStr.flush();
            resultMsg.payload = byteArrayOutStr.toByteArray();

             synchronized (out) {
                out.write(resultMsg.pack());
                out.flush();
             }
        } catch (IOException e) {
            System.err.println("Error executing task: " + e.getMessage());
        }
    }   
}
