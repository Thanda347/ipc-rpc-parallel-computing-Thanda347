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

        this.workerId = (int) (System.currentTimeMillis() % 10000) + (int)(Math.random() * 1000);

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
        
        } else {
            System.err.println("Registration failed");
            return;
        }
          
        Thread heartbeat = new Thread(() -> {
            while (running){
                try {
                    Message hbMsg = new Message();
                    hbMsg.type = Message.Heartbeat;
                    hbMsg.sender = String.valueOf(workerId);
                    hbMsg.workerId = workerId;

                    synchronized (out) {
                    out.write(hbMsg.pack());
                    out.flush();
                    }

                    Thread.sleep(1000); 
                } catch (IOException | InterruptedException e) {
                    System.err.println("Heartbeat failed: " + e.getMessage());
                    running = false;
                    break;
                }
            }
        });
        heartbeat.setDaemon(true);
        heartbeat.start();
     
        
        while (running) {
            try{
                Message taskMsg =Message.unpack(in);
                if (taskMsg == null) {
                    System.out.println("Connection closed by master");
                    break;
                }

             if (taskMsg.type.equals(Message.Task)) {
                final Message finalTaskMsg = taskMsg;
                threadPool.submit(() -> execute(finalTaskMsg));
             }else if (taskMsg.type.equals(Message.Shutdown)) {
                System.out.println("Received shutdown command");
             
                running = false;
                break;
            } 
            
        }

         threadPool.shutdown();
         socket.close();
         System.out.println("Worker" + workerId + "shut down");
    }
         catch (IOException e) {
            System.err.println("Lost connection: " + e.getMessage());
            throw e;
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
        System.out.println("Worker " + workerId + "starting task" + task.taskId);

        ByteArrayInputStream byteArrayInpStr = new ByteArrayInputStream(Task.payload);
        DataInputStream dataInpStr = new DataInputStream(byteArrayInpStr);

        try{
            int startRow = dataInpStr.readInt();
            int endRow = dataInpStr.readInt();
            int cols = dataInpStr.readInt();
           

            int rows = endRow -startRow;
            
             
            int[][] matrixData = new int[rows][cols];
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    matrixData[r][c] = dataInpStr.readInt();
                    
                }
            }

            int[][] result = new int[rows][cols];
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    result[r][c] = matrixData[r][c] * 2;
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
            dataOutStr.writeInt(cols);

             for (int r = 0; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    dataOutStr.writeInt(result[r][c]);
                }
            }
            dataOutStr.flush();
            resultMsg.payload = byteArrayOutStr.toByteArray();

             synchronized (out) {
                out.write(resultMsg.pack());
                out.flush();
             }

             System.out.println("sent result for task" + task.taskId);


        } catch (IOException e) {
            System.err.println("Error executing task: " + e.getMessage());
        }
    }   
}
