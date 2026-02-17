package pdc;


import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Arrays;



/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {

    class WorkerInfo {
        Socket socket;
        DataInputStream in;
        DataOutputStream out;
        long lastHeartbeat;
        int workerId;
    }

    ConcurrentHashMap<Integer, WorkerInfo> workers = new ConcurrentHashMap<>();


    private ServerSocket serverSocket;
    private LinkedBlockingQueue<int[]> taskQueue = new LinkedBlockingQueue<>();
    private ConcurrentHashMap<Integer, int[][]> results = new ConcurrentHashMap<>();
    private AtomicInteger completedTasks = new AtomicInteger(0);
    private int totalTasks = 0;
    private volatile boolean listening = true;



    private final ExecutorService systemThreads = Executors.newCachedThreadPool();

    /**
     * Entry point for a distributed computation.
     * 
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     * 
     * @param operation A string descriptor of the matrix operation (e.g.
     *                  "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (data == null || data.length == 0 || data[0] == null || workerCount <=0) {
          return null;


        }
        if (workerCount > data.length) {
            workerCount = data.length;
        }
        // HINT: Think about how MapReduce or Spark handles 'Task Reassignment'.
        
        int totalRows = data.length;
        int chunkSize = totalRows/workerCount;
        for (int startRow  = 0; startRow < totalRows; startRow += chunkSize) {
            int endRow = Math.min(startRow + chunkSize, totalRows);
            int [] taskData = Arrays.copyOfRange(data[startRow], 0, data[startRow].length);
            taskQueue.add(taskData);
            totalTasks++;
        }

            for (WorkerInfo worker : workers.values()) {
                systemThreads.submit(() -> {
                    try {

                        int[] currenttask = taskQueue.poll();
                        if (currenttask != null) {
                        Message taskMsg = new Message();
                        taskMsg.type = Message.Task;
                        taskMsg.sender = "Master";

                        ByteArrayOutputStream byteArrayOutStr = new ByteArrayOutputStream();
                        DataOutputStream dataOutStr = new DataOutputStream(byteArrayOutStr);
                        for (int value : currenttask) {
                            dataOutStr.writeInt(value);
                        }
                        dataOutStr.flush();
                        taskMsg.payload = byteArrayOutStr.toByteArray();
                        
                        synchronized (worker.out) {
                            worker.out.write(taskMsg.pack());
                            worker.out.flush();
                        }
                    }
                } catch (IOException e) {
                        System.err.println("Failed to send task: " + e.getMessage());
                }
            });
        }
                        
            long startWait = System.currentTimeMillis();
             while (completedTasks.get() < totalTasks) {
                if (System.currentTimeMillis() - startWait > 10000) break;
                try {
                       Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            int [][] result = new int [totalRows] [data[0].length];
            return result;
    } 
            

    /**
     * Start the communication listener.
     * Use your custom protocol designed in Message.java.
     */
    
    public void listen(int port) throws IOException {
        reconcileState();
        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(1000);
        System.out.println("Master listening on port " + port);
        
        systemThreads.submit(new Runnable()  {
           public void run(){
                while (listening){
                    try{
                       Socket socket = serverSocket.accept();
                       handleWorkerConnection(socket);
                   } catch (java.net.SocketTimeoutException e) {
                     continue;

                   } catch (IOException e) {
                       System.err.println("error accepting connection: " + e.getMessage());
                     break;
               }
            }
       }
    });
}

private void handleWorkerConnection(Socket socket){
    try{        
        DataInputStream in = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                
                
        Message response = Message.unpack(in);
                
        Message ackMsg = new Message();
        ackMsg.type = Message.ack;
        out.write(ackMsg.pack());
        out.flush();

        WorkerInfo info = new WorkerInfo();
        info.socket = socket;
        info.in = in;
        info.out = out;
        info.lastHeartbeat = System.currentTimeMillis();

        workers.put(response.workerId, info);
        

        systemThreads.submit(() -> handleWorkerMessages(info));

    } catch (IOException e) {
        System.err.println("Error: " + e.getMessage());
    }
}
        private void handleWorkerMessages(WorkerInfo info){
             try{
                while(listening) {
                    Message msg = Message.unpack(info.in);
                    if (msg == null) break;
                    if (msg.type.equals(Message.Heartbeat)) {
                        info.lastHeartbeat = System.currentTimeMillis();
                    } else if (msg.type.equals(Message.Response)) {
                        completedTasks.incrementAndGet();
                    }
                }
            } catch (IOException e) {
                System.err.println("Worker connection lost:" + e.getMessage());
            } finally {
                workers.remove(info.workerId);
            }
        }
    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        Thread monitor = new Thread(() -> {
            while (true) {
                for (ConcurrentHashMap.Entry<Integer, WorkerInfo> entry : workers.entrySet()) {
                    int workerId = entry.getKey();
                    WorkerInfo info = entry.getValue();

                    System.out.println("Current time: " + System.currentTimeMillis());
                    System.out.println("Last heartbeat: " + info.lastHeartbeat);
                    if (System.currentTimeMillis() - info.lastHeartbeat > 5000) {
                        System.out.println("Worker " + workerId + " is dead.");
                        workers.remove(workerId);
                    }

                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }); 
        monitor.setDaemon(true);
        monitor.start();
    }
}
