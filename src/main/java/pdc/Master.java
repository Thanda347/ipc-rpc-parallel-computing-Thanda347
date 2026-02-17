package pdc;


import java.io.ByteArrayInputStream;
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
import java.util.Map;



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
        int cols = data[0].length;

        taskQueue.clear();
        results.clear();
        completedTasks.set(0);
        totalTasks = 0;

        int chunkSize = (int) Math.ceil ((double) totalRows / workerCount);


        for (int startRow  = 0; startRow < totalRows; startRow += chunkSize) {
            int endRow = Math.min(startRow + chunkSize, totalRows);
            int rowsInChunk = endRow -startRow;


            int [] taskChunk = new int[rowsInChunk] [cols];
            for ( int i = 0; i < rowsInChunk; i++) {
                taskChunk[i] = Arrays.copyOf(data[startRow + i],  cols);

            }
              TaskInfo taskInfo = new TaskInfo(totalTasks, startRow, endRow, taskChunk);

            taskQueue.add(taskInfo);
            totalTasks++;
        }

            for (WorkerInfo worker : workers.values()) {
                systemThreads.submit(() -> {
                    try {

                        TaskInfo task = taskQueue.poll();

                        if (task != null) {
                        Message taskMsg = new Message();
                        taskMsg.type = Message.Task;
                        taskMsg.sender = "Master";
                        taskMsg.taskId = task.taskId;

                        ByteArrayOutputStream byteArrayOutStr = new ByteArrayOutputStream();
                        DataOutputStream dataOutStr = new DataOutputStream(byteArrayOutStr);

                        dataOutStr.writeInt(task.startRow);
                        dataOutStr.writeInt(task.endRow);
                        dataOutStr.writeInt(cols);

                        for (int r = 0; r < task.data.length; r++) {
                            for (int c = 0; c < cols; c++){
                            dataOutStr.writeInt(task.data[r][c]);
                        }
                    }
                        dataOutStr.flush();
                        taskMsg.payload = byteArrayOutStr.toByteArray();
                        
                        synchronized (worker.out) {
                            worker.out.write(taskMsg.pack());
                            worker.out.flush();
                        }
                         System.out.println("Sent task" + task.taskId + "to worker");
                    }
                } catch (IOException e) {
                        System.err.println("Failed to send task: " + e.getMessage());
                }
            });
        }
                        
            long startWait = System.currentTimeMillis();
            long timeout = 30000;


             while (completedTasks.get() < totalTasks) {
                if (System.currentTimeMillis() - startWait > timeout) {
                    System.err.println("Timeout waiting for tasks");
                 break;
                }

                try {
                       Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            int [][] result = new int [totalRows] [cols];

            for (int i = 0; i < totalTasks; i++){
                int [][] taskResult = results.get(i);
                if (taskResult != null) {
                    for (Map.Entr<Integer, int[][]> entry : results.entrySet()) {
                        int taskId = entry.getKey();
                        int [][] taskData = entry.getValue();

                        int taskStartRow = taskId * chunkSize;
                        for (int r = 0; r < taskData.length; r++ ) {
                            if (taskStartRow + r <totalRows) {
                                result[taskStartRow + r] = taskData[r];
                            }
                        }
                    }
                } else {
                    System.err.println("Missing result for task" + i);
                }
            }
            return result;
    } 
        
    private static class TaskInfo {
        int taskId;
        int startRow;
        int endRow;
        int [][] data;

        TaskInfo(int id, int start, int end, int [][] taskData) {
            this.taskId = id;
            this.startRow = start;
            this.endRow = end;
            this.data = taskData;
        }
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
                    if (msg == null) {
                        System.out.println("Worker" + info.workerId + "closed connection");
                        break;
                    }

                    if (msg.type.equals(Message.Heartbeat)) {
                        info.lastHeartbeat = System.currentTimeMillis();
                    } else if (msg.type.equals(Message.Response)) {

                        try {
                            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(msg.payload);
                            DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

                             int startRow = dataInputStream.readInt();
                    int endRow = dataInputStream.readInt();
                    int cols = dataInputStream.readInt();
                    
                    int rows = endRow - startRow;
                    int[][] resultChunk = new int[rows][cols];
                    
                    for (int r = 0; r < rows; r++) {
                        for (int c = 0; c < cols; c++) {
                            resultChunk[r][c] = dataInputStream.readInt();
                        }
                    }
                     
                    results.put(msg.taskId, resultChunk);
                    completedTasks.incrementAndGet();
                    
                    System.out.println("Received result for task " + msg.taskId + " from worker " + info.workerId);
                    
                } catch (IOException e) {
                    System.err.println("Failed to parse result: " + e.getMessage());
                        
            }
        }
    }
}catch (IOException e) {
        System.err.println("Worker connection lost: " + e.getMessage());
    } finally {
        workers.remove(info.workerId);
        System.out.println("Worker " + info.workerId + " removed");
    }
}
    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        Thread monitor = new Thread(() -> {
            while (listening) {
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
                    Thread.sleep(100);
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
