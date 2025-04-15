package uk.ac.ed.acp.cw2.data;

public class TransformMessagesRequest {
    private String readQueue;
    private String writeQueue;
    private int messageCount;

    // Getters and setters
    public String getReadQueue() { return readQueue; }
    public void setReadQueue(String readQueue) { this.readQueue = readQueue; }
    public String getWriteQueue() { return writeQueue; }
    public void setWriteQueue(String writeQueue) { this.writeQueue = writeQueue; }
    public int getMessageCount() { return messageCount; }
    public void setMessageCount(int messageCount) { this.messageCount = messageCount; }
}