package uk.ac.ed.acp.cw2.data;

public class ProcessMessagesRequest {
    private String readTopic;
    private String writeQueueGood;
    private String writeQueueBad;
    private int messageCount;

    // Getters and setters
    public String getReadTopic() { return readTopic; }
    public void setReadTopic(String readTopic) { this.readTopic = readTopic; }
    public String getWriteQueueGood() { return writeQueueGood; }
    public void setWriteQueueGood(String writeQueueGood) { this.writeQueueGood = writeQueueGood; }
    public String getWriteQueueBad() { return writeQueueBad; }
    public void setWriteQueueBad(String writeQueueBad) { this.writeQueueBad = writeQueueBad; }
    public int getMessageCount() { return messageCount; }
    public void setMessageCount(int messageCount) { this.messageCount = messageCount; }
}