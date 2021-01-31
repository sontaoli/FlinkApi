package com.list.flink.pojo;

public class DspProcessPojo {
    @Override
    public String toString() {
        return "DspProcessPojo{" +
                "logTime='" + logTime + '\'' +
                ", traceId='" + traceId + '\'' +
                ", sendTime=" + sendTime +
                ", receveTime=" + receveTime +
                ", elapsedTime=" + elapsedTime +
                '}';
    }

    private String logTime;
    private String traceId;
    private int sendTime;
    private int receveTime;
    private int elapsedTime;

    public String getLogTime() {
        return logTime;
    }

    public void setLogTime(String logTime) {
        this.logTime = logTime;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public int getSendTime() {
        return sendTime;
    }

    public void setSendTime(int sendTime) {
        this.sendTime = sendTime;
    }

    public int getReceveTime() {
        return receveTime;
    }

    public void setReceveTime(int receveTime) {
        this.receveTime = receveTime;
    }

    public int getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(int elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

}
