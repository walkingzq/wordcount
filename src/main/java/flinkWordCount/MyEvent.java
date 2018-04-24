package flinkWordCount;

/**
 * Create by Zhao Qing on 2018/4/24
 * flink最终输出的数据格式
 */
public class MyEvent {
    private long inTime;

    private long outTime;

    private int delay;

    public MyEvent(){}

    public MyEvent(long inTime, long outTime, int delay) {
        this.inTime = inTime;
        this.outTime = outTime;
        this.delay = delay;
    }

    public long getInTime() {
        return inTime;
    }

    public void setInTime(long inTime) {
        this.inTime = inTime;
    }

    public long getOutTime() {
        return outTime;
    }

    public void setOutTime(long outTime) {
        this.outTime = outTime;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }
}
