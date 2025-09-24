package dk.ku.di.dms.vms.modb.common.runnable;

import static java.lang.Thread.sleep;

/**
 * Abstract class that provides common features for server classes
 */
public abstract class StoppableRunnable implements Runnable {

    private volatile boolean running;

    private volatile boolean paused;

    public StoppableRunnable() {
        // starts running as default
        this.running = true;
        this.paused = false;
    }

    public boolean isRunning() {
        return this.running;
    }
    public boolean isPaused() {
        return this.paused;
    }

    public void stop() {
        this.running = false;
    }
    public void pauseHandler(boolean pause) {
        this.paused = pause;
    }

    public void giveUpCpu(int sleepTime){
        if(sleepTime > 0){
            try { sleep(sleepTime); } catch (InterruptedException ignored) { }
            return;
        }
        Thread.yield();
    }

}
