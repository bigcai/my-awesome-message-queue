package io.openmessaging.entrycode.util;

/**
 * Created by nbs on 2017/5/7.
 */
public class IdGenerator {
    private static long lastTime;
    private static long currentTime;


    public static synchronized long generatorId(){
        currentTime = System.nanoTime();
        while (lastTime >= currentTime){
            currentTime = System.nanoTime();
        }
        lastTime = currentTime;
        return currentTime;
    }
}
