package io.openmessaging.entrycode.interfaces;

/**
 * Created by nbs on 2017/5/7.
 */
public interface FileInit {
    void initFilesIfIndexFileNotExist();
    int initQueueDataFile();
    int initMessageDataFile();
    
}
