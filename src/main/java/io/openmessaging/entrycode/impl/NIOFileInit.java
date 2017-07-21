package io.openmessaging.entrycode.impl;

import io.openmessaging.entrycode.interfaces.FileInit;
import io.openmessaging.entrycode.util.Constant;
import io.openmessaging.entrycode.util.FileUtil;

import java.io.File;
import java.util.List;

/**
 * Created by nbs on 2017/5/7.
 */
public class NIOFileInit implements FileInit {
    private String path;

    NIOFileInit(String path){
        this.path = path;
        File file = new File(this.path);
        if( !file.exists() ){
        	file.mkdir();
        }
    }

    public boolean isQueueIndexFileExist(){
        List<File> fileList = FileUtil.getFilesByPathAndFilterByName(path,Constant.DEFAULT_QUEUE_INDEX_NAME);
        if(fileList == null || fileList.size() == 0){
            return false;
        }
        return true;
    }

    public void initFilesIfIndexFileNotExist(){
        if(!isQueueIndexFileExist()){
            initQueueIndexFile();
            initQueueDataFile();
            initMessageDataFile();
        }
    }

    public void initQueueIndexFile(){
        createFileByName(Constant.DEFAULT_QUEUE_INDEX_NAME);
    }
    public int initQueueDataFile(){
        return createFileByName(Constant.DEFAULT_QUEUE_DATA_NAME);
    }
    public int initMessageDataFile(){
        return createFileByName(Constant.DEFAULT_MESSAGE_DATA_NAME);
    }

    private int createFileByName(String name){
        int id = FileUtil.getMaxIdByPath(path, name) + 1;
        FileUtil.createFile(path + id+ "-"+name);
        return id;
    }
}
