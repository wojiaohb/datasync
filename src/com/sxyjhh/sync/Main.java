package com.sxyjhh.sync;

public class Main {
    public static void main(String[] args){
        while (true){
            SyncDataUtil syncDataUtil = SyncDataUtil.getInstance();
            syncDataUtil.syncRunController();//执行同步

            //休眠一分钟
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

