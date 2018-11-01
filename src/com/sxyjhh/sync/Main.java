package com.sxyjhh.sync;

public class Main {
    public static void main(String[] args){
        while (true){
            System.out.println("执行时间："+System.currentTimeMillis());
            SyncDataUtil syncDataUtil = SyncDataUtil.getInstance();
            syncDataUtil.syncRunController();//执行同步

            //休眠时间
            try {
                Thread.sleep(3000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}

