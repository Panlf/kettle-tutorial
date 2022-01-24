package com.plf.kettle.web;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.KettleEnvironment;

public class KettleWeb {
    public static void main(String[] args) throws Exception {
        //nohup ./carte.sh 0.0.0.0  8081 &   // Linux 下面的启动命令
        // ./carte.bat 0.0.0.0  8081    //Windows下面的启动命令
        KettleEnvironment.init();
        SlaveServer slaveServer = new SlaveServer();
        slaveServer.setName("slave1");
        slaveServer.setHostname("localhost");
        slaveServer.setPort("8081");
        slaveServer.setUsername("cluster");
        slaveServer.setPassword("cluster");
        // CPU 核数
        System.out.println(slaveServer.getStatus().getCpuCores());
    }
}
