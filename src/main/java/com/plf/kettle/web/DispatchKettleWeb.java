package com.plf.kettle.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.filerep.KettleFileRepository;
import org.pentaho.di.repository.filerep.KettleFileRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.www.SlaveServerJobStatus;
import org.pentaho.di.www.SlaveServerTransStatus;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class DispatchKettleWeb {

    private final String kettleXMLDirectory = "C:\\Data\\Soft\\Kettle\\kettlexml";

    private SlaveServer slaveServer = null;

    /**
     * 初始化Kettle服务器
     */
    @BeforeEach
    public void init() {
        try {
            KettleEnvironment.init();
            slaveServer = new SlaveServer();
            slaveServer.setName("slave1");
            slaveServer.setHostname("localhost");
            slaveServer.setPort("8081");
            slaveServer.setUsername("cluster");
            slaveServer.setPassword("cluster");
        }catch (Exception e){
            System.out.println("启动失败,错误是===>"+e.getMessage());
        }
    }




    /**
     * 将KJB内容发布到kettle服务器上执行。
     * @param jobName
     * @return
     * @throws Exception
     */
    public String startJob(String jobName) throws Exception{
        try {
            String id = "", name = "", description = "无描述";
            // Job文件存放路径
            String localKettleBaseDirectory =  "/KettleHome";
            KettleFileRepositoryMeta repInfo = new KettleFileRepositoryMeta(id,
                    name, description, localKettleBaseDirectory);
            // 文件形式的资源库
            KettleFileRepository rep = new KettleFileRepository();
            rep.init(repInfo);
            RepositoryDirectoryInterface tree = rep.loadRepositoryDirectoryTree();
            RepositoryDirectoryInterface rd = tree.findDirectory("");

            JobMeta jobMeta = rep.loadJob(jobName, rd, null, null); // jobName = jobName[.kjb]

//			JobMeta jobMeta = new JobMeta(inputStream, null, null);
            JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
            jobExecutionConfiguration.setRemoteServer(slaveServer);

            jobExecutionConfiguration.setExecutingLocally(false);
            jobExecutionConfiguration.setExecutingRemotely(true);
            jobExecutionConfiguration.setPassingExport(false);
            jobExecutionConfiguration.setRepository(rep);
            //设置全局参数 此处的Variables 会被JobMeta的Variables覆盖 详见源代码
//			jobExecutionConfiguration.setVariables(variablesMap);
            return Job.sendToSlaveServer(jobMeta, jobExecutionConfiguration, null, null);
        } catch (KettleXMLException e) {
            return null;
        }
    }
    /**
     * 开始job作业
     * @param in kjb文本
     * @return kettle执行kjb的作业Id
     */
    public String startJob(InputStream in) throws Exception{
        try {
            // 配置任务信息
            JobMeta meta = new JobMeta(in, null,null);
            JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
            jobExecutionConfiguration.setRemoteServer(slaveServer);// 配置远程服务
            // 需要判断是否在执行,否则每执行一次就会产生一个lastCarteObjectId
            String carteObjectId = Job.sendToSlaveServer(meta, jobExecutionConfiguration, null, null);
            return carteObjectId;
        } catch (Exception e) {
            throw e;
        }
    }
    /**
     * 开始job作业  带环境变量
     * @param in kjb文本
     * @return kettle执行kjb的作业Id
     */
    public String startJob(InputStream in, Map<String, String> variables, SlaveServer server) throws Exception{
        try {
            // 配置任务信息
            JobMeta meta = new JobMeta(in, null, null);
            JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
            jobExecutionConfiguration.setRemoteServer(server);// 配置远程服务
            jobExecutionConfiguration.setVariables(variables);// 设置环境变量
            // 需要判断是否在执行,否则每执行一次就会产生一个lastCarteObjectId
            String carteObjectId = Job.sendToSlaveServer(meta, jobExecutionConfiguration, null, null);
            JobEntryCopy startPoint =  meta.findJobEntry(JobMeta.STRING_SPECIAL_START);
            JobEntrySpecial jes = (JobEntrySpecial) startPoint.getEntry();
            boolean isRepeat = jes.isRepeat();	//判断是否为任务是否设置了重复
            System.out.println("任务是否设置了重复:"+isRepeat);
            return carteObjectId;
        } catch (Exception e) {
            throw e;
        }
    }


    /**
     * 获取作业的状态
     */
    @Test
    public void getJobStatus(){
        try {
            //只能获取运行的任务
            List<SlaveServerJobStatus> jobStatusList =  slaveServer.getStatus().getJobStatusList();

            //获取作业的状态
            jobStatusList.forEach(
                    x-> {
                        //获取状态
                        //System.out.println(x.getStatusDescription());
                        System.out.println(x.getId());
                        System.out.println(x.getJobName());
                        // System.out.println(x.getResult()); //null
                    }
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getJobLog() throws Exception {
        System.out.println(slaveServer.getJobStatus("insert",
                "99102801-c385-4550-9c1f-9f7d29ddf471",
                0).getLoggingString());
    }

    /**
     * 停止作业
     */
    @Test
    public void stopJob(){
        try {
            slaveServer.stopJob("",
                    "");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除作业
     */
    @Test
    public void removeJob(){
        try {
            slaveServer.removeJob("",
                    "");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //===================================== Trans 转换 =====================================
    /**
     * 执行转换
     * @param ktrInputStream
     * @return
     * @throws Exception
     */
    public String startTrans(InputStream ktrInputStream) throws Exception {
        try {
            // 配置任务信息
            TransMeta meta = new TransMeta(ktrInputStream,
                    null,
                    false,
                    null,
                    null);
            TransExecutionConfiguration transExecutionConfiguration = new TransExecutionConfiguration();
            transExecutionConfiguration.setRemoteServer(slaveServer);// 配置远程服务
            // 需要判断是否在执行,否则每执行一次就会产生一个lastCarteObjectId
            String carteObjectId = Trans.sendToSlaveServer(meta, transExecutionConfiguration,
                    null, null);
            return carteObjectId;
        } catch (Exception e) {
            System.out.println("执行任务失败,错误是===>"+e.getMessage());
            return null;
        }
    }

    /**
     * 获取转换日志
     */
    @Test
    public void getTransLog(){
        try {
            SlaveServerTransStatus slaveServerTransStatus =  slaveServer
                    .getTransStatus("tran_data_test",
                            "35f6a22b-92f3-4574-9a75-0bdb7cd7bb10",
                            2);

            System.out.println(slaveServerTransStatus.getLoggingString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取Trans的状态
     */
    @Test
    public void getTransStatus(){
        try {
            List<SlaveServerTransStatus> transStatusList = slaveServer.getStatus().getTransStatusList();
            //获取转换的
            transStatusList.forEach(
                    x-> System.out.println(x.getStatusDescription())
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 执行Trans
     */
    @Test
    public void executeOnceTrans(){
        try {
            InputStream ktrInputStream = new FileInputStream("update_insert_trans.ktr");
            startTrans(ktrInputStream);
        } catch (FileNotFoundException e) {
            System.out.println("读取文件失败,错误是===>"+e.getMessage());
            e.printStackTrace();
        } catch (Exception e){
            System.out.println("执行KJB文件失败,错误是===>"+e.getMessage());
        }
    }

    /**
     * 启动Job
     */
    @Test
    public void executeJob(){
        try {
            InputStream ktrInputStream = new FileInputStream("C:\\Data\\Soft\\Kettle\\kettlexml\\insert.kjb");
            System.out.println(startJob(ktrInputStream));
        } catch (FileNotFoundException e) {
            System.out.println("读取文件失败,错误是===>"+e.getMessage());
            e.printStackTrace();
        } catch (Exception e){
            System.out.println("执行KJB文件失败,错误是===>"+e.getMessage());
        }
    }
}
