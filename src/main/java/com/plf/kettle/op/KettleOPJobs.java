package com.plf.kettle.op;

import com.plf.kettle.base.KettleTutorial;
import org.apache.commons.io.FileUtils;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.delete.DeleteMeta;
import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;
import org.pentaho.di.trans.steps.switchcase.SwitchCaseMeta;
import org.pentaho.di.trans.steps.switchcase.SwitchCaseTarget;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

public class KettleOPJobs {

    public static void main(String[] args) {
        try {
            KettleEnvironment.init();
            KettleOPJobs kettleOPJobs =new KettleOPJobs();
            TransMeta transMeta = kettleOPJobs.generateJobs();
            String transXml = transMeta.getXML();
            String transName = "src/main/resources/etl/op_data_trans.ktr";
            File file = new File(transName);
            FileUtils.writeStringToFile(file, transXml, "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    public TransMeta generateJobs() throws KettleException {
        //设置serverTimeZone
        Properties properties = new Properties();
        properties.put("EXTRA_OPTION_MYSQL.serverTimezone","Asia/Shanghai");
        properties.put("EXTRA_OPTION_MYSQL.useSSL","true");

        TransMeta transMeta = new TransMeta();
        //设置转换名称
        transMeta.setName("tran_data_op_test");

        //添加数据源
        DatabaseMeta sourceDataMeta =
                new DatabaseMeta("test_pre", "MySQL", "Native","localhost", "test_pre", "3306", "root", "root");
        sourceDataMeta.setAttributes(properties);
        sourceDataMeta.setDBPort("3306");
        transMeta.addDatabase(sourceDataMeta);

        DatabaseMeta targetDataMeta =
                new DatabaseMeta("test", "MySQL", "Native","localhost", "test", "3306", "root", "root");
        targetDataMeta.setDBPort("3306");
        targetDataMeta.setAttributes(properties);
        transMeta.addDatabase(targetDataMeta);

        //registry是给每个步骤生成一个标识id
        PluginRegistry registry = PluginRegistry.getInstance();


        //表插入步骤
        //查询需要插入的表的
        TableInputMeta tableInputMetaMaxId = new TableInputMeta();

        String tableInputMetaMaxIdPluginId = registry.getPluginId(StepPluginType.class, tableInputMetaMaxId);

        tableInputMetaMaxId.setDatabaseMeta(targetDataMeta);
        tableInputMetaMaxId.setSQL("select coalesce(max(id),0) as id from test");
        StepMeta tableInputMetaMaxIdStep =
                new StepMeta(tableInputMetaMaxIdPluginId, "target_max_id",tableInputMetaMaxId);

        //给步骤添加在spoon工具中的显示位置
        tableInputMetaMaxIdStep.setDraw(true);
        tableInputMetaMaxIdStep.setLocation(100, 200);
        transMeta.addStep(tableInputMetaMaxIdStep);

        //表插入步骤
        //从原始表查询出数据
        TableInputMeta tableInputSearchData = new TableInputMeta();
        String tableInputSearchDataPluginId = registry.getPluginId(StepPluginType.class, tableInputSearchData);
        tableInputSearchData.setDatabaseMeta(sourceDataMeta);
        tableInputSearchData.setSQL("select * from test_pre where id > ?");
        //连接上一步
        tableInputSearchData.setLookupFromStep(tableInputMetaMaxIdStep);
        tableInputSearchData.setExecuteEachInputRow(true);
        tableInputSearchData.setVariableReplacementActive(true);
        // 新建步骤
        StepMeta tableInputSearchDataStep =
                new StepMeta(tableInputSearchDataPluginId, "source_search_data",tableInputSearchData);
        //给步骤添加在spoon工具中的显示位置
        tableInputSearchDataStep.setDraw(true);
        tableInputSearchDataStep.setLocation(300, 200);
        transMeta.addStep(tableInputSearchDataStep);

        transMeta.addTransHop(new TransHopMeta(tableInputMetaMaxIdStep, tableInputSearchDataStep));


        //插入更新步骤
        InsertUpdateMeta insertUpdateMeta = new InsertUpdateMeta();
        String insertUpdateMetaPluginId = registry.getPluginId(StepPluginType.class, insertUpdateMeta);
        insertUpdateMeta.setDatabaseMeta(targetDataMeta);
        insertUpdateMeta.setTableName("test");
        insertUpdateMeta.setKeyLookup(new String[]{"xh"});
        insertUpdateMeta.setKeyCondition(new String[]{"="});
        insertUpdateMeta.setKeyStream(new String[]{"xh"});
        insertUpdateMeta.setKeyStream2(new String[]{""});
        insertUpdateMeta.setUpdateLookup(new String[]{"xh","name","op","id"});
        insertUpdateMeta.setUpdateStream(new String[]{"xh","name","op","id"});
        insertUpdateMeta.setUpdate(new Boolean[]{true,true,true,true});
        insertUpdateMeta.setCommitSize("1000");
        StepMeta insertUpdateMetaStep =
                new StepMeta(insertUpdateMetaPluginId, "insert_update_data",insertUpdateMeta);
        //给步骤添加在spoon工具中的显示位置
        insertUpdateMetaStep.setDraw(true);
        insertUpdateMetaStep.setLocation(600, 100);
        transMeta.addStep(insertUpdateMetaStep);

        //删除步骤
        DeleteMeta deleteMeta = new DeleteMeta();
        String deleteMetaPluginId = registry.getPluginId(StepPluginType.class, deleteMeta);
        deleteMeta.setDatabaseMeta(targetDataMeta);
        deleteMeta.setTableName("test");

        deleteMeta.setKeyLookup(new String[]{"xh"});
        deleteMeta.setKeyCondition(new String[]{"="});
        deleteMeta.setKeyStream(new String[]{"xh"});
        deleteMeta.setKeyStream2(new String[]{""});
        deleteMeta.setCommitSize("1000");
        StepMeta deleteMetaStep =
                new StepMeta(deleteMetaPluginId, "delete_data",deleteMeta);
        //给步骤添加在spoon工具中的显示位置
        deleteMetaStep.setDraw(true);
        deleteMetaStep.setLocation(600, 300);
        transMeta.addStep(deleteMetaStep);

        //SwitchCase步骤
        SwitchCaseMeta switchCaseMeta = new SwitchCaseMeta();
        String switchCaseMetaPluginId = registry.getPluginId(StepPluginType.class, switchCaseMeta);

        switchCaseMeta.setFieldname("op");
        switchCaseMeta.setContains(true);
        switchCaseMeta.setCaseValueType(ValueMetaBase.getType("String"));
        // 默认步骤
        switchCaseMeta.setDefaultTargetStep(insertUpdateMetaStep);
        SwitchCaseTarget switchCaseTarget = new SwitchCaseTarget();
        switchCaseTarget.caseTargetStepname = "delete_data";
        switchCaseTarget.caseValue="delete";
        switchCaseMeta.setCaseTargets(Arrays.asList(switchCaseTarget));

        StepMeta switchCaseMetaStep =
                new StepMeta(switchCaseMetaPluginId, "switch_op",switchCaseMeta);
        //给步骤添加在spoon工具中的显示位置
        switchCaseMetaStep.setDraw(true);
        switchCaseMetaStep.setLocation(500, 200);
        transMeta.addStep(switchCaseMetaStep);


        transMeta.addTransHop(new TransHopMeta(tableInputSearchDataStep, switchCaseMetaStep));
        transMeta.addTransHop(new TransHopMeta(switchCaseMetaStep, insertUpdateMetaStep));
        transMeta.addTransHop(new TransHopMeta(switchCaseMetaStep, deleteMetaStep));

        return transMeta;
    }

}
