package com.plf.kettle.op;

import org.pentaho.di.trans.steps.delete.DeleteMeta;
import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;
import org.pentaho.di.trans.steps.switchcase.SwitchCaseMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

public class KettleOPJobs {

    public void generateJobs(){
        //表插入步骤
        //查询需要插入的表的
        TableInputMeta tableInputMetaMaxId = new TableInputMeta();

        //表插入步骤
        //从原始表查询出数据
        TableInputMeta tableInputSearchData = new TableInputMeta();

        //SwitchCase步骤
        SwitchCaseMeta switchCaseMeta = new SwitchCaseMeta();
        // 默认步骤
        switchCaseMeta.setDefaultTargetStep(null);

        //插入更新步骤
        InsertUpdateMeta insertUpdateMeta = new InsertUpdateMeta();

        //删除步骤
        DeleteMeta deleteMeta = new DeleteMeta();

    }

}
