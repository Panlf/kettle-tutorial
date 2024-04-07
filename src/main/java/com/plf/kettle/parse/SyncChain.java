package com.plf.kettle.parse;

import lombok.Data;

/**
 * @author panlf
 * @date 2024/4/3
 */
@Data
public class SyncChain {
    private String startServer;
    private String startDataBase;
    private String startTable;
    private String endServer;
    private String endDataBase;
    private String endTable;
}
