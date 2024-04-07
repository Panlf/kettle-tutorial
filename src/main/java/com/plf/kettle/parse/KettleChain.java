package com.plf.kettle.parse;

import java.util.List;
import lombok.Data;

/**
 * @author panlf
 * @date 2024/4/1
 */
@Data
public class KettleChain {
    private String server;
    private String table;
    private String nodeName;
    private List<KettleChain> afterChain;
    private List<KettleChain> beforeChain;
}
