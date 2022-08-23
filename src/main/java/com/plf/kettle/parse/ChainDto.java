package com.plf.kettle.parse;


import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class ChainDto {
    private String startName;
    private String startTableInfo;
    private DbInfo startDbInfo;
    private String endName;
    private DbInfo endDbInfo;
    private String endTableInfo;
}
