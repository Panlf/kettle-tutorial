package com.plf.kettle.parse;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class DbInfo {
    private String name;
    private String server;
    private String database;
    private String username;
}
