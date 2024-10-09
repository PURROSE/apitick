--初始化配置表
CREATE TABLE apitick.S_MSG (
    ID TEXT PRIMARY KEY,
    REQ_TIME TIMESTAMP,
    RES_TIME TIMESTAMP,
    URL TEXT NOT NULL,
    REQ_DATA TEXT,
    RES_DATA TEXT,
    STATUS SMALLINT,
    LEVEL SMALLINT DEFAULT 0,
    CREATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--Api Tick接口数据表
--产品列表
CREATE TABLE apitick.S_PRODUCT (
    code text not null,
    name text not null,
    type SMALLINT not null,
    is_del smallint DEFAULT 0,
    create_time timestamp DEFAULT CURRENT_TIMESTAMP,
    modify_time timestamp DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (code)
);
