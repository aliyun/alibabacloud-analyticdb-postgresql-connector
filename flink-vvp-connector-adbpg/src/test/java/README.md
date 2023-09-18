# How to run unittests

## Setup envs
```bash
export ADBPG_URL="jdbc:postgresql://${INSTANCE_ID}-master.gpdb.zhangbei.rds.aliyuncs.com:5432/postgres"
export ADBPG_USERNAME="${YOUR_USERNAME}"
export ADBPG_PASSWORD="${YOUR_PASSWORD}"
```

## Run mvn test
```bash
mvn test
```