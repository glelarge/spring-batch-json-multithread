# MCVE on Spring Batch JSON multi-thread

As requested in the Spring Batch [issue reporting guidelines](https://github.com/spring-projects/spring-batch/blob/main/ISSUE_REPORTING.md), this project provides an MCVE on potential issue with multi-threaded JSON item writer (Spring Batch issue [#4708](https://github.com/spring-projects/spring-batch/issues/4708)).

Sources are initialized from [spring-batch-mcve.zip](https://raw.githubusercontent.com/wiki/spring-projects/spring-batch/mcve/spring-batch-mcve.zip).


# Issue

Spring Batch offers the possibility to configure a chunk-oriented step as multi-threaded by configuring a TaskExecutor in the Step configuration.

When configured with multi-thread, the produced JSON is sometimes **not correctly formatted**.

Detected with **Spring Batch 5.1.2** and **Spring Framework 6.1.13**.

## Expected result
```json
[
 {"code":10001,"ref":"A1B2C3D4E5F6","type":10,"nature":5,"etat":1,"ref2":"A1B2C3D4E5F6"},
 {"code":10002,"ref":"B2C3D4E5F6G7","type":11,"nature":6,"etat":2,"ref2":"B2C3D4E5F6G7"},
 {"code":10003,"ref":"C3D4E5F6G7H8","type":12,"nature":7,"etat":3,"ref2":"C3D4E5F6G7H8"},
 {"code":10004,"ref":"D4E5F6G7H8I9","type":13,"nature":8,"etat":4,"ref2":"D4E5F6G7H8I9"}
]
```

## Wrong result

Sometime the produced JSON is misformatted :
```json
[
,
 {"code":10002,"ref":"B2C3D4E5F6G7","type":11,"nature":6,"etat":2,"ref2":"B2C3D4E5F6G7"} {"code":10001,"ref":"A1B2C3D4E5F6","type":10,"nature":5,"etat":1,"ref2":"A1B2C3D4E5F6"},
 {"code":10003,"ref":"C3D4E5F6G7H8","type":12,"nature":7,"etat":3,"ref2":"C3D4E5F6G7H8"},
 {"code":10004,"ref":"D4E5F6G7H8I9","type":13,"nature":8,"etat":4,"ref2":"D4E5F6G7H8I9"}
]
```

When the file is not well formatted, the error is always the same :
- a single comma appears at the first line
- the second line contains 2 JSON records without separator

# Investigation

Starting with a chunk size quite big (>100), the issue was not met, or perhaps not seen because the file format was not checked systematically.

For other tests, the chunk size has been reduced to 2, and issue start to appear (or detected).

The class `SynchronizedItemStreamWriter` manages the multi-thread [with a Lock](https://github.com/spring-projects/spring-batch/blob/fc1f3fcfc791196273b1249157c4e860b1df9025/spring-batch-infrastructure/src/main/java/org/springframework/batch/item/support/SynchronizedItemWriter.java#L50C1-L58C3).

Adding logs show that data seems to be flushed (meaning written to the real file) by the TransactionAwareBufferedWriter, but they are **actually** written right before the commit of the transaction.


Digging into the code:
- the `JsonFileItemWriter` inherits from `AbstractFileItemWriter`
- `AbstractFileItemWriter.write()` calls an internal `OutputState.write()` to write data ([check the code](https://github.com/spring-projects/spring-batch/blob/fc1f3fcfc791196273b1249157c4e860b1df9025/spring-batch-infrastructure/src/main/java/org/springframework/batch/item/support/AbstractFileItemWriter.java#L235))
- the `OutputState.write()` method writes and flush data to the writer ([check the code](https://github.com/spring-projects/spring-batch/blob/fc1f3fcfc791196273b1249157c4e860b1df9025/spring-batch-infrastructure/src/main/java/org/springframework/batch/item/support/AbstractFileItemWriter.java#L516C1-L523C4))
- the writer is a `TransactionAwareBufferedWriter` ([check the code](https://github.com/spring-projects/spring-batch/blob/fc1f3fcfc791196273b1249157c4e860b1df9025/spring-batch-infrastructure/src/main/java/org/springframework/batch/item/support/AbstractFileItemWriter.java#L581))
- this `TransactionAwareBufferedWriter` is actually writing data into the file, but the method `flush()` does not write any data ([check the code](https://github.com/spring-projects/spring-batch/blob/fc1f3fcfc791196273b1249157c4e860b1df9025/spring-batch-infrastructure/src/main/java/org/springframework/batch/support/transaction/TransactionAwareBufferedWriter.java#L187C1-L191C3))

Going even further, the issue seem to appear between the lock released by `SynchronizedItemStreamWriter` and the semaphore acquirement by `TaskletStep` to update the stream (and actually write data to the real file before to commit the transaction).

In a multi-threaded step, it seems that follow happens:
- thread T1 begins to read data and produces formatted JSON **without** JSON delimiter :
    ```json
    {record1}
    ```
- thread T2 reads data and produces formatted JSON **with** a JSON delimiter :
    ```json
    ,
    {record2}
    ```
- issue happens here:
    - both threads releases locks
    - the `TaskletStep` semaphore can be acquired by T1 or T2
    - if T1 gets the semaphore, all is right :
        ```json
        {record1},
        {record2}
        ```
    - if T2 gets the semaphore, the 2nd record is written first and makes the JSON wrong formatted
        ```json
        ,
        {record2} {record1}
        ```


# Proposed fixes

The code of `TransactionAwareBufferedWriter` has been updated in 2 different ways to fix the issue, both relying on the `forceSync` parameter, perhaps this parameter is not adapted.

Update code is flagged with `// FIXME` .

## Update TransactionAwareBufferedWriter.write() methods

- If `forceSync` is set, the methods writes directly into the channel instead of simply returning a buffer.
- Both methods `write()` are updated (with `char[]` and `String`).
- This does not write data when `flush()` is called

Updated code is [here](./src/main/java_spring_batch_5.1.2_fix_write/org/springframework/batch/support/transaction/TransactionAwareBufferedWriter.java).

## Update TransactionAwareBufferedWriter.flush() method

- If `forceSync` is set, the method get the stored buffer to write it into the channel.
- Stored buffer is cleaned

Updated code is [here](./src/main/java_spring_batch_5.1.2_fix_flush/org/springframework/batch/support/transaction/TransactionAwareBufferedWriter.java).


# Project 

## Main sources

The file [MyBatchJobConfiguration.java](./src/main/java/org/springframework/batch/MyBatchJobConfiguration.java) defines all the beans to build a Spring Batch job composed of a single chunk-oriented step.

The step :
- reads data from H2 DB
- processes data (simply write to the console)
- writes data to the file `mydata_YYYYMMDD_hhmmssSSS.json`

Multi-threading is configured helping the `TaskExecutor` and deprecated `throttleLimit(2)` method.

## Optional sources

The `pom.xml` provides 3 profiles to embed optional sources :
- profile `logs` adds some updated Spring Batch sources to add logs ([sources](./src/main/java_spring_batch_5.1.2_logs/))
- profile `fix_write` adds some updated Spring Batch sources to add logs and the fix on `TransactionAwareBufferedWriter.write()` methods ([sources](./src/main/java_spring_batch_5.1.2_fix_write/))
- profile `fix_flush` adds some updated Spring Batch sources to add logs and the fix on `TransactionAwareBufferedWriter.flush()` methods ([sources](./src/main/java_spring_batch_5.1.2_fix_flush/))

The folder ([./src/main/java_spring_batch_5.1.2/](./src/main/java_spring_batch_5.1.2/)) contains original of Spring Batch sources (version 5.1.2).


# How to run tests

The command is working :
```bash
mvn package exec:java -Dexec.mainClass=org.springframework.batch.MyBatchJobConfiguration
```
but as the error is met randomly, it's better to run a test loop until encountering the error.

The script [run.sh](./run.sh) runs a loop of 100 to run the jar.


```bash
# Build the jar
# 1 optional profile can be added :
# -Plogs
# -Pfix_write
# -Pfix_flush
mvn clean package # [-Plogs|-Pfix_write|-Pfix_flush]

# Run the tests
./run.sh
```


Example of logs:
```log
================================ loop 1
COMPLETED
Renaming output.log to mydata_20241115_173300344.json.log

================================ loop 2
COMPLETED
2:,
Error: Found empty line in the json file mydata_20241115_173302605.json
Renaming output.log to mydata_20241115_173302605.json____ko.log
```
When export is:
- OK : the `output.log` is simply renamed with the JSON filename
- KO : the `output.log` is renamed with the JSON filename suffixed by `____ko`

When run with a fix, 100 JSON files are produced.
