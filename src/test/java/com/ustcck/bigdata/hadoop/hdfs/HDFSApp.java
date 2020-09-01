package com.ustcck.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://hadoop000:8020";
    Configuration configuration = null;
    FileSystem fileSystem = null;

    /**
     * 构造一个访问指定HDFS系统的客户端对象
     * 第一个参数：HDFS的URI
     * 第二个参数：客户端指定的配置参数，这里是默认的
     * 第三个参数：客户端身份，说白了就是用户名
     */
    @Before
    public void setUp() throws Exception {
        System.out.println("--------setUp---------");
        configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    @Test
    public void mkdir() throws Exception {
        Path path = new Path("/hdfsapi2/test/");
        boolean result = fileSystem.mkdirs(path);
        System.out.println(result);
    }

    /**
     * 参看HDFS内容
     */
    @Test
    public void text() throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/cdh_version.properties"));
        IOUtils.copyBytes(fsDataInputStream, System.out, 1024);
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws IOException {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi2/test/b.text"));
        fsDataOutputStream.writeUTF("hello, ustcck");
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws IOException {
        Path src = new Path("/hdfsapi2/test/b.text");
        Path dst = new Path("/hdfsapi2/test/c.text");
        boolean result = fileSystem.rename(src, dst);
        System.out.println(result);
    }

    /**
     * copy本地文件到hdfs文件系统
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        Path src = new Path("D:\\lenovo.template");
        Path dst = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(src, dst);
    }

    @Test
    public void testReplication() {
        System.out.println(configuration.get("dfs.replication"));
    }

    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("--------tearDown---------");
    }

}
