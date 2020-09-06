package com.ustcck.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
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

    /**
     * copy本地大文件到hdfs文件系统：带进度
     */
    @Test
    public void copyFromLocalBigFile() throws IOException {
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(new File("D:\\soft\\jdk-8u221-windows-x64.exe")));

        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/jdk-8u221-windows-x64.exe"), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096);
    }

    /**
     * copy hdfs文件系统文件到本地
     */
    @Test
    public void copyToLocalFile() throws IOException {
        Path src = new Path("/hdfsapi/test/a.txt");
        Path dst = new Path("/ustcck");
        fileSystem.copyToLocalFile(false, src, dst, true);
    }

    /**
     * 查看hdfs文件系统上目标文件夹下的所有文件
     */
    @Test
    public void listFiles() throws IOException {
        Path src = new Path("/hdfsapi/test/");
        FileStatus[] fileStatuses = fileSystem.listStatus(src);
        for (FileStatus fileStatus : fileStatuses) {
            String path = fileStatus.getPath().toString();
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            String permission = fileStatus.getPermission().toString();
            long len = fileStatus.getLen();
            short replication = fileStatus.getReplication();
            System.out.println(path + "\t" + isDir + "\t" + permission + "\t" + len + "\t" + replication);
        }
    }

    /**
     * 查看hdfs文件系统上目标文件夹下的所有文件 递归列出文件夹下的所有文件
     */
    @Test
    public void listFilesRecursive() throws IOException {
        Path src = new Path("/hdfsapi/test/");
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(src, true);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            String path = fileStatus.getPath().toString();
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            String permission = fileStatus.getPermission().toString();
            long len = fileStatus.getLen();
            short replication = fileStatus.getReplication();
            System.out.println(path + "\t" + isDir + "\t" + permission + "\t" + len + "\t" + replication);
        }
    }

    /**
     * 查看文件块信息
     */
    @Test
    public void getFileBlockLocations() throws IOException {
        Path src = new Path("/hdfsapi/test/jdk-8u221-windows-x64.exe");
        FileStatus fileStatus = fileSystem.getFileStatus(src);
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation block : blocks) {
            for (String name : block.getNames()) {
                System.out.println(name + "===" + block.getOffset() + "===" + block.getLength());
            }
        }
    }

    /**
     * 删除文件
     */
    @Test
    public void delete() throws IOException {
        Path src = new Path("/hdfsapi/test/jdk-8u221-windows-x64.exe");
        boolean b = fileSystem.delete(src, true);
        System.out.println(b);
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
