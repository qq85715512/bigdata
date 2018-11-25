package study.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * TODO:
 * 
 * @author caiwenming
 * @version 1.0, 2018年11月22日/下午1:48:21
 * 
 */
public class TestHDFS {

	public final static String HDFS_PATH = "hdfs://hadoop1:9000/";
	public final static String USER = "hadoop";

	/**
	 * 使用JAVA原生的URL工具访问hdfs文件
	 * @throws IOException
	 */
	@Test
	public void readFromURL() throws IOException{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		URL url = new URL("hdfs://hadoop1:9000/user/hadoop/test.txt");
		InputStream in =  url.openStream();
		IOUtils.copyBytes(in, System.out, 4096, false);
		in.close();
	}

	/**
	 * 使用HDFS API访问文件，读取文件内容
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 */
	@Test
	public void readFromFs() throws IOException, URISyntaxException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.newInstance(new URI(HDFS_PATH), conf, USER);
		Path p = new Path("/user/hadoop/test.txt");
		FSDataInputStream is = fs.open(p, 1024);
		IOUtils.copyBytes(is, System.out, 4096, true);
	}

	/**
	 * 使用HDFS API访问文件，读取文件内容
	 * @throws IOException
	 */
	@Test
	public void readFromAPI() throws IOException{
		FileSystem fs = getFsByConf();
		Path p = new Path("/user/hadoop/test.txt");
		FSDataInputStream is = fs.open(p);
		IOUtils.copyBytes(is, System.out, 1024);
	}

	/**
	 * 使用HDFS API创建文件，并写进内容
	 * @throws IOException
	 */
	@Test
	public void createFile() throws IOException, URISyntaxException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.newInstance(new URI(HDFS_PATH), conf, "hadoop");
		Path p = new Path("/test1.txt");
		FSDataOutputStream fdos = fs.create(p, true);
		fdos.write("Hello Hadoop".getBytes());
		fdos.close();
	}

	/**
	 * 向HDFS中已存在的文件添加新的内容
	 * 目前添加失败，因为hdfs配置的问题，暂不支持添加
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 */
	@Test
	public void appendToFile() throws IOException, URISyntaxException, InterruptedException{
		FileSystem fs = getFsByConf();
		Path p = new Path("/user/hadoop/test.txt");
		FSDataOutputStream os = fs.append(p, 1024);
		String txt = "new people, new life";
		os.write(txt.getBytes());
		os.close();
		readFromFs();
	}

	/**
	 * 如果path最前面没有“/”则表示当前目录为当前用户的目录，
	 * 如果hdfs还没有当前用户目录，则会自动创建当前用户目录
	 * @throws IOException
	 */
	@Test
	public void mkdir() throws IOException {
		FileSystem fs = getFsByConf();
		//这个path将在/user/下创建test文件夹
		Path p = new Path("/user/test/");
		/*
		  ps:当前用户为windows中的用户：caiwenming
		  这个path将在/user/caiwenming/下创建user/test文件夹
		  其中caiwenming目录如果原先不存在，则会自动创建
		*/
		//Path p = new Path("user/test/");
		fs.mkdirs(p);
	}

	/**
	 * 使用HDFS API删除文件或目录，支持递归删除
	 * @throws IOException
	 */
	@Test
	public void deleteDirOrFile() throws IOException {
		FileSystem fs = getFsByConf();
		Path p = new Path("/user/test");
		fs.delete(p, true);
	}

	/**
	 * 获取HDFS的文件状态
	 */
	@Test
	public void listFileStatus() throws IOException {
		FileSystem fs = getFsByConf();
		Path p = new Path("/");
		FileStatus[] fstArr = fs.listStatus(p);
		for (FileStatus fst : fstArr){
			System.out.println(fst.getPath().getName());
		}
	}

	@Test
	public void listFileStatusRecursive() throws IOException {
		Path p = new Path("/");
		printFileStatusRecursive(p);
	}

	private void printFileStatusRecursive(Path path) throws IOException {
		FileSystem fs = getFsByConf();
		if (fs.isFile(path)) {
			System.out.println(path);
			return;
		}
		FileStatus [] fstArr = fs.listStatus(path);
		for (FileStatus fst : fstArr) {
			if (fs.isDirectory(fst.getPath())) {
				System.out.println(getParent(fst.getPath()));
			}
			printFileStatusRecursive(fst.getPath());
		}
	}

	private String getParent(Path path) {
		if (path.getParent() != null) {
			return getParent(path.getParent()) + "/" + path.getName();
		}
		return "";
	}

	/**
	 * 获取HFDS 文件系统的实例
	 * @return
	 * @throws IOException
	 */
	public FileSystem getFsByConf() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", HDFS_PATH);
		FileSystem fs = FileSystem.newInstance(conf);
		return fs;
	}
}
