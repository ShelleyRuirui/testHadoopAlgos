package testHDFS;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class HDFSUtil {

	static{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void testRawRead(String filePath){
		InputStream in=null;
		try{
			in=new URL(filePath).openStream();
			IOUtils.copyBytes(in, System.out, 4096,false);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(in);
		}
	}
	
	public static void testReadWithFS(String filePath) throws IOException{
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(filePath),conf);
		FSDataInputStream in=null;
		try{
			in=fs.open(new Path(filePath)); //FSDataInputStream, is seekable
			IOUtils.copyBytes(in, System.out, 4096,false);
			in.seek(0);
			IOUtils.copyBytes(in, System.out, 4096,false);
		}finally{
			IOUtils.closeStream(in);
		}
	}
	
	public static void testWriteWithProgress(String localSrc,String dst) throws IOException{
		InputStream in=new BufferedInputStream(new FileInputStream(localSrc));
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(dst),conf);
		OutputStream out=fs.create(new Path(dst),new Progressable(){
			public void progress(){
				System.out.print(".");
			}
		});
		
		IOUtils.copyBytes(in, out, 4096,true);
	}
	
	public static void testListFileStats(String uri) throws IOException{
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(uri),conf);
		Path path=new Path(uri);
		FileStatus[] status=fs.listStatus(path);
		Path[] listedPaths=FileUtil.stat2Paths(status);
		for(Path p:listedPaths){
			System.out.println(p);
		}
	}
	
	public static void main(String[] args) throws IOException {
		String dir="hdfs://localhost:9000/user/shelleyruirui/input";
		String path="hdfs://localhost:9000/user/shelleyruirui/input/core-site.xml";
		//HDFSUtil.testRawRead(path);
		//HDFSUtil.testReadWithFS(path);
		
//		String input="/home/shelleyruirui/softwares/programming/java/hadoop/hadoop-1.2.1/CHANGES.txt";
//		String dst="hdfs://localhost:9000/user/shelleyruirui/input/CHANGES.data";
//		HDFSUtil.testWriteWithProgress(input,dst);
		
		testListFileStats(dir);
	}
	
}
