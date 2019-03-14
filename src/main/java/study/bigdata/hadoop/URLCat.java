package study.bigdata.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.io.IOUtils;

/**
 * TODO:
 * 
 * @author caiwenming
 * @version 1.0, 2018年11月21日/下午8:08:39
 * 
 */
public class URLCat {
	
	public static void main(String[] args) throws IOException {
		System.out.println("xxxxx");
		InputStream in = null;
		try {
			in = new URL(args[0]).openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			in.close();
		}
	}
}
