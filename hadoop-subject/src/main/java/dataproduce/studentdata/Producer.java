package dataproduce.studentdata;

import java.io.*;

/**
 * Created by Languomao on 2019/5/30.
 * args[0],数据的数量
 * args[1]，数据输出的位置
 */
public class Producer {

    public static void main(String[] args) throws IOException {

        int num =Integer.parseInt(args[0]);
        for(int i =0;i<num;i++){
            charOutStream(RandomStudent.getStudent().toString(),args[1]);
            //System.out.println(getStudent().toString());
        }
    }

    public static void charOutStream(String text, String filepath) throws IOException {
        // 1：利用File类找到要操作的对象
        //File file2 = new File("D:" + File.separator + "demo" + File.separator + "test.txt");
        //System.out.println(file2.toString());
        File file = new File(filepath);
        if(!file.getParentFile().exists()){
            file.getParentFile().mkdirs();
        }

        //2：准备输出流
        Writer out = new FileWriter(file);
        out.write(text);
        out.close();

    }

    public static void byteOutStream(String text,String filepath) throws IOException {

        //1:使用File类创建一个要操作的文件路径
        //File file = new File("D:" + File.separator + "demo" + File.separator + "test.txt");
        File file = new File(filepath);
        if(!file.getParentFile().exists()){ //如果文件的目录不存在
            file.getParentFile().mkdirs(); //创建目录

        }

        //2: 实例化OutputString 对象
        OutputStream output = new FileOutputStream(file);

        //3: 准备好实现内容的输出

        //String msg = "HelloWorld";
        //将字符串变为字节数组
        byte data[] = text.getBytes();
        output.write(data);
        //4: 资源操作的最后必须关闭
        output.close();

    }
}
