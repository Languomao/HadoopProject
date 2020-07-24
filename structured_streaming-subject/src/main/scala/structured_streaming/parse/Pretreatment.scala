package structured_streaming.parse

import java.io.{File, FileOutputStream, FileWriter}
import java.util.regex.{Matcher, Pattern}

import org.apache.commons.lang.StringUtils
import structured_streaming.data_pretreatment.DataPretreatment.getDataStr

/**
  * Classname Pretreatment
  * Description TODO
  * Date 2020/7/21 10:53
  * Created by LanKorment
  * 使用scala来预处理数据
  */
object Pretreatment {
  def main(args: Array[String]): Unit = {
    preprocessingData("D:\\WorkSpace\\Telemetry\\dump-cpu.txt","D:\\WorkSpace\\Telemetry\\test.txt");
  }

  //传入参数原始数据的路径以及经过处理后的数据的输出路径
  def preprocessingData(inputpath : String,outputpath : String): Unit ={
    val str = getDataStr(inputpath)
    pretreatment(data2str(str),outputpath)
  }

  def data2str(strbuff: StringBuffer): Array[String] = {
    val data = strbuff.toString
    //System.out.println(data);
    val result = data.split("------- ")
    result
  }

  def replaceBlank(str: String): String = {
    var dest = ""
    if (str != null) {
      val p = Pattern.compile("\\s*|\t|\r|\n")
      val m = p.matcher(str)
      dest = m.replaceAll("")
    }
    dest
  }

  //strarr为将文件转换成的字符串，即data2str()的返回，传入参数path为预处理数据输出的位置
  @throws[Exception]
  def pretreatment(strarr: Array[String],outpath : String ): Unit = {
    var i = 1
    while ( {
      i < strarr.length
    }) {
      val str = strarr(i).replace("-------\n", "")
      val header = StringUtils.substringBetween(str, "2020-", "]") + "]"
      val body = StringUtils.substringAfter(str, header)

      //将Header加入到Json中
      /*val data = replaceBlank(body);
      val json = new StringBuffer(data).insert(1,"\"Header\":" + "\"" + header + "\"" + ",").toString();*/

      //未将Header加入到body中
      val json = replaceBlank(body)
      val filePath = outpath
      //文件追加内容
      val fos = new FileOutputStream(filePath, true)
      fos.write(json.getBytes)
      fos.write("\n".getBytes)
      //System.out.println(header);
      //System.out.println(json);

      {
        i += 1; i - 1
      }
    }
  }

}
