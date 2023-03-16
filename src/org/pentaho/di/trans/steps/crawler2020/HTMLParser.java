package org.pentaho.di.trans.steps.crawler2020;

import java.io.*;
import java.util.Properties;
import java.net.URL;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import com.xgn.search.Search;
import com.xgn.search.html.HTMLTractor;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2005</p>
 *
 * <p>Company: </p>
 *
 * @author ������
 * @version 1.0
 */
public class HTMLParser {
  String content;
  String startMark;
  String endMark;

  int nestedLevel;

  int offSet; //the offset of the start mark in the string. �ڼ�����ʼ���
  
  private String encoding;

//  public HTMLParser(String urlString) throws IOException, Exception {
//    HTMLTractor tr = new HTMLTractor(urlString);
//    content = tr.getSource();
//  }

  public HTMLParser(URL url) throws IOException, Exception {
    this(url,"utf-8");
  }

  
  public HTMLParser(final URL url,final String encoding) throws IOException, Exception {
	  this.encoding = encoding;
	  Thread thread = new Thread()
	  {
		public void run()
		{
			try{
			com.xgn.search.html.HTMLTractor tr = new com.xgn.search.html.HTMLTractor(url.toString());
		    content = tr.getSource(encoding);
			}catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
	  };
	  thread.start();
	  int count=0;
	  while(count<60 && thread.isAlive()) //wait 60 second
	  {
	      Thread.sleep(1000);  
	      if(count>=5 && count % 5 ==0)
	      Search.debug("count:"+count);
	      count++;
	  }
	  if(thread.isAlive())
		  thread.stop();
	  
  }
  

  public HTMLParser(URL url, Properties p, String httpeHeadContent) throws
      IOException {
    com.xgn.search.html.HTMLTractor tr = new HTMLTractor(url.toString(), p,false);
    content = tr.getSourceFromPost(httpeHeadContent);
  }

  public HTMLParser(String c) {
    content = c;
  }

  public void setNestedLevel(int nestedLevel) {
    this.nestedLevel = nestedLevel;
  }

  public void setStartMark(String startMark) {
    this.startMark = startMark;
  }

  public void setString(String string) {
    this.content = string;
  }

  public void setEndMark(String endMark) {
    this.endMark = endMark;
  }

  public void setOffSet(int offSet) {
    this.offSet = offSet;
  }

  /**
       * get the trunked string according to the start mark ,end mark and nested level
   * @return String
   */
  public String getTrunkedString() {
    int startIndex = getTunkedStringStartIndex();
    int endIndex;
    if (this.nestedLevel == 0) {
      endIndex = content.indexOf(this.endMark, startIndex) + endMark.length();
      return content.substring(startIndex, endIndex);
    }
    else {
      endIndex = startIndex;
      for (int i = 0; i < this.nestedLevel; i++) {
        endIndex = content.indexOf(this.endMark, endIndex) + endMark.length();
      }
      return content.substring(startIndex, endIndex) + endMark;
    }
  }

  /**
   * �����ĳ������ַ���֮�������, ����ַ����ͻ�õ��ַ�����ͬһ��TAG��
   * @return String mark ����ַ���
   * @return int offset �ı����ж������ַ���ʱ,ָ���ı���ַ�����˳���.���ֻ��һ��,��ò���Ϊ0.
   */
  public String getTrunkedStringNextMark(String mark, int offset) {
    int index = getIndexOfMark(mark, offset);
    if (index == -1) {
      return ""; //û�иñ�־,���ؿ��ַ���
    }
    int begin = index + mark.length();
    return content.substring(begin, content.indexOf('<', begin));
  }

  /**
   * �����ĳ������ַ���֮��� Tag �������, Tag �� startMark �� endMark ����
   * @return String mark ����ַ���
   * @return int offset �ı����ж������ַ���ʱ,ָ���ı���ַ�����˳���.���ֻ��һ��,��ò���Ϊ0.
   */
  public String getTrunkedStringAfterMark(String mark, int offset) {
    int index = getIndexOfTagContent(mark, offset);
    String result;
    if (index == -1) {
      return ""; //û�иñ�־,���ؿ��ַ���
    }
    index++;
    int beginIndex = content.indexOf(this.startMark, index);
    int endIndex = content.indexOf(this.endMark, beginIndex);

    if (endIndex > beginIndex + startMark.length()) {
      result = content.substring(content.indexOf('>',
                                                 beginIndex + startMark.length()) +
                                 1, endIndex);
    }
    else {
      result = content.substring(index + mark.length(),
                                 beginIndex + startMark.length());
    }
    return deleteAllTags(result);
  }

  /** mark is part content of an element, as the follong fomat
   *  "... > mark other chars < ... "
   * this method can get the index of  "mark" in the content
   * @param mark
   * @param offset: the offset of mark in the string
   * @return -1 if no mark in the string
   */
  public int getIndexOfMark(String mark, int offset) {
    int index = -1;
    int i = 0;
    for (i = 0; i <= offset; i++) {
      index = content.indexOf(mark, index + 1);
    }
    return index;
  }

  /** mark is the content of an element, as the follong fomat
   *  "... > mark < ... "
   * this method can get the index of  "mark" in the content
   * @param mark
   * @param offset: the offset of mark in the string
   * @return -1 if no mark in the string
   */
  public int getIndexOfTagContent(String tagContent, int index) {
    int offset = -1;
    int i = 0;
    Pattern p = Pattern.compile(">" + "[\\s]*" + tagContent + "[:|��]{0,1}[\\s]*" + "<");
    Matcher matcher = p.matcher(content);
    while (matcher.find()) {
      if (i++ == index) {
        offset = matcher.start();
        break;
      }
      else {
        continue;
      }
    } 
    return offset;
  }

  /**
   * ��� mark ��Ǻ���� tag ��ǩ������ݡ�
   *
   * @param mark: һ������ַ������磺 "�� Ȧ" , "�� ַ"
   * @param order: the order of tag after the mark, start from 0
   * @param tag: html tag without "<" or ">" ,such as "TD"
   * @return "" if no mark in the string
   *
   * �� �ַ���
   * <td>��ַ</td> <td ><font size = 12>�йش�</td>
   *
   * ����
   * mark �� ����ַ��
   * order �� 0
   * tag �� td
   *
   * ���� �йش�
   */
  public String getTagContentAfterMark(String mark, int order, String tag) {
    int offset = getIndexOfTagContent(mark, 0);

    if (offset == -1) {
      return "";
    }
    int tagIndex = getTagIndexIgnoreCase(tag,order, offset + mark.length()+1);
    return getTagContent(tagIndex, tag);
  }

  /**
   * ��� mark ������ڵ� tag ��ǩ������ݡ�
   *
   * @param mark: һ������ַ������磺 "�� Ȧ" , "�� ַ"
   * @param offset: the offset of mark in the string��������ַ����е�λ��
   * @param tag: html tag without "<" or ">" ,such as "TD"
   * @return "" if no mark in the string
   *
   * �� �ַ���
   * <td font size = 12>��ַ�� �йش� </td>
   *
   * ����
   * mark �� ����ַ��
   * offset �� 0
   * tag �� td
   *
   * ���� ��ַ�� �йش�
   */
  public String getTagContentOfMark(String mark, int offset, String tag) {
    int index = getIndexOfMark(mark, offset);
    if (index == -1) {
      return "";
    }
    int tagIndex = getLastTagIndexIgnoreCase(tag, index + mark.length());
    return getTagContent(tagIndex, tag);
  }

  /**
   * get the Nth tag after offset
   * @param tag
   * @param order: the sequence of the tag after offset
   * @param offset
   * @return
   */
  public int getTagIndexIgnoreCase(String tag,int order, int offset) {
    int tagOffset = offset;
    for (int i = 1;i<= order;i++)
    tagOffset =  content.toUpperCase().indexOf(tag.toUpperCase(), tagOffset);
    return tagOffset;
  }

  public int getLastTagIndexIgnoreCase(String tag, int offset) {
    return content.toUpperCase().lastIndexOf(tag.toUpperCase(), offset);
  }

  /**
   * get the index of a tag which the offset belongs to
   * @param offset
   * @return
   */
  public int getTagIndex(int offset) {
    char c = content.charAt(offset);
    while (c != '<') {
      c = content.charAt(--offset);
    }
    return offset;
  }

  /**
   * ����ָ����ǩ��Ĳ������ݣ���������ǩ����������
   * @param cont
   * @param tag
   * @return
   */
//  public String getTagContentBySegment (String seg, String tag)
//  {
//
//  }

  /**
   * ���ָ��һ�Ա�ǩ�������
   * @param index ��ʼ������λ��
   * @param tag
   * @return
   *
   * �� <TD FONT = 12 >13261217005 <A>look up the phone</A></TD>
   * para:     tag = TD
   * para:     index �� 0 �� 14 ֮���һ������ 0 ���� "<" ,14 ���� ">"
   * return:   13261217005 <A>look up the phone</A>
   */
  public String getTagContent(int index, String tag) {

    char c = content.charAt(index);
    if (c != '<') { // index ��tag ��, Ӧ��ʹ��һ�����������.
      index = getTagIndex(index);
    }
    c = content.charAt(index);
    boolean tagBegin = false, flag = true;
    int begin = 0, end = 0;
    while (flag) {
      switch (c) {
        case '<':
          if (content.substring(index + 1, index + tag.length() + 1).
              equalsIgnoreCase(tag)) {
            tagBegin = true;
          }
          c = content.charAt(++index);
          break;
        case '>':
          if (tagBegin) {
            begin = index + 1;
            tagBegin = false;
          }
          if (content.substring(index - tag.length() - 1, index).
              equalsIgnoreCase("/" + tag) && begin != 0) {
            end = index - tag.length() - 2;
            flag = false;
          }
          if(content.length() == index+1)
          {
        	  return "";
          }
          c = content.charAt(++index);
          break;
        default:
          c = content.charAt(++index);
          break;
      }
    }
    return content.substring(begin, end);

  }

  
  /**
   * ���ָ����־����һ����ǩ�������
   * @param index ��ʼ������λ��
   * @param tag
   * @return
   *
   * �� <TD> phone </TD> <TD FONT = 12 >13261217005 <A>look up the phone</A></TD>
   * para:     mark = phone 
   * return:   13261217005 <A>look up the phone</A>
   */
  public String getNextTagContent(String mark) {
	  
	  int index = this.getIndexOfTagContent(mark, 0);
	  if(index==-1)
		  return null;
	  Pattern StartTagPattern =  Pattern.compile("[<]{1}[^/]{1}[a-z|A-Z|\\s|\\\"|/|=]*[>]{1}");
	  Matcher matcher = StartTagPattern.matcher(content);
	  int startIndexOfNextTag;
	  int endIndexOfNextTag;
	  
	  if( matcher.find(index))
		  {startIndexOfNextTag = matcher.start();
		   endIndexOfNextTag = matcher.end();
		  }
	  else
		  return null;
	  
	  String tag = content.substring(startIndexOfNextTag+1 ,endIndexOfNextTag-1 ).trim();
	  if(tag.indexOf(" ")>-1)
		  tag = tag.substring(0,tag.indexOf(" "));
	  return getTagContent(startIndexOfNextTag, tag);
  }
  
  /**
   * ���mark��� tag��ǩ��
   * @param mark
   * @param offset
   * @param tag
   * @return  ֻ���ؿ�ʼ��ǩ�������ؽ�����ǩ
   *
   * �� �ַ���
   * <td>��ַ</td> <td ><font size = 12>�йش�</font></td>
   *
   * ����
   * mark �� ����ַ��
   * offset �� 0
   * tag �� font
   *
   * ���� <font size = 12>
   */
  public String getTagAfterMark(String mark, int offset, String tag) {
    int index = getIndexOfMark(mark, offset);
    if (index == -1) {
      return "";
    }
    if (!tag.startsWith("<")) {
      tag = "<" + tag;
    }
    int tagStartIndex = this.content.indexOf(tag, index);
    int tagEndIndex = this.content.indexOf(">", tagStartIndex);
    return content.substring(tagStartIndex, tagEndIndex + 1);
  }

  /**
   * ȥ��һ���ַ����������Tag
   * @return String
   */
  public static String deleteAllTags(String str) {
    StringBuffer result = new StringBuffer();
    char[] array = str.toCharArray();
    //boolean tag = false;
    int level = 0;
    for (int i = 0; i < array.length; i++) {
      switch (array[i]) {
        case '<':
          level++;
          break;
        case '>':
          level--;
          break;
        default:
          if (level == 0) {
            result.append(array[i]);
          }
          break;
      }
    }
    return result.toString().replaceAll("&nbsp;", " ").trim();
  }

  /**
   * getTunkedStringIndex
   *
   * @return int
   */
  private int getTunkedStringStartIndex() {
    int index = 0;
    for (int i = 0; i < offSet; i++) {
      index = content.indexOf(this.startMark, index) + startMark.length();
    }
    return index - startMark.length();
  }

  /**
   *
   * @param s String: the <a> tag such as "<a href=shou.asp>"
   * @return String
   */
  public String getURLFromTagA(String tag) {
    if (tag.trim().equals("") || tag == null) {
      return null;
    }
    int begin = tag.indexOf("href");
    int end = tag.indexOf(" ", begin) == -1 ? tag.indexOf(">", begin) :
        tag.indexOf(" ", begin);
    if(end==-1)
    	end = tag.length();
    String comparativeURL = tag.substring(begin + 4, end).trim();
    comparativeURL = comparativeURL.substring(comparativeURL.indexOf("=") + 1);
    if ( (comparativeURL.startsWith("\"") && comparativeURL.endsWith("\"")) ||
        (comparativeURL.startsWith("'") && comparativeURL.endsWith("'"))) {
      comparativeURL = comparativeURL.substring(1,
                                                comparativeURL.length() - 1);

    }
    return comparativeURL.replaceAll("&amp;", "&"); //��&amp;�滻Ϊ&
  }

  /**
   * getTunkedStringIndex
   *
   * @return int
   */
  private int getTunkedStringEndIndex() {
    int index = 0;
    for (int i = 0; i < offSet; i++) {
      index = content.indexOf(this.endMark, index);
    }
    return index;
  }

//  public static void main(String[] args) {
//    File file = new File("c:/news.txt");
//    FileReader reader = null;
//    StringWriter writer = new StringWriter();
//    try {
//      reader = new FileReader(file);
//    }
//    catch (FileNotFoundException ex) {
//    }
//    char[] buffer = new char[1024];
//
//    int length;
//    try {
//      length = reader.read(buffer, 0, 1024);
//      while (length != -1) {
//        writer.write(buffer, 0, length);
//        length = reader.read(buffer, 0, 1024);
//      }
//    }
//    catch (IOException ex1) {
//    }
//    String string = writer.toString();
//    HTMLParser parser = new HTMLParser(string);
//    parser.setOffSet(7);
//    parser.setNestedLevel(1);
//    parser.setStartMark("<table");
//    parser.setEndMark("</table>");
//    String content = parser.getTrunkedString();
//    System.out.println(content);
//  }

  public static boolean isDigital(char c) {
    return c <= 57 && c >= 48;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

public String getEncoding() {
	return encoding;
}

public void setEncoding(String encoding) {
	this.encoding = encoding;
}


public boolean includeTag(String value) {
	  Pattern StartTagPattern =  Pattern.compile("[<]{1}[^/]{1}[a-z|A-Z|\\s|\\\"|/|=]*[>]{1}");
	  Matcher matcher = StartTagPattern.matcher(value);
	  if(matcher.find())
		  return true;
	  else
		  return false;
}

}
